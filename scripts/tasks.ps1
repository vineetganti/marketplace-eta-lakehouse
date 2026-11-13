<#
.SYNOPSIS
  Windows task runner for the marketplace ETA lakehouse.
  Replaces `make` and the bash scripts. Run it through tasks.cmd in the project root:

      .\tasks help
      .\tasks up

  Works on Windows PowerShell 5.1 and PowerShell 7. Keep this file ASCII-only:
  Windows PowerShell 5.1 reads BOM-less files as ANSI and mangles other characters.
#>
param(
    [Parameter(Position = 0)][string]$Target = 'help',
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)][string[]]$Rest = @(),
    [string]$SimDate = '2025-01-15',
    [switch]$Force          # bypass the duplicate-job guard (see Get-LiveJobNames)
)

# 'Continue' on purpose: in Windows PowerShell 5.1, 'Stop' turns harmless native stderr output
# (docker prints progress there) into terminating errors. We check exit codes explicitly instead.
$ErrorActionPreference = 'Continue'
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$OutputEncoding = New-Object System.Text.UTF8Encoding($false)   # no BOM when piping text to docker

$ConnectUrl = 'http://localhost:8083'
$FlinkUrl = 'http://localhost:8081'
$ConnectorName = 'pg-marketplace'

# ---------------------------------------------------------------- helpers
function Say([string]$Message) { Write-Host $Message -ForegroundColor Cyan }

function Run {
    # Run a native command; fail loudly on a non-zero exit code.
    $exe = $args[0]
    $rest = @()
    if ($args.Count -gt 1) { $rest = $args[1..($args.Count - 1)] }
    & $exe @rest
    if ($LASTEXITCODE -ne 0) { throw "command failed (exit $LASTEXITCODE): $exe $rest" }
}

function Assert-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'docker not found. Install Docker Desktop and reopen the terminal.'
    }
    & docker info *> $null
    if ($LASTEXITCODE -ne 0) { throw 'Docker is installed but not running. Start Docker Desktop and wait for "Engine running".' }
}

function Find-Python {
    # Returns @(exe, args...) for a Python >= 3.10, skipping the Microsoft Store stub.
    # Candidates are plain strings split on spaces: single-element arrays get flattened by PowerShell.
    foreach ($cand in @('py -3', 'python', 'python3')) {
        $c = @($cand.Split(' '))
        if (-not (Get-Command $c[0] -ErrorAction SilentlyContinue)) { continue }
        $extra = @()
        if ($c.Count -gt 1) { $extra = $c[1..($c.Count - 1)] }
        try {
            $ok = & $c[0] @extra -c "import sys; print(sys.version_info >= (3, 10))" 2>$null
            if ($LASTEXITCODE -eq 0 -and "$ok".Trim() -eq 'True') { return , $c }
        } catch { }
    }
    throw 'Python 3.10+ not found. Install it from python.org (tick "Add python.exe to PATH") and reopen the terminal.'
}

function Get-VenvPython {
    foreach ($rel in @('.venv\Scripts\python.exe', '.venv/bin/python')) {
        $p = Join-Path $Root $rel
        if (Test-Path $p) { return $p }
    }
    throw 'No virtual environment yet. Run:  .\tasks venv'
}

function Wait-Url([string]$Url, [int]$TimeoutSec = 180, [scriptblock]$Ready = $null) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    Write-Host -NoNewline "waiting for $Url "
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-RestMethod -Uri $Url -TimeoutSec 5
            if ($null -eq $Ready -or (& $Ready $r)) { Write-Host ' ok'; return }
        } catch { }
        Write-Host -NoNewline '.'
        Start-Sleep -Seconds 3
    }
    Write-Host ''
    throw "timed out waiting for $Url (check:  .\tasks ps   and   .\tasks logs)"
}

function Wait-Flink { Wait-Url "$FlinkUrl/overview" 300 { param($o) $o.taskmanagers -ge 1 } }

# ---------------------------------------------------------------- Flink SQL submission
function Render-Sql([string]$RelPath, [string[]]$Pairs) {
    # Fills {{KEY}} placeholders and writes the result to flink/sql/_rendered/, which the
    # containers see through the existing ./flink/sql bind mount (no docker cp needed).
    $src = Join-Path $Root ('flink/sql/' + $RelPath)
    if (-not (Test-Path $src)) { throw "no such file: $src" }
    $text = [IO.File]::ReadAllText($src) -replace "`r`n", "`n"
    foreach ($kv in $Pairs) {
        $i = $kv.IndexOf('=')
        if ($i -lt 1) { throw "expected KEY=VALUE, got '$kv'" }
        $text = $text.Replace('{{' + $kv.Substring(0, $i) + '}}', $kv.Substring($i + 1))
    }
    $left = [regex]::Matches($text, '\{\{[A-Z_]+\}\}') | ForEach-Object { $_.Value } | Sort-Object -Unique
    if ($left) { throw ('unresolved placeholders: ' + ($left -join ', ') + '  (pass KEY=VALUE after the file)') }

    $outDir = Join-Path $Root 'flink/sql/_rendered'
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    $name = $RelPath -replace '[\\/]', '_'
    $dst = Join-Path $outDir $name
    [IO.File]::WriteAllText($dst, $text, (New-Object System.Text.UTF8Encoding($false)))
    return $name
}

function Get-LiveJobNames {
    # Names of Flink jobs that are not finished, failed or cancelled. Every streaming job here
    # sets 'pipeline.name'. Submitting one twice makes two copies that both read the topics from
    # the start and write the same tables: append-only tables double, and the extra jobs use up
    # task slots so the batch reconcile hangs (BUILD_LOG session 7).
    try {
        $jobs = (Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview" -TimeoutSec 10).jobs
    } catch {
        Write-Host "warning: could not list Flink jobs ($($_.Exception.Message)); duplicate-job guard skipped" -ForegroundColor Yellow
        return @()
    }
    $live = @($jobs | Where-Object { $_.state -notin @('FINISHED', 'FAILED', 'CANCELED') })
    return @($live | ForEach-Object { $_.name })
}

function Get-PipelineName([string]$RelPath) {
    # Reads  SET 'pipeline.name' = '...';  from a SQL file. Returns $null if there is none.
    $src = Join-Path $Root ('flink/sql/' + $RelPath)
    if (-not (Test-Path $src)) { return $null }
    $m = [regex]::Match([IO.File]::ReadAllText($src), "SET\s+'pipeline\.name'\s*=\s*'([^']+)'")
    if ($m.Success) { return $m.Groups[1].Value }
    return $null
}

function Submit-Sql([string]$RelPath, [string[]]$Pairs = @()) {
    Assert-Docker
    # Guard: only for the long-running streaming jobs, not DDL or batch files.
    if ($RelPath -like 'jobs/*' -and -not $Force) {
        $pn = Get-PipelineName $RelPath
        if ($pn -and ((Get-LiveJobNames) -contains $pn)) {
            throw "job '$pn' is already running; submitting it again would duplicate its output. Check $FlinkUrl, or add -Force to override."
        }
    }
    $name = Render-Sql $RelPath $Pairs
    Say "submitting $RelPath"
    Run docker compose exec -T jobmanager /opt/flink/bin/sql-client.sh -i /opt/flink/sql/init.sql -f "/opt/flink/sql/_rendered/$name"
}

# ---------------------------------------------------------------- stack setup
function New-Topics {
    Assert-Docker
    foreach ($t in @('pg.public.trips', 'pg.public.drivers', 'pg.public.merchants', 'driver.pings')) {
        # 7-day retention is what makes Kafka-side replay possible. "Already exists" is fine.
        & docker compose exec -T redpanda rpk topic create $t -p 3 -r 1 -c retention.ms=604800000
    }
    Run docker compose exec -T redpanda rpk topic list
}

function Register-Connector {
    Wait-Url "$ConnectUrl/connectors" 180
    $cfg = (Get-Content -Raw (Join-Path $Root 'infra/debezium/pg-connector.json') | ConvertFrom-Json).config
    $body = $cfg | ConvertTo-Json -Depth 5 -Compress
    Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$ConnectorName/config" `
        -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 6
    Start-Sleep -Seconds 3
    Invoke-RestMethod -Uri "$ConnectUrl/connectors/$ConnectorName/status" | ConvertTo-Json -Depth 6
}

function Initialize-Lakehouse {
    Assert-Docker
    Wait-Url "$ConnectUrl/connectors" 240
    New-Topics
    Register-Connector
    Wait-Url 'http://localhost:8181/v1/config' 120
    Wait-Flink
    Submit-Sql 'ddl/01_iceberg_tables.sql'
}

function Submit-AllJobs {
    Wait-Flink
    # Idempotent: a job that is already running is skipped, so re-running  .\tasks jobs  is safe
    # and also restarts only the ones that died. -Force submits everything regardless.
    $live = @()
    if (-not $Force) { $live = Get-LiveJobNames }
    $skipped = 0
    foreach ($f in @('10_bronze_cdc_raw', '20_silver_trips', '21_silver_pings', '23_silver_dim_versions', '30_gold_demand_supply')) {
        $pn = Get-PipelineName "jobs/$f.sql"
        if ($pn -and ($live -contains $pn)) {
            Say "skipping jobs/$f.sql: '$pn' is already running"
            $skipped++
            continue
        }
        Submit-Sql "jobs/$f.sql"
    }
    if ($skipped -gt 0) { Say "$skipped job(s) already running were left alone." }
    if ($skipped -lt 5) { Say "jobs submitted. Open $FlinkUrl to watch them." } else { Say "all jobs were already running. Open $FlinkUrl to watch them." }
}

# ---------------------------------------------------------------- Flink state, cancel, restore
function Save-FlinkState([string]$Label) {
    # Writes job states, checkpoint counters, the last completed checkpoint and the restore point of
    # every live job to evidence/. Checkpoint ids live in the MinIO volume and `fresh` deletes them,
    # so this is the only durable record (BUILD_LOG Session 9: the ids were not noted and were lost).
    $dir = Join-Path $Root 'evidence'
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $file = Join-Path $dir ('flink_state_{0}_{1}.txt' -f $Label, [DateTime]::UtcNow.ToString('HHmmss'))
    $lines = @('captured_utc=' + [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ'), "label=$Label")
    try {
        $jobs = @((Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview" -TimeoutSec 10).jobs)
        foreach ($j in ($jobs | Where-Object { $_.state -notin @('FINISHED', 'CANCELED') })) {
            $lines += "=== $($j.name) $($j.jid) state=$($j.state)"
            try {
                $c = Invoke-RestMethod -Uri "$FlinkUrl/jobs/$($j.jid)/checkpoints" -TimeoutSec 10
                $lines += 'counts: ' + ($c.counts | ConvertTo-Json -Compress)
                $lines += "latest_completed: id=$($c.latest.completed.id) path=$($c.latest.completed.external_path)"
                $lines += 'restored: ' + ($c.latest.restored | ConvertTo-Json -Compress)
                $e = Invoke-RestMethod -Uri "$FlinkUrl/jobs/$($j.jid)/exceptions" -TimeoutSec 10
                $r = $e.exceptionHistory.entries | Select-Object -First 1
                if ($r) {
                    $when = [DateTimeOffset]::FromUnixTimeMilliseconds($r.timestamp).UtcDateTime.ToString('HH:mm:ss')
                    $lines += "latest_exception: $($r.exceptionName) at $when UTC"
                }
            } catch { $lines += "(could not read checkpoints: $($_.Exception.Message))" }
        }
    } catch { $lines += "flink unreachable: $($_.Exception.Message)" }
    [IO.File]::WriteAllLines($file, $lines, (New-Object System.Text.UTF8Encoding($false)))
    Say "saved $file"
}

function Stop-Pipeline([string]$PipelineName) {
    $jobs = @((Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview").jobs | Where-Object {
        $_.name -eq $PipelineName -and $_.state -notin @('FINISHED', 'FAILED', 'CANCELED') })
    if ($jobs.Count -eq 0) { Say "no live job named '$PipelineName'"; return }
    foreach ($j in $jobs) {
        Invoke-RestMethod -Method Patch -Uri "$FlinkUrl/jobs/$($j.jid)?mode=cancel" | Out-Null
        Say "cancelled $PipelineName ($($j.jid)). Its newest checkpoint is kept; use  .\tasks restore  to resume from it."
    }
}

function Get-RetainedCheckpoint([string]$PipelineName) {
    # Newest completed checkpoint of the most recent cancelled or failed job with this name.
    $jobs = @((Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview").jobs | Where-Object {
        $_.name -eq $PipelineName -and $_.state -in @('CANCELED', 'FAILED') } | Sort-Object { $_.'end-time' } -Descending)
    foreach ($j in $jobs) {
        $c = Invoke-RestMethod -Uri "$FlinkUrl/jobs/$($j.jid)/checkpoints"
        if ($c.latest.completed.external_path) { return $c.latest.completed.external_path }
    }
    return $null
}

function Restore-Job([string]$RelPath, [string]$CheckpointPath) {
    # Resume a cancelled streaming job from its retained checkpoint. Submitting it fresh would read
    # the topics from the start again and write append-only tables a second time.
    Assert-Docker
    $pn = Get-PipelineName $RelPath
    if (-not $pn) { throw "$RelPath has no pipeline.name" }
    if (-not $Force -and ((Get-LiveJobNames) -contains $pn)) {
        throw "job '$pn' is still live. Cancel it first:  .\tasks cancel $pn"
    }
    if (-not $CheckpointPath) { $CheckpointPath = Get-RetainedCheckpoint $pn }
    if (-not $CheckpointPath) { throw "no retained checkpoint found for '$pn'; pass one:  .\tasks restore $RelPath s3://flink/checkpoints/<job-id>/chk-N" }
    $name = Render-Sql $RelPath @()
    $dir = Join-Path $Root 'flink/sql/_rendered'
    $restoreName = 'restore_' + $name
    $text = "SET 'execution.savepoint.path' = '$CheckpointPath';`n" + [IO.File]::ReadAllText((Join-Path $dir $name))
    [IO.File]::WriteAllText((Join-Path $dir $restoreName), $text, (New-Object System.Text.UTF8Encoding($false)))
    Say "restoring '$pn' from $CheckpointPath"
    Run docker compose exec -T jobmanager /opt/flink/bin/sql-client.sh -i /opt/flink/sql/init.sql -f "/opt/flink/sql/_rendered/$restoreName"
}

# ---------------------------------------------------------------- chaos
function Invoke-Chaos([string]$Action, [string]$Arg) {
    Assert-Docker
    $log = Join-Path $Root 'chaos.log'
    $secs = 60
    if ($Arg) { $secs = [int]$Arg }
    function Log([string]$m) {
        $line = ([DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')) + ' ' + $m
        Write-Host $line -ForegroundColor Yellow
        Add-Content -Path $log -Value $line -Encoding Ascii
    }
    function Pg([string]$sql) {
        Run docker compose exec -T postgres psql -U lakehouse -d marketplace -v ON_ERROR_STOP=1 -c $sql
    }
    function Watch-Recovery([long]$SinceMs, [int]$MaxSec = 600) {
        # Healthy = every live job RUNNING with a checkpoint completed after SinceMs, for 6 polls
        # (60 s) in a row. A job that restarts resets the count, so brief recoveries do not count.
        # Logs every change of state so the time to recovery is on record.
        $t0 = Get-Date; $stable = 0; $last = ''
        while (((Get-Date) - $t0).TotalSeconds -lt $MaxSec) {
            Start-Sleep -Seconds 10
            $summary = ''; $healthy = $false
            try {
                $jobs = @((Invoke-RestMethod -Uri "$FlinkUrl/jobs/overview" -TimeoutSec 5).jobs | Where-Object { $_.state -notin @('FINISHED', 'CANCELED') })
                $summary = (($jobs | Sort-Object name | ForEach-Object { "$($_.name)=$($_.state)" }) -join ' ')
                $fresh = $true
                foreach ($j in $jobs) {
                    $c = Invoke-RestMethod -Uri "$FlinkUrl/jobs/$($j.jid)/checkpoints" -TimeoutSec 5
                    if ($j.state -ne 'RUNNING' -or -not $c.latest.completed -or [long]$c.latest.completed.latest_ack_timestamp -lt $SinceMs) { $fresh = $false }
                }
                $healthy = ($jobs.Count -ge 5) -and $fresh
            } catch { $summary = 'flink unreachable' }
            if ($summary -ne $last) { Log ('jobs: ' + $summary); $last = $summary }
            if ($healthy) { $stable++ } else { $stable = 0 }
            if ($stable -ge 6) {
                Log ('RECOVERED: all jobs RUNNING with fresh checkpoints, stable 60 s, {0:N0} s after the failure ended' -f (((Get-Date) - $t0).TotalSeconds - 60))
                return
            }
        }
        Log ("NOT RECOVERED after $MaxSec s. Do not run fresh: save the state with  .\tasks flink-state  and inspect the Flink UI and the catalog log.")
    }
    function Get-NowMs { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() }
    switch ($Action) {
        'broker-loss' {
            Log "CHAOS broker-loss: stopping redpanda for ${secs}s"
            Run docker compose stop redpanda
            Start-Sleep -Seconds $secs
            Run docker compose start redpanda
            Log 'redpanda restarted. Watch: Flink job restarts, Debezium task state, consumer lag.'
        }
        'taskmanager-kill' {
            Save-FlinkState 'taskmanager-kill-before'
            Log 'CHAOS taskmanager-kill: SIGKILL taskmanager'
            Run docker compose kill -s SIGKILL taskmanager
            # `docker compose kill` counts as a manual stop, so Docker does NOT apply the container's
            # restart policy (restart: unless-stopped). Measured: exited (137), restarts=0, never came
            # back (BUILD_LOG Session 10). This script therefore plays the orchestrator and starts it
            # after a short pause, as Kubernetes or ECS would after a real crash.
            Start-Sleep -Seconds 5
            Log 'starting taskmanager again (Docker does not restart a container stopped with docker kill)'
            Run docker compose start taskmanager
            # Record whether (and when) the container comes back.
            $t0 = Get-Date; $back = $false
            for ($i = 0; $i -lt 18 -and -not $back; $i++) {
                Start-Sleep -Seconds 5
                $cid = (& docker compose ps -a -q taskmanager) | Select-Object -First 1
                $st = 'no container'
                if ($cid) { $st = (& docker inspect -f '{{.State.Status}} restarts={{.RestartCount}}' $cid) }
                Log ("taskmanager container {0:N0}s after kill: {1}" -f ((Get-Date) - $t0).TotalSeconds, $st)
                if ("$st" -like 'running*') { $back = $true }
            }
            if (-not $back) { Log 'taskmanager did NOT come back within 90 s. Start it:  docker compose up -d taskmanager' }
            $since = Get-NowMs
            Save-FlinkState 'taskmanager-kill-after'
            Watch-Recovery $since
            Save-FlinkState 'taskmanager-kill-recovered'
        }
        'connect-restart' {
            Log 'CHAOS connect-restart: restarting Debezium Connect'
            Run docker compose restart connect
            Log 'Then check bronze for duplicate payloads: sql/trino/04_verify.sql query 3'
        }
        'minio-outage' {
            if (-not $Arg) { $secs = 45 }
            Save-FlinkState 'minio-outage-before'
            Log "CHAOS minio-outage: stopping minio for ${secs}s (checkpoints and Iceberg commits will fail)"
            Run docker compose stop minio
            Start-Sleep -Seconds $secs
            Run docker compose start minio
            Log 'minio back.'
            $since = Get-NowMs
            Save-FlinkState 'minio-outage-after'
            Watch-Recovery $since
            Save-FlinkState 'minio-outage-recovered'
            Log 'Verify no loss/dups once the replay is finished and settled:  .\tasks reconcile'
        }
        'catalog-outage' {
            if (-not $Arg) { $secs = 45 }
            Save-FlinkState 'catalog-outage-before'
            Log "CHAOS catalog-outage: stopping iceberg-rest for ${secs}s"
            Run docker compose stop iceberg-rest
            Start-Sleep -Seconds $secs
            Run docker compose start iceberg-rest
            Wait-Url 'http://localhost:8181/v1/config' 120
            Log 'catalog back and answering.'
            $since = Get-NowMs
            Save-FlinkState 'catalog-outage-after'
            Watch-Recovery $since
            Save-FlinkState 'catalog-outage-recovered'
            Log 'Verify no loss/dups once the replay is finished and settled:  .\tasks reconcile'
        }
        'schema-add-column' {
            Log 'CHAOS schema-add-column: ALTER TABLE trips ADD COLUMN surge_multiplier'
            Pg 'ALTER TABLE trips ADD COLUMN IF NOT EXISTS surge_multiplier DOUBLE PRECISION;'
            Pg "UPDATE trips SET surge_multiplier = 1.5 WHERE trip_id % 10 = 0 AND status <> 'completed';"
            Log 'Expect: Debezium emits the new field; Flink ignores unknown fields, so it is silently DROPPED until silver is evolved. That silent drop IS the finding.'
        }
        'schema-rename-column' {
            Log 'CHAOS schema-rename-column: trips.distance_km -> trip_distance_km (BREAKING)'
            Pg 'ALTER TABLE trips RENAME COLUMN distance_km TO trip_distance_km;'
            Log 'Expect: distance_km becomes NULL downstream with no error. Revert: ALTER TABLE trips RENAME COLUMN trip_distance_km TO distance_km;'
        }
        'poison-message' {
            Log 'CHAOS poison-message: producing malformed JSON to driver.pings'
            '{"driver_id": 1, "seq": ' | docker compose exec -T redpanda rpk topic produce driver.pings
            Log 'Expect: jobs with json.ignore-parse-errors=false FAIL and restart-loop on that offset. Decide: DLQ vs skip vs fail-fast.'
        }
        default {
            Write-Host 'chaos actions: broker-loss [secs] | taskmanager-kill | connect-restart | minio-outage [secs] | catalog-outage [secs]'
            Write-Host '               schema-add-column | schema-rename-column | poison-message'
            throw "unknown chaos action '$Action'"
        }
    }
}

# ---------------------------------------------------------------- help
function Show-Help {
    @'

  Usage:   .\tasks <target> [args]

  First time (no Docker needed)
    venv              create .venv and install the replayer + dev tools
    test              run unit tests
    lint              ruff
    replay-dry        print 20 fake events, touch nothing

  Stack
    up                build + start all containers (first build is slow; needs internet)
    ps                container status
    logs              follow logs (Ctrl+C to stop)
    init              wait for services, then create topics, Debezium connector, Iceberg tables
    jobs              submit the 5 streaming Flink jobs (skips any that are already running; -Force overrides)
    down              stop containers, keep data
    nuke              stop containers and DELETE all data (do this between replays)
    fresh             nuke + up + init + jobs, in one go

  Data and checks
    replay            2 simulated hours of synthetic traffic at 60x (writes manifest.json)
    replay-tlc        replay real NYC TLC trips (run fetch-tlc first)
    fetch-tlc         download a month of TLC data + zone centroids into data\
    reconcile         compare the lakehouse with the replayer manifest (PASS/FAIL)
    reconcile-after   same, after the batch gold repair
    reconcile-gold    batch-rewrite one closed day of gold:   .\tasks reconcile-gold -SimDate 2025-01-15

  Query
    trino             Trino CLI (catalog iceberg)
    trino-views       create the serving views
    trino-file <f>    run a SQL file through Trino, e.g.  .\tasks trino-file sql\trino\04_verify.sql

  Failure injection
    chaos <action> [secs]    broker-loss | taskmanager-kill | connect-restart | minio-outage |
                             catalog-outage | schema-add-column | schema-rename-column | poison-message
                             (taskmanager-kill, minio-outage, catalog-outage save Flink state to evidence\
                             before and after, then wait and log the time to recovery)
    flink-state [label]      save job states + checkpoint ids to evidence\ (do this before any fresh)
    cancel <pipeline-name>   cancel one streaming job; its newest checkpoint is kept
    restore <job file> [cp]  resume a cancelled job from its retained checkpoint, e.g.
                             .\tasks restore jobs/21_silver_pings.sql   (never resubmit it fresh)
    reconcile ... --allow-bronze-extras   accept extra bronze rows (only after a Debezium restart)

  Advanced
    submit <file> [KEY=VALUE ...]   run any file under flink\sql, e.g.  .\tasks submit jobs/21_silver_pings.sql
    render <file> [KEY=VALUE ...]   only write the filled-in SQL to flink\sql\_rendered (to inspect it)
    connector                       (re)register the Debezium connector
    topics                          (re)create Kafka topics

  Custom replays - call Python directly:
    .\.venv\Scripts\python -m replayer run --seed 42 --speedup 30 --duration-min 120 --manifest manifest.json

'@ | Write-Host
}

# ---------------------------------------------------------------- dispatch
try {
    switch ($Target) {
        'help'            { Show-Help }
        'venv' {
            $py = Find-Python
            $extra = @()
            if ($py.Count -gt 1) { $extra = $py[1..($py.Count - 1)] }
            Run $py[0] @extra -m venv .venv
            $vp = Get-VenvPython
            Run $vp -m pip install -q -U pip
            Run $vp -m pip install -q -e '.[dev]'
            Say 'venv ready. Next:  .\tasks test'
        }
        'test'            { Run (Get-VenvPython) -m pytest -q }
        'lint'            { Run (Get-VenvPython) -m ruff check . }
        'replay-dry'      { Run (Get-VenvPython) -m replayer run --dry-run --speedup 0 --limit 20 }

        'up'              { Assert-Docker; Run docker compose up -d --build }
        'down'            { Assert-Docker; Run docker compose down }
        'nuke'            { Assert-Docker; Run docker compose down -v }
        'ps'              { Assert-Docker; Run docker compose ps }
        'logs'            { Assert-Docker; Run docker compose logs -f --tail=50 }
        'init'            { Initialize-Lakehouse }
        'topics'          { New-Topics }
        'connector'       { Register-Connector }
        'ddl'             { Wait-Flink; Submit-Sql 'ddl/01_iceberg_tables.sql' }
        'jobs'            { Submit-AllJobs }
        'fresh' {
            Assert-Docker
            Run docker compose down -v
            Run docker compose up -d --build
            Initialize-Lakehouse
            Submit-AllJobs
        }

        'replay'          { Run (Get-VenvPython) -m replayer run --source synth --speedup 60 --duration-min 120 --manifest manifest.json }
        'fetch-tlc'       { Run (Get-VenvPython) scripts/fetch_tlc.py --month 2024-01 --out data }
        'replay-tlc' {
            Run (Get-VenvPython) -m replayer run --source tlc --tlc-path data/yellow_tripdata_2024-01.parquet `
                --zones-csv data/zone_centroids.csv --sample 0.05 --duration-min 240 --speedup 120 --manifest manifest.json
        }
        'reconcile'       { Run (Get-VenvPython) scripts/reconcile.py --manifest manifest.json @Rest }
        'reconcile-after' { Run (Get-VenvPython) scripts/reconcile.py --manifest manifest.json --after-reconcile @Rest }
        'reconcile-gold'  { Submit-Sql 'batch/40_reconcile_gold.sql' @("DATE=$SimDate") }

        'trino'           { Assert-Docker; Run docker compose exec trino trino --catalog iceberg }
        'trino-views' {
            Assert-Docker
            Get-Content -Raw (Join-Path $Root 'sql/trino/01_serving_views.sql') | docker compose exec -T trino trino --catalog iceberg
            if ($LASTEXITCODE -ne 0) { throw "trino exited with $LASTEXITCODE" }
        }
        'trino-file' {
            Assert-Docker
            if ($Rest.Count -lt 1) { throw 'usage: .\tasks trino-file <path to .sql>' }
            Get-Content -Raw $Rest[0] | docker compose exec -T trino trino --catalog iceberg
            if ($LASTEXITCODE -ne 0) { throw "trino exited with $LASTEXITCODE" }
        }

        'flink-state' {
            $label = 'manual'
            if ($Rest.Count -gt 0) { $label = $Rest[0] }
            Save-FlinkState $label
        }
        'cancel' {
            if ($Rest.Count -lt 1) { throw 'usage: .\tasks cancel <pipeline-name>   e.g.  .\tasks cancel silver-pings' }
            Stop-Pipeline $Rest[0]
        }
        'restore' {
            if ($Rest.Count -lt 1) { throw 'usage: .\tasks restore <file under flink\sql\jobs> [s3://flink/checkpoints/<job-id>/chk-N]' }
            $cp = ''
            if ($Rest.Count -gt 1) { $cp = $Rest[1] }
            Restore-Job $Rest[0] $cp
        }

        'chaos' {
            if ($Rest.Count -lt 1) { Invoke-Chaos '' '' } else {
                $a2 = ''
                if ($Rest.Count -gt 1) { $a2 = $Rest[1] }
                Invoke-Chaos $Rest[0] $a2
            }
        }

        'submit' {
            if ($Rest.Count -lt 1) { throw 'usage: .\tasks submit <file under flink\sql> [KEY=VALUE ...]' }
            $pairs = @()
            if ($Rest.Count -gt 1) { $pairs = $Rest[1..($Rest.Count - 1)] }
            Submit-Sql $Rest[0] $pairs
        }
        'render' {
            if ($Rest.Count -lt 1) { throw 'usage: .\tasks render <file under flink\sql> [KEY=VALUE ...]' }
            $pairs = @()
            if ($Rest.Count -gt 1) { $pairs = $Rest[1..($Rest.Count - 1)] }
            $n = Render-Sql $Rest[0] $pairs
            Say "wrote flink\sql\_rendered\$n"
        }

        default {
            Write-Host "unknown target '$Target'" -ForegroundColor Red
            Show-Help
            exit 2
        }
    }
} catch {
    Write-Host ''
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
