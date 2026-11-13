@echo off
rem Wrapper so you can type .\tasks <target> from cmd.exe or PowerShell without changing
rem the execution policy. The real logic lives in scripts\tasks.ps1.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\tasks.ps1" %*
exit /b %ERRORLEVEL%
