-- Separate database for the Iceberg REST catalog's metadata (tables, namespaces, pointers).
-- Why not SQLite: it is single-writer. Five Flink jobs commit about every 30 s each, and the
-- SQLite file returned SQLITE_BUSY (HTTP 500) when commits collided, which put every job in a
-- restart loop after a catalog restart (docs/BUILD_LOG.md, Session 9). Postgres handles
-- concurrent writers with row-level locks. The catalog creates its own tables on first start.
-- Runs only when the pgdata volume is empty (docker-entrypoint-initdb.d); `.\tasks fresh` does that.
CREATE DATABASE iceberg_catalog;
