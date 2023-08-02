
# Temporarily disable index
https://fle.github.io/temporarily-disable-all-indexes-of-a-postgresql-table.html

This is untested, but kept here for future reference to potentially speed up the import.

```sql
UPDATE pg_index
SET indisready=false
WHERE indrelid = (
    SELECT oid
    FROM pg_class
    WHERE relname='<TABLE_NAME>'
);

UPDATE <TABLE_NAME> SET ...;

UPDATE pg_index
SET indisready=true
WHERE indrelid = (
    SELECT oid
    FROM pg_class
    WHERE relname='<TABLE_NAME>'
);

REINDEX <TABLE_NAME>;

```
