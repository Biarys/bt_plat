
-- show hanging queries
SELECT
  datname,
  NOW() - query_start AS duration,
  pid,
  query
FROM
  pg_stat_activity
WHERE
    query <> '<IDLE>'
  AND
  NOW() - query_start > '1 second'
ORDER BY duration DESC;

-- show hanging sessions
SELECT t.schemaname,
  t.relname,
  l.locktype,
  l.page,
  l.virtualtransaction,
  l.pid,
  l.mode,
  l.granted
FROM pg_locks l
  JOIN pg_stat_all_tables t ON l.relation = t.relid
WHERE t.schemaname <> 'pg_toast'
::name AND t.schemaname <> 'pg_catalog'::name
  ORDER BY t.schemaname, t.relname;


-- drop hanging sessions
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'TARGET_DB' -- ← change this to your DB
  AND pid <> pg_backend_pid();



--What I did is first check what are the running processes by

SELECT *
FROM pg_stat_activity
WHERE state = 'active';

--Find the process you want to kill, then type:

SELECT pg_cancel_backend(<pid>)

--If the process cannot be killed, try:

SELECT pg_terminate_backend(<pid>)