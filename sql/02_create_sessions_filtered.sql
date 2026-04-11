-- 02_create_sessions_filtered.sql
-- Purpose: keep only active users and create the filtered session-level working table
-- Dialect: Spark SQL / Databricks SQL

CREATE OR REPLACE TABLE workspace.default.sessions_filtered_sk
USING DELTA
AS
WITH filtered_users AS (
    SELECT
        user_id
    FROM sessions_joined_sk
    WHERE session_start > '2023-01-04'
    GROUP BY user_id
    HAVING COUNT(*) > 7
)

SELECT *
FROM sessions_joined_sk
WHERE session_start > '2023-01-04'
  AND user_id IN (SELECT user_id FROM filtered_users);