file -inlinebatch END_OF_BATCH

DROP PROCEDURE BatchInsert IF EXISTS;
DROP PROCEDURE BatchInsertVoltTable IF EXISTS;
DROP PROCEDURE insert_session IF EXISTS;
DROP PROCEDURE CopyToStream IF EXISTS;
DROP VIEW app_usage IF EXISTS;
DROP TABLE app_session IF EXISTS;
DROP TABLE app_session_tracking IF EXISTS;
DROP STREAM app_session_stream IF EXISTS;

END_OF_BATCH

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE app_session (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  --ts               TIMESTAMP    DEFAULT NOW
  time_nanos       BIGINT
);
PARTITION TABLE app_session ON COLUMN deviceid;
CREATE INDEX app_session_idx ON app_session (deviceid);
CREATE INDEX app_session_ts_idx ON app_session (time_nanos);

CREATE STREAM app_session_stream
PARTITION ON COLUMN deviceid
EXPORT TO TARGET archive (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  time_nanos       BIGINT
  --ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE app_session_tracking (
  dummy_deviceid   INTEGER      NOT NULL,
  last_time_nanos  BIGINT
  --last_ts          TIMESTAMP
);
PARTITION TABLE app_session_tracking ON COLUMN dummy_deviceid;


-- CREATE VIEW app_sessions_minutely AS
-- SELECT appid, truncate(minute,ts) as minute, count(*) as sessions
-- FROM app_session
-- GROUP BY appid, truncate(minute,ts);


CREATE VIEW app_usage AS
SELECT appid, deviceid, count(*) as ct
FROM app_session
GROUP BY appid, deviceid;

CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.BatchInsert;
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.BatchInsertVoltTable;
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.CopyToStream;

END_OF_BATCH
