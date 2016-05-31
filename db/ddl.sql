file -inlinebatch END_OF_BATCH

DROP PROCEDURE BatchInsert IF EXISTS;
DROP PROCEDURE BatchInsertVoltTable IF EXISTS;
DROP PROCEDURE apps_by_unique_devices IF EXISTS;
DROP PROCEDURE SelectDeviceSessions IF EXISTS;
DROP PROCEDURE insert_session IF EXISTS;
DROP VIEW app_sessions_minutely IF EXISTS;
DROP VIEW app_usage IF EXISTS;
DROP TABLE app_session IF EXISTS;

END_OF_BATCH

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE app_session (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);
PARTITION TABLE app_session ON COLUMN deviceid;

CREATE INDEX app_session_idx ON app_session (deviceid);

CREATE VIEW app_sessions_minutely AS
SELECT appid, truncate(minute,ts) as minute, count(*) as sessions
FROM app_session
GROUP BY appid, truncate(minute,ts);


CREATE VIEW app_usage AS
SELECT appid, deviceid, count(*) as ct
FROM app_session
GROUP BY appid, deviceid;

-- Define procedures
CREATE PROCEDURE apps_by_unique_devices AS
SELECT appid, COUNT(deviceid) as unique_devices, SUM(ct) as total_sessions
FROM app_usage
GROUP BY appid
ORDER BY unique_devices DESC;

CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.SelectDeviceSessions;
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.BatchInsert;
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.BatchInsertVoltTable;

END_OF_BATCH
