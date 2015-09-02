file -inlinebatch END_OF_BATCH

DROP PROCEDURE apps_by_unique_devices IF EXISTS;
DROP PROCEDURE SelectDeviceSessions IF EXISTS;
DROP VIEW app_sessions_minutely IF EXISTS;
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
SELECT appid, truncate(minute,ts) as minute, count(*)
FROM app_session
GROUP BY appid, truncate(minute,ts);

CREATE PROCEDURE apps_by_unique_devices AS
SELECT appid, COUNT(deviceid) as unique_devices, SUM(ct) as total_sessions
FROM
(
SELECT appid, deviceid, count(*) as ct
FROM app_session
GROUP BY appid, deviceid
) a
GROUP BY appid
ORDER BY unique_devices DESC;

-- Define procedures
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS procedures.SelectDeviceSessions;

END_OF_BATCH
