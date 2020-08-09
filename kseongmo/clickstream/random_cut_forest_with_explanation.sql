-- ** Anomaly detection **
-- Compute an anomaly score for each record in the source stream using Random Cut Forest
-- Creates a temporary stream and defines a schema
CREATE OR REPLACE STREAM "TEMP_STREAM" (
   "TIMESTAMPS"          TIMESTAMP,
   "CLICKSTREAM_ID"      INTEGER,
   "URL"                 INTEGER,
   "IS_PURCHASED"        INTEGER,
   "IS_PAGE_ERRORED"     DOUBLE,
   "USER_SESSION_ID"     INTEGER,
   "CITY"                INTEGER,
   "STATE"               INTEGER,
   "COUNTRY"             INTEGER,
   "BIRTH_DT"            INTEGER,
   "GENDER_CD"           INTEGER,
   "ANOMALY_SCORE"       DOUBLE,
   "ANOMALY_EXPLANATION" varchar(512));
-- Creates an output stream and defines a schema
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
   "TIMESTAMPS"          TIMESTAMP,
   "CLICKSTREAM_ID"      INTEGER,
   "URL"                 INTEGER,
   "IS_PURCHASED"        INTEGER,
   "IS_PAGE_ERRORED"     DOUBLE,
   "USER_SESSION_ID"     INTEGER,
   "CITY"                INTEGER,
   "STATE"               INTEGER,
   "COUNTRY"             INTEGER,
   "BIRTH_DT"            INTEGER,
   "GENDER_CD"           INTEGER,
   "ANOMALY_SCORE"       DOUBLE,
   "ANOMALY_EXPLANATION" varchar(512));


-- RANDOM_CUT_FOREST anomaly score is a number between 0 and LOG2(subSampleSize)
-- See RANDOM_CUT_FOREST anomaly score explanation https://forums.aws.amazon.com/message.jspa?messageID=751928
-- Normalize the "ANOMALY_SCORE" by dividing it by LOG2(subSampleSize)
--   "shingleSize": 4, 24, 48
--    "numberOfTrees" : 100, 200

CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "TEMP_STREAM"
SELECT STREAM "TIMESTAMPS", "CLICKSTREAM_ID", "URL", "IS_PURCHASED", "IS_PAGE_ERRORED", "USER_SESSION_ID", 
              "CITY", "STATE", "COUNTRY", "BIRTH_DT", "GENDER_CD", ANOMALY_SCORE, ANOMALY_EXPLANATION FROM
  TABLE(RANDOM_CUT_FOREST_WITH_EXPLANATION(
    CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001"), -- inputStream
    100, -- numberOfTrees
    256, -- subSampleSize
    100000, -- timeDecay
    1, -- shingleSize
    true
  )
);

CREATE OR REPLACE PUMP "OUTPUT_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM * FROM "TEMP_STREAM"
ORDER BY FLOOR("TEMP_STREAM".ROWTIME TO SECOND), ANOMALY_SCORE DESC;


