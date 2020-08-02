-- ** Anomaly detection **
-- Compute an anomaly score for each record in the source stream using Random Cut Forest
-- Creates a temporary stream and defines a schema
CREATE OR REPLACE STREAM "TEMP_STREAM" (
   "TIMESTAMPS"        TIMESTAMP,
   "IP_ADDRESS"        VARCHAR(50),
   "URL"               VARCHAR(50),
   "IS_PURCHASED"      INTEGER,
   "IS_PAGE_ERRORED"   DOUBLE,
   "USER_SESSION_ID"   VARCHAR(100),
   "CITY"              VARCHAR(100),
   "STATE"             VARCHAR(10),
   "COUNTRY"           VARCHAR(20),
   "BIRTH_DT"          VARCHAR(20),
   "GENDER_CD"         VARCHAR(5),
   "ANOMALY_SCORE"     DOUBLE);
-- Creates an output stream and defines a schema
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
   "TIMESTAMPS"        TIMESTAMP,
   "IP_ADDRESS"        VARCHAR(50),
   "URL"               VARCHAR(50),
   "IS_PURCHASED"      INTEGER,
   "IS_PAGE_ERRORED"   DOUBLE,
   "USER_SESSION_ID"   VARCHAR(100),
   "CITY"              VARCHAR(100),
   "STATE"             VARCHAR(10),
   "COUNTRY"           VARCHAR(20),
   "BIRTH_DT"          VARCHAR(20),
   "GENDER_CD"         VARCHAR(5),
   "ANOMALY_SCORE"     DOUBLE);



-- RANDOM_CUT_FOREST anomaly score is a number between 0 and LOG2(subSampleSize)
-- See RANDOM_CUT_FOREST anomaly score explanation https://forums.aws.amazon.com/message.jspa?messageID=751928
-- Normalize the "ANOMALY_SCORE" by dividing it by LOG2(subSampleSize)
--   "shingleSize": 4, 24, 48
--    "numberOfTrees" : 100, 200

CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "TEMP_STREAM"
SELECT STREAM "TIMESTAMPS", "IP_ADDRESS", "URL", "IS_PURCHASED", 
              "IS_PAGE_ERRORED", "USER_SESSION_ID", "CITY", "STATE",
              "COUNTRY", "BIRTH_DT", "GENDER_CD", 
              "ANOMALY_SCORE" FROM
  TABLE(RANDOM_CUT_FOREST(
    CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001"), -- inputStream
    100, -- numberOfTrees
    256, -- subSampleSize
    100000, -- timeDecay
    1 -- shingleSize
  )
);

CREATE OR REPLACE PUMP "OUTPUT_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM * FROM "TEMP_STREAM"
ORDER BY "TEMP_STREAM".ROWTIME ASC;


