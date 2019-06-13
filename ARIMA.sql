/* ################################################################################
-- USER DEFINED ARI(MA) MODELLING
################################################################################ */

-- SKIP TO BOTTOM TO SEE EXAMPLE (RELIES ON FUNCTIONS IN WLS.SQL)
-- Can currently only do ARIMA(p1,d,0)(p2,0,0) where p1 in {0,1,2}, d in {0,1}, p2 in {0,1}
-- (Estimated using WLS not MLE as cannot do loops on snowflake)

/* ########################################
-- DEFINE TABLE UDFs
######################################## */

-- This is just a table created for use in the example below
  CREATE TABLE IF NOT EXISTS udf.arima_raw AS (
      SELECT
        partition_variable
        , time_series
        , y
        , ROW_NUMBER() OVER(ORDER BY partition_variable, time_series)::INT id
      FROM raw_data
  );

-- SET UP REQUIRED PARAMETERS
  SET udf_arima_raw = 'udf.arima_raw';
  SET AR1 = 1;
  SET AR1_l1 = 2;
  SET AR2 = 2;
  SET AR2_l1 = 3;
  SET seasonal = 52;
  SET seasonal_l1 = 53;

-- SET UP DATA FOR ARIMA MODELLING THAT FITS INTO WLS UDTFs
  CREATE OR REPLACE FUNCTION udf.setup_ARIMA(integrated BOOLEAN, autoregressive VARCHAR)
      RETURNS TABLE (id VARCHAR
                     , time_series TIMESTAMP, partition_variable VARCHAR, weight FLOAT
                     , orig_y FLOAT, y FLOAT
                     , x1 FLOAT, x2 FLOAT, x3 FLOAT, x4 FLOAT, x5 FLOAT)
        AS
        $$
        WITH parameters AS (
          SELECT
              *
            , integrated AS integrated
            , autoregressive AS autoregressive
          FROM TABLE($udf_arima_raw)
          )
        SELECT
            id::VARCHAR
          , time_series::TIMESTAMP
          , partition_variable::VARCHAR
          , 1::FLOAT
          , y::FLOAT AS orig_y
          , CASE WHEN integrated
                 THEN y - LAG(y,1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                 ELSE y
                 END::FLOAT AS y
          , CASE WHEN (autoregressive = 'AR1' OR autoregressive = 'AR2')
                 THEN CASE WHEN integrated
                           THEN LAG(y,$AR1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                                - LAG(y,$AR1_l1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           ELSE LAG(y,$AR1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           END
                 ELSE 0
                 END::FLOAT AS x1
          , CASE WHEN autoregressive = 'AR2'
                 THEN CASE WHEN integrated
                           THEN LAG(y,$AR2) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                                - LAG(y,$AR2_l1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           ELSE LAG(y,$AR2) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           END
                 ELSE 0
                 END::FLOAT AS x2
          , CASE WHEN $seasonal != 0
                 THEN CASE WHEN integrated
                           THEN LAG(y,$seasonal) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                                - LAG(y,$seasonal_l1) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           ELSE LAG(y,$seasonal) OVER(PARTITION BY partition_variable ORDER BY time_series ASC)
                           END
                 ELSE 0
                 END::FLOAT AS x3
          , 0::FLOAT AS x4
          , 0::FLOAT AS x5
        FROM parameters
        $$;

GRANT SELECT ON udf.arima_raw TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.setup_ARIMA(BOOLEAN, VARCHAR) TO ROLE SCHEMA_UDF;

/* ####################
-- EXAMPLE
#################### */

-- Select table to regress on
-- Required columns: id, time_series, partition_variable, y
-- Required function variables:
   -- Integrated (BOOLEAN for whether to do first differencing or not)
   -- Auto-regressive (VARCHAR either 'AR1' or 'AR2')
-- Required parameters:
   -- Raw table name
      SET udf_arima_raw = 'udf.arima_raw';
   -- Auto-regressive terms (Only AR1 or AR2)
      SET AR1 = 13; -- only needed if autoregressive IN ('AR1','AR2'). If set to anything else, then these are dropped.
      SET AR1_l1 = 14; -- only needed if autoregressive IN ('AR1','AR2') AND integrated = TRUE
      SET AR2 = 14; -- only needed if autoregressive = 'AR2'. If set to anything else, then these are dropped.
      SET AR2_l1 = 15; -- only needed if autoregressive = 'AR2' AND integrated = TRUE
   -- Seasonal terms (only AR)
      -- Auto-regressive terms
         SET seasonal = 52; -- Set to zero if seasonal terms are not needed
         SET seasonal_l1 = 53; -- Set to zero if seasonal terms are not needed. Only needed if integrated = TRUE
-- Then run formula to setup ARIMA!
CREATE TABLE IF NOT EXISTS udf.arima_raw_regress AS (
  SELECT *
  FROM TABLE(udf.setup_ARIMA(TRUE, 'AR1'))
);
-- This will now play nicely with the WLS udf function :)
-- (see udf/WLS.sql for more details)
SET wls_raw_data = 'udf.arima_raw_regress';
CREATE TABLE IF NOT EXISTS udf.arima_prediction AS (
  WITH wls_regress AS (
    SELECT *
    FROM TABLE(udf.WLS_no_int())
  )
  SELECT
      raw.y
    , raw.time_series
    , raw.partition_variable
    , LAG(raw.y,$AR1) OVER(PARTITION BY raw.partition_variable ORDER BY raw.time_series ASC) + prediction AS arima_prediction
  FROM udf.arima_raw raw
  LEFT JOIN wls_regress wls
    ON raw.id = wls.id
  ORDER BY raw.id::INT
);

GRANT SELECT ON udf.arima_raw_regress TO ROLE SCHEMA_UDF;
GRANT SELECT ON udf.arima_prediction TO ROLE SCHEMA_UDF;
