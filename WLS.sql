/* ################################################################################
-- USER DEFINED MULTIVARIATE WLS REGRESSION FUNCTIONS
################################################################################ */

-- SKIP STRAIGHT TO ROW 250 FOR EXAMPLE

/* ########################################
-- DEFINE INLINE UDFs
######################################## */

-- Weighted mean
  CREATE OR REPLACE FUNCTION
    udf.WEIGHTED_MEAN(x FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  (SUM(CASE WHEN X IS NOT NULL THEN X*weight END) OVER(PARTITION BY partition_variable))
  /
  (SUM(CASE WHEN X IS NOT NULL THEN weight END) OVER(PARTITION BY partition_variable))
  $$;

-- Weighted de-mean
  CREATE OR REPLACE FUNCTION
    udf.DEMEAN(x FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  X - udf.WEIGHTED_MEAN(x, weight, partition_variable)
  $$;

-- Weighted beta calculation
  CREATE OR REPLACE FUNCTION
    udf.CALCULATE_WEIGHTED_BETA(y_demeaned FLOAT, X_demeaned FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  SUM(CASE WHEN X_demeaned IS NOT NULL AND y_demeaned IS NOT NULL
           THEN weight * X_demeaned * y_demeaned
           END) OVER(PARTITION BY partition_variable)
  /
  SUM(CASE WHEN X_demeaned IS NOT NULL AND y_demeaned IS NOT NULL
           THEN weight * POW(X_demeaned,2)
           END) OVER(PARTITION BY partition_variable)
  $$;

-- Weighted alpha calculation
  CREATE OR REPLACE FUNCTION
    udf.CALCULATE_WEIGHTED_ALPHA(y FLOAT
                                 , X1 FLOAT, beta1 FLOAT
                                 , X2 FLOAT, beta2 FLOAT
                                 , X3 FLOAT, beta3 FLOAT
                                 , X4 FLOAT, beta4 FLOAT
                                 , X5 FLOAT, beta5 FLOAT
                                 , weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  udf.WEIGHTED_MEAN(y,weight,partition_variable) - (beta1 * udf.WEIGHTED_MEAN(X1,weight,partition_variable))
                                                 - (beta2 * udf.WEIGHTED_MEAN(X2,weight,partition_variable))
                                                 - (beta3 * udf.WEIGHTED_MEAN(X3,weight,partition_variable))
                                                 - (beta4 * udf.WEIGHTED_MEAN(X4,weight,partition_variable))
                                                 - (beta5 * udf.WEIGHTED_MEAN(X5,weight,partition_variable))
  $$;

-- Weighted beta calculation without an intercept
  CREATE OR REPLACE FUNCTION
    udf.CALCULATE_WEIGHTED_BETA_NO_INT(y FLOAT, X FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  SUM(CASE WHEN X IS NOT NULL AND y IS NOT NULL
           THEN weight * X * y
           END) OVER(PARTITION BY partition_variable)
  /
  SUM(CASE WHEN X IS NOT NULL and y IS NOT NULL
           THEN weight * POW(X,2)
           END) OVER(PARTITION BY partition_variable)
  $$;

-- Weighted mean absolute error
  CREATE OR REPLACE FUNCTION
    udf.CALCULATE_WMAE(y FLOAT, y_hat FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  SUM((ABS(y_hat - y))*weight) OVER(PARTITION BY partition_variable) / SUM(weight) OVER(PARTITION BY partition_variable)
  $$;

-- Weighted mean absolute percentage error
  CREATE OR REPLACE FUNCTION
    udf.CALCULATE_WMAPE(y FLOAT, y_hat FLOAT, weight FLOAT, partition_variable VARCHAR)
  returns FLOAT
  AS
  $$
  SUM(ABS((y_hat - y)/NULLIF(y,0)) * (CASE WHEN y_hat IS NULL THEN 0 ELSE weight END)) OVER(PARTITION BY partition_variable)
  /
  SUM((CASE WHEN y_hat IS NULL OR y IS NULL THEN 0 ELSE weight END)) OVER(PARTITION BY partition_variable)
  $$;

-- Weighted decay functions
  CREATE OR REPLACE FUNCTION udf.exponential_decay(time_series DATE, paritition_variable VARCHAR, scale_constant FLOAT, time_constant FLOAT)
  returns FLOAT
  AS
  $$
  scale_constant * EXP(-(RANK() OVER(PARTITION BY paritition_variable ORDER BY time_series DESC))/time_constant)
  $$;


/* ########################################
-- DEFINE TABLE UDFs
######################################## */

-- This is just a table created for use in the example below
CREATE TABLE IF NOT EXISTS udf.wls_raw_example AS (
   SELECT
       partition_variable
     , ROW_NUMBER() OVER(ORDER BY partition_variable) AS id
     , y
     , LOG(10, X+1) X1
     , 0::FLOAT AS X2
     , 0::FLOAT AS X3
     , 0::FLOAT AS X4
     , 0::FLOAT AS X5
     , (SUM(weight))::FLOAT AS weight
   FROM raw_data
);

SET wls_raw_data = 'udf.wls_raw_example';

-- Weighted Least Squares (with an intercept)
CREATE OR REPLACE FUNCTION udf.WLS()
    RETURNS TABLE (id VARCHAR
                   , y FLOAT, X1 FLOAT, X2 FLOAT, X3 FLOAT, X4 FLOAT, X5 FLOAT
                   , weight FLOAT, partition_variable VARCHAR
                   , wls_beta1 FLOAT, wls_beta2 FLOAT, wls_beta3 FLOAT, wls_beta4 FLOAT, wls_beta5 FLOAT
                   , alpha FLOAT
                   , prediction FLOAT
                   , wmae FLOAT, wmape FLOAT)
      AS
      $$
      WITH demeaned AS (
        SELECT
            id
          , y
          , X1
          , X2
          , X3
          , X4
          , X5
          , weight
          , partition_variable
          , udf.DEMEAN(X1, weight, partition_variable) X1_demeaned
          , udf.DEMEAN(X2, weight, partition_variable) X2_demeaned
          , udf.DEMEAN(X3, weight, partition_variable) X3_demeaned
          , udf.DEMEAN(X4, weight, partition_variable) X4_demeaned
          , udf.DEMEAN(X5, weight, partition_variable) X5_demeaned
          , udf.DEMEAN(y, weight, partition_variable) y_demeaned
        FROM TABLE($wls_raw_data)
      )
      , prediction AS (
        SELECT
            id::VARCHAR
          , y
          , X1
          , X2
          , X3
          , X4
          , X5
          , weight
          , partition_variable
          , udf.CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X1_demeaned,0), weight, partition_variable) wls_beta1
          , udf.CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X2_demeaned,0), weight, partition_variable) wls_beta2
          , udf.CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X3_demeaned,0), weight, partition_variable) wls_beta3
          , udf.CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X4_demeaned,0), weight, partition_variable) wls_beta4
          , udf.CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X5_demeaned,0), weight, partition_variable) wls_beta5
          , udf.CALCULATE_WEIGHTED_ALPHA(y, X1, COALESCE(wls_beta1,0)
                                          , X2, COALESCE(wls_beta2,0)
                                          , X3, COALESCE(wls_beta3,0)
                                          , X4, COALESCE(wls_beta4,0)
                                          , X5, COALESCE(wls_beta5,0)
                                          , weight, partition_variable) wls_alpha
          , (wls_alpha*1
             + COALESCE(wls_beta1,0)*X1
             + COALESCE(wls_beta2,0)*X2
             + COALESCE(wls_beta3,0)*X3
             + COALESCE(wls_beta4,0)*X4
             + COALESCE(wls_beta5,0)*X5) y_hat
        FROM demeaned)
      SELECT
          *
        , udf.CALCULATE_WMAE(y, y_hat, weight, partition_variable) wmae
        , udf.CALCULATE_WMAPE(y, y_hat, weight, partition_variable) wmape
      FROM prediction
      $$;

-- Weighted Least Squares (without an intercept - forcing through zero)
CREATE OR REPLACE FUNCTION udf.WLS_no_int()
    RETURNS TABLE (id VARCHAR
                   , y FLOAT, X1 FLOAT, X2 FLOAT, X3 FLOAT, X4 FLOAT, X5 FLOAT
                   , weight FLOAT, partition_variable VARCHAR
                   , wls_beta1 FLOAT, wls_beta2 FLOAT, wls_beta3 FLOAT, wls_beta4 FLOAT, wls_beta5 FLOAT
                   , prediction FLOAT
                   , wmae FLOAT, wmape FLOAT)
      AS
      $$
      WITH prediction AS (
        SELECT
            id::VARCHAR
          , y
          , X1
          , X2
          , X3
          , X4
          , X5
          , weight
          , partition_variable
          , udf.CALCULATE_WEIGHTED_BETA_NO_INT(y, NULLIF(X1,0), weight, partition_variable) wls_beta1
          , udf.CALCULATE_WEIGHTED_BETA_NO_INT(y, NULLIF(X2,0), weight, partition_variable) wls_beta2
          , udf.CALCULATE_WEIGHTED_BETA_NO_INT(y, NULLIF(X3,0), weight, partition_variable) wls_beta3
          , udf.CALCULATE_WEIGHTED_BETA_NO_INT(y, NULLIF(X4,0), weight, partition_variable) wls_beta4
          , udf.CALCULATE_WEIGHTED_BETA_NO_INT(y, NULLIF(X5,0), weight, partition_variable) wls_beta5
          , (COALESCE(wls_beta1,0)*X1
             + COALESCE(wls_beta2,0)*X2
             + COALESCE(wls_beta3,0)*X3
             + COALESCE(wls_beta4,0)*X4
             + COALESCE(wls_beta5,0)*X5) y_hat
        FROM TABLE($wls_raw_data))
      SELECT
          *
        , udf.CALCULATE_WMAE(y, y_hat, weight, partition_variable) wmae
        , udf.CALCULATE_WMAPE(y, y_hat, weight, partition_variable) wmape
      FROM prediction
      $$;

GRANT SELECT ON udf.wls_raw_example TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.WEIGHTED_MEAN(FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.DEMEAN(FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.CALCULATE_WEIGHTED_BETA(FLOAT, FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.CALCULATE_WEIGHTED_ALPHA(FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.CALCULATE_WEIGHTED_BETA_NO_INT(FLOAT, FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.CALCULATE_WMAE(FLOAT, FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.CALCULATE_WMAPE(FLOAT, FLOAT, FLOAT, VARCHAR) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.EXPONENTIAL_DECAY(DATE, VARCHAR, FLOAT, FLOAT) TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.WLS() TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.WLS_no_int() TO ROLE SCHEMA_UDF;

/* ####################
-- EXAMPLE
#################### */

-- Select table to regress on (the table is set up in the query above)
-- Required columns: y, X1, X2, X3, X4, X5, weight, partition_variable (set any non-required columns to 1)
SET wls_raw_data = 'udf.wls_raw_example';
-- Then just run weighted-least-squares regression!
CREATE TABLE IF NOT EXISTS udf.wls_regressed_example AS (
  SELECT *
  FROM TABLE(udf.WLS())
  ORDER BY id::INT
);

GRANT SELECT ON udf.wls_regressed_example TO ROLE SCHEMA_UDF;
