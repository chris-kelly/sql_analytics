-- Univariate Weighted Least Squares Regression (with logarithmic transformation of X)
-- Tested on snowflake

/* ########################################
-- User defined functions
######################################## */

CREATE OR REPLACE FUNCTION demean(x float, weight float)
returns FLOAT
AS
$$
X - (SUM(X*weight) OVER()) / (SUM(weight) OVER())
$$;

CREATE OR REPLACE FUNCTION calculate_weighted_beta(y_demeaned float, X_demeaned float, weight float)
returns FLOAT
AS
$$
SUM(weight * X_demeaned * y_demeaned) OVER() / SUM(weight * POW(X_demeaned,2)) OVER()
$$;

CREATE OR REPLACE FUNCTION calculate_weighted_alpha(y float, X float, beta float, weight float)
returns FLOAT
AS
$$
(SUM(y*weight) OVER()) / (SUM(weight) OVER()) - (beta * (SUM(X*weight) OVER()) / (SUM(weight) OVER()))
$$;

CREATE OR REPLACE FUNCTION calculate_wmape(y float, y_hat float, weight float)
returns FLOAT
AS
$$
100 * SUM((ABS(y_hat - y)/y)*weight) OVER() /SUM(weight) OVER()
$$;

SET rounding_level = 2;

/* ########################################
-- Import raw data
######################################## */

WITH raw_data AS (
  SELECT
    -- X
    round(X/$rounding_level)*$rounding_level AS X_rounded -- optional
    , y
    , weights
  FROM <RAW_TABLE> AS raw_data
)

/* ########################################
-- Intermediate table
######################################## */

, intermediate_table AS (
  SELECT
    LOG(10, X_rounded + 1) X2 -- optional
    , y
    , weights
    , DEMEAN(y, weights) y_demeaned
    , DEMEAN(X2, weights) X2_demeaned
    , calculate_weighted_beta(y, X2, weights)
  FROM raw_data
)

/* ########################################
-- Calculate beta and alpha
-- Make predictions
######################################## */

, predictions AS (
  SELECT
    weights
    , X2
    , y
    , calculate_weighted_beta(y_demeaned, X2_demeaned, weights) beta
    , calculate_weighted_alpha(y, X2, beta, weights) alpha
    , alpha + beta*X2 wls_prediction
  FROM intermediate_table
)

/* ########################################
-- Measure performance
######################################## */

SELECT
  *
  , calculate_wmape(y, wls_prediction, weights) weighted_mape
FROM predictions