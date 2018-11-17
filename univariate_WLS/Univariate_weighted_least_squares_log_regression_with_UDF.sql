-- Univariate Weighted Least Squares Regression (with logarithmic transformation of X)
-- Tested on snowflake
-- Adapt alpha function to make it multivariate!

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


/* ########################################
-- Intermediate table
######################################## */

CREATE OR REPLACE TEMPORARY TABLE aggregate.tmp_intermediate_table AS (
  SELECT
    partition_window
    , X_raw
    , LOG(10, X_raw + 1) X
    , y
    , weights
    , DEMEAN(y, weights, partition_window) y_demeaned
    , DEMEAN(X, weights, partition_window) X_demeaned
  FROM aggregate.tmp_raw
);

/* ########################################
-- Calculate beta and alpha
-- Make predictions
######################################## */

CREATE OR REPLACE TEMPORARY TABLE aggregate.tmp_predictions AS (
  SELECT
    partition_window
    , weights
    , X
    , y
    , SUM(y * weights) OVER(PARTITION BY partition_window, X) / SUM(weights) OVER(PARTITION BY partition_window, X) observed_y_over_partition
    , CALCULATE_WEIGHTED_BETA(y_demeaned, NULLIF(X_demeaned,0), weights, partition_window) beta
    , CALCULATE_WEIGHTED_ALPHA(y, X, beta, weights, partition_window) alpha
    , GREATEST(alpha + beta*X,0) wls_prediction
  FROM aggregate.tmp_intermediate_table
);

/* ########################################
-- Measure performance (optional)
######################################## */

CREATE OR REPLACE TABLE aggregate.est_city_conversion_curve AS (
  SELECT
    CURRENT_TIMESTAMP(0) date_time
    , partition_window
    , weights
    , X
    , y
    , observed_y_over_partition
    , beta
    , alpha
    , wls_prediction
    , CALCULATE_WMAE(observed_y_over_partition, wls_prediction, weights, partition_window) weighted_mae_partition
    , CALCULATE_WMAPE(observed_y_over_partition, wls_prediction, weights, partition_window) weighted_mape_partition
  FROM aggregate.tmp_predictions
  ORDER BY 1,3
);