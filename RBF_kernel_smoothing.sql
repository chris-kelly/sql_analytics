/* ################################################################################
 -- RADIAL BASIS FUNCTIONS, KERNEL SMOOTHING USER DEFINED (TABLE) FUNCTIONS (UDTFs)
 ################################################################################ */

/* ####################
-- MIN_MAX_SCALING UDFs
#################### */

CREATE OR REPLACE FUNCTION udf.MIN_MAX_SCALER(x FLOAT, partition_variable VARCHAR)
returns FLOAT
AS
$$
(x - MIN(x) OVER(PARTITION BY partition_variable)) / NULLIF((MAX(x) OVER(PARTITION BY partition_variable) - MIN(x) OVER(PARTITION BY partition_variable)),0)
$$;

CREATE OR REPLACE FUNCTION udf.UNDO_MIN_MAX_SCALER(scaled_X FLOAT, x FLOAT, partition_variable VARCHAR)
returns FLOAT
AS
$$
(scaled_X * (MAX(x) OVER(PARTITION BY partition_variable) - MIN(x) OVER(PARTITION BY partition_variable))) + (MIN(x) OVER(PARTITION BY partition_variable))
$$;

/* ####################
-- 2D SIGMA UDF
#################### */

CREATE OR REPLACE FUNCTION udf.SIGMA_1D(demeaned_x FLOAT, partition_variable VARCHAR)
returns FLOAT
AS
$$
SQRT(
  (
  SUM(POW(demeaned_x,2)) OVER(PARTITION BY partition_variable)
  )/(
  NULLIF(COUNT(*) OVER(PARTITION BY partition_variable),0)-1
  )
)
$$;

CREATE OR REPLACE FUNCTION udf.SIGMA_2D(demeaned_x FLOAT, demeaned_y FLOAT, partition_variable VARCHAR)
returns FLOAT
AS
$$
SQRT(
  (
  SUM(POW(demeaned_x,2)+POW(demeaned_y,2)) OVER(PARTITION BY partition_variable)
  )/(
  NULLIF(COUNT(*) OVER(PARTITION BY partition_variable),0)-2
  )
)
$$;

/* ####################
-- RADIAL BASIS FUNCTION UDF
#################### */

CREATE OR REPLACE FUNCTION udf.LINEAR_KERNEL_2D(x1_a FLOAT, x1_b FLOAT, x2_a FLOAT, x2_b FLOAT)
returns FLOAT
AS
$$
SQRT(POW(x1_a-x1_b,2) + POW(x2_a-x2_b,2))
$$;

CREATE OR REPLACE FUNCTION udf.EXP_KERNEL_2D(x1_a FLOAT, x1_b FLOAT, x2_a FLOAT, x2_b FLOAT, std_dev_2d FLOAT, sensitivity FLOAT)
returns FLOAT
AS
$$
EXP(-1*(POW(x1_a-x1_b,2) + POW(x2_a-x2_b,2))/(2*std_dev_2d*sensitivity))
$$;


/* ####################
-- EXPONENTIAL KERNEL 2D SMOOTHING UDTF
#################### */

CREATE TABLE IF NOT EXISTS udf.test_1d_kernel AS (
  SELECT
      COLUMN1 AS id
    , COLUMN2 AS partition_variable
    , COLUMN3 AS x1
    , COLUMN4 AS x2
  FROM(VALUES
      (1,1,1,0.8948),
      (2,1,4,0.9946),
      (3,1,7,0.9548),
      (4,1,10,1.3087),
      (5,1,13,1.1497),
      (6,1,16,0.6164),
      (7,1,19,0.6708),
      (8,1,22,0.3161),
      (9,1,25,0.0243),
      (10,1,28,0.2343),
      (11,1,31,-0.0296),
      (12,1,34,-0.2793),
      (13,1,37,-0.4333),
      (14,1,40,-0.1464),
      (15,1,43,-0.3993),
      (16,1,46,-0.2473),
      (17,1,49,0.2918),
      (18,1,52,0.1291),
      (19,1,55,0.1974),
      (20,1,58,0.0123),
      (21,1,61,0.4546),
      (22,1,64,-0.0591),
      (23,1,67,-0.3456),
      (24,1,70,-0.0251),
      (25,1,73,-0.502),
      (26,1,76,-0.7766),
      (27,1,79,-0.8863),
      (28,1,82,-0.8879),
      (29,1,85,-1.4002),
      (30,1,88,-0.9557),
      (31,1,91,-1.1226),
      (32,1,94,-0.8662),
      (33,1,97,-0.4899),
      (34,1,100,-0.7449),
      (35,1,103,0.0801),
      (36,1,106,0.0704),
      (37,1,109,0.847),
      (38,1,112,0.9089),
      (39,1,115,1.3258),
      (40,1,118,2.0204),
      (41,1,121,2.0007),
      (42,1,124,2.0822),
      (43,1,127,1.6692),
      (44,1,130,1.785),
      (45,1,133,1.2123),
      (46,1,136,0.6122),
      (47,1,139,0.1124),
      (48,1,142,-0.3847),
      (49,1,145,-0.8045),
      (50,1,148,-0.993),
      (51,1,151,-1.4895),
      (52,1,154,-1.7689),
      (53,1,157,-1.7233),
      (54,1,160,-1.7441),
      (55,1,163,-2.0915),
      (56,1,166,-1.6653),
      (57,1,169,-1.1232),
      (58,1,172,-0.8498),
      (59,1,175,-0.8237),
      (60,1,178,-0.066),
      (61,1,181,0.1428),
      (62,1,184,0.4754),
      (63,1,187,0.9993),
      (64,1,190,1.1888),
      (65,1,193,0.9961),
      (66,1,196,1.4411),
      (67,1,199,0.9116),
      (68,1,202,1.1221),
      (69,1,205,1.1018),
      (70,1,208,0.753),
      (71,1,211,0.269),
      (72,1,214,0.0113),
      (73,1,217,0.1683),
      (74,1,220,-0.3469),
      (75,1,223,-0.1272),
      (76,1,226,-0.2421),
      (77,1,229,0.0825),
      (78,1,232,0.1249),
      (79,1,235,0.1294),
      (80,1,238,0.3778),
      (81,1,241,-0.0886),
      (82,1,244,0.5033),
      (83,1,247,0.2598),
      (84,1,250,0.3028),
      (85,1,253,0.1198),
      (86,1,256,-0.3048),
      (87,1,259,-0.3401),
      (88,1,262,-0.5499),
      (89,1,265,-0.5536),
      (90,1,268,-0.7984),
      (91,1,271,-1.2115),
      (92,1,274,-1.3719),
      (93,1,277,-1.2532),
      (94,1,280,-0.9145),
      (95,1,283,-0.8726),
      (96,1,286,-0.972),
      (97,1,289,-0.6255),
      (98,1,292,-0.1526),
      (99,1,295,0.5809),
      (100,1,298,0.8523)
    ));

SET rbf_kernel_smooth_raw = 'udf.test_1d_kernel';

CREATE OR REPLACE FUNCTION udf.EXPONENTIAL_KERNEL_SMOOTHER(smoothing_sensitivity FLOAT, density_sensitivity FLOAT)
  RETURNS TABLE (id VARCHAR, partition_variable VARCHAR
                 , x1 FLOAT, x2 FLOAT
                 , x2_smoothed_uniform FLOAT, x2_smoothed_inv_bandwith FLOAT
                 , dif_x2 FLOAT, std_dev_dif_x2 FLOAT)
    AS
    $$
    /* Calculate standard deviation of */
    WITH sigma AS (
      SELECT
          *
        , smoothing_sensitivity
        , density_sensitivity
        , udf.SIGMA_1D(x1, partition_variable) sigma
        , sigma * POW((4/3)/COUNT(x1) OVER(PARTITION BY partition_variable), 1/5) rule_of_thumb_bandwidth
      FROM TABLE($rbf_kernel_smooth_raw)
    )
    /* CREATE DISTANCE MATRIX, MEASURED BY RADIAL BASIS FUNCTION DISTANCE */
    , rbf_distance_matrix AS (
      SELECT
          l.*
        , r.x1 x1_b
        , r.x2 x2_b
        , udf.LINEAR_KERNEL_2D(l.x1,r.x1,0,0) linear_distance
        , udf.EXP_KERNEL_2D(l.x1,r.x1,0,0,l.rule_of_thumb_bandwidth,l.smoothing_sensitivity) gaussian_distance_uniform_bandwith
      FROM sigma l
      LEFT JOIN sigma r
        ON l.partition_variable = r.partition_variable
    )
    /* ADJUST BANDWIDTH BASED ON POINT DENSITY */
    , bandwith_adjustment AS (
      SELECT
          *
        , SUM(linear_distance) OVER(PARTITION BY id, partition_variable) density_per_spot
      FROM rbf_distance_matrix
    )
    , bandwith_adjustment_2 AS (
      SELECT
          *
        , (density_sensitivity - (density_sensitivity * udf.MIN_MAX_SCALER(density_per_spot, partition_variable)) + 1)/(density_sensitivity+1) bandwidth_adjustment
      FROM bandwith_adjustment
    )
    , distance_matrix_bandwidth_adjustment AS (
      SELECT
          *
        , udf.EXP_KERNEL_2D(x1,x1_b,0,0,rule_of_thumb_bandwidth,bandwidth_adjustment*smoothing_sensitivity) gaussian_distance_inv_bandwith
      FROM bandwith_adjustment_2
    )
    /* CALCULATE SMOOTHED FIGURES */
    , smoothing AS (
      /* APPLY SMOOTHING using Nadaraya-Watson kernel-weighted average */
      SELECT
          id::VARCHAR id
        , partition_variable::VARCHAR partition_variable
        , x1::FLOAT x1
        , x2::FLOAT x2
        , (SUM(gaussian_distance_uniform_bandwith * x2_b)/SUM(gaussian_distance_uniform_bandwith))::FLOAT x2_smoothed_uniform
        , (SUM(gaussian_distance_inv_bandwith * x2_b)/SUM(gaussian_distance_inv_bandwith))::FLOAT x2_smoothed_inv_bandwith
      FROM distance_matrix_bandwidth_adjustment
    GROUP BY 1,2,3,4
    ORDER BY id::INT
    )
    , anomaly_detection AS (
    SELECT
        *
        , (x2_smoothed_inv_bandwith - x2) dif_x2
        , udf.SIGMA_1D(x2_smoothed_inv_bandwith - x2, partition_variable) AS std_dev_dif_x2
        , dif_x2/std_dev_dif_x2 AS difference_std
--         , CASE WHEN ABS(difference_std) > 1.645 THEN TRUE ELSE FALSE END anomaly_95
--         , CASE WHEN ABS(difference_std) > 1.96 THEN TRUE ELSE FALSE END anomaly_99
    FROM smoothing
    )
    SELECT
        id
      , partition_variable
      , x1
      , x2
      , x2_smoothed_uniform
      , x2_smoothed_inv_bandwith
      , dif_x2
      , std_dev_dif_x2
    FROM anomaly_detection
    $$;

GRANT SELECT ON udf.test_1d_kernel TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.MIN_MAX_SCALER(FLOAT, VARCHAR) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.UNDO_MIN_MAX_SCALER(FLOAT, FLOAT, VARCHAR) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.SIGMA_1D(FLOAT, VARCHAR) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.SIGMA_2D(FLOAT, FLOAT, VARCHAR) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.LINEAR_KERNEL_2D(FLOAT, FLOAT, FLOAT, FLOAT) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.EXP_KERNEL_2D(FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, FLOAT) TO ROLE ANALYTICS;
GRANT USAGE ON FUNCTION udf.EXPONENTIAL_KERNEL_SMOOTHER(FLOAT, FLOAT) TO ROLE ANALYTICS;

/* ####################
-- EXAMPLE
#################### */

-- Required columns: id, partition, x1, x2
SET rbf_kernel_smooth_raw = 'udf.test_1d_kernel';
-- Then just run the function(s)!
CREATE TABLE IF NOT EXISTS udf.kernel_smoothed_example AS (
  WITH smoother AS (
    SELECT *
    FROM TABLE(udf.EXPONENTIAL_KERNEL_SMOOTHER(1.0::FLOAT     -- use rule-of-thumb smoothness
                                               , 0::FLOAT)) s -- do not differentiate by point density
  )
  , spikier AS (
    SELECT *
    FROM TABLE(udf.EXPONENTIAL_KERNEL_SMOOTHER(0.5::FLOAT   -- Make a spikier smoothness curve
                                               , 5::FLOAT)) -- decrease bandwidth of least dense points by 5
  )
  SELECT
      s.id
    , s.partition_variable
    , s.x1
    , s.x2
    , s.x2_smoothed_uniform x2_smoothed_uniform_1_0
    , s.x2_smoothed_inv_bandwith x2_smoothed_inv_bandwith_1_0
    , ns.x2_smoothed_uniform x2_smoothed_uniform_025_5
    , ns.x2_smoothed_inv_bandwith x2_smoothed_inv_bandwith_025_5
  FROM smoother s
  LEFT JOIN spikier ns
    ON s.id = ns.id
);

GRANT SELECT ON udf.kernel_smoothed_example TO ROLE ANALYTICS;