/* ################################################################################
 -- USER DEFINED TEXT COSINE SIMILARITY FUNCTIONS (DOT PRODUCT FROM L2 TF-IDF)
 ################################################################################ */

 -- SKIP STRAIGHT TO ROW 200 FOR EXAMPLE

/* ########################################
-- L2 NORMALISATION
######################################## */

CREATE OR REPLACE FUNCTION udf.L2_NORM(id INT, id2 INT, x FLOAT)
  RETURNS FLOAT
  AS
  $$
  x/SQRT(SUM(POW(x, 2)) OVER(PARTITION BY id, id2))
  $$;

GRANT USAGE ON FUNCTION udf.L2_NORM(INT,INT,FLOAT) TO ROLE SCHEMA_UDF;

/* ########################################
-- N-GRAM TOKENISATION
######################################## */

CREATE OR REPLACE FUNCTION udf.ngram_tokenisation(parsed_text VARCHAR, split_type VARCHAR, sub_int NUMBER, ngram NUMBER)
  RETURNS VARCHAR
  AS
  $$
  CASE WHEN split_type = 'letter'
       THEN CASE WHEN LENGTH(REPLACE(TRIM(SUBSTR(parsed_text, sub_int, ngram)),' ')) = ngram
                 THEN TRIM(SUBSTR(parsed_text, sub_int, ngram))
                 END
       WHEN split_type = 'word'
       THEN SPLIT(parsed_text, ' ')[sub_int - 1]
  END
  $$;

GRANT USAGE ON FUNCTION udf.ngram_tokenisation(VARCHAR,VARCHAR,NUMBER,NUMBER) TO ROLE SCHEMA_UDF;

/* ########################################
-- CALCULATING INVERSE DOCUMENT FREQUENCY
######################################## */

CREATE OR REPLACE FUNCTION udf.idf_fit(id INT, term VARCHAR)
  RETURNS FLOAT
  AS
  $$
  1 + LN(COUNT(DISTINCT id) OVER() -- total_number_docs
         / COUNT(DISTINCT id) OVER(PARTITION BY term) -- num_docs_w_term
         )
  $$;

GRANT USAGE ON FUNCTION udf.idf_fit(INT, VARCHAR) TO ROLE SCHEMA_UDF;

/* ########################################
-- SET UP TABLES FOR FIT/TRANSFORM
######################################## */

-- This is just a table created for use in the example below
CREATE TABLE IF NOT EXISTS udf.cs_raw_fit_example AS (
  SELECT
      ROW_NUMBER() OVER(ORDER BY 1) AS id
    , original_text
    , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS text_string
  FROM <TABLE_TO_FIT_W_ID>
);

-- This is just a table created for use in the example below
CREATE TABLE IF NOT EXISTS udf.cs_raw_transform_example AS (
  SELECT
      ROW_NUMBER() OVER(ORDER BY 1) AS id
    , original_text
    , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS text_string
  FROM <TABLE_TO_TRANSFORM_W_ID>
);

/* ########################################
-- CALCULATING (L2 NORM) TF-IDF
######################################## */

SET TEXT_TABLE = 'udf.cs_raw_fit_example';
SET MAX_ROWCOUNT = (SELECT MAX(LENGTH(text_string)) FROM TABLE($TEXT_TABLE));

CREATE OR REPLACE FUNCTION udf.tf_idf(split_type VARCHAR, tf_log_true BOOLEAN, idf_true BOOLEAN)
    RETURNS TABLE (id INT, text_string VARCHAR, term VARCHAR, TF FLOAT, IDF FLOAT, TF_IDF FLOAT, L2_norm_tfidf FLOAT)
      AS
      $$
      -- Tri-gram tokenisation
      WITH tokenisation AS (
          SELECT
              id
            , text_string
            , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
            , udf.ngram_tokenisation(text_string, split_type, sub_int, 1) unigram
            , udf.ngram_tokenisation(text_string, split_type, sub_int, 2) bigram
            , udf.ngram_tokenisation(text_string, split_type, sub_int, 3) trigram
          FROM TABLE($TEXT_TABLE)
          LEFT JOIN TABLE(generator(rowcount => $MAX_ROWCOUNT))
        )
      -- Calculate Text Frequency
        , tf_fit AS (
          SELECT
              id
            , text_string
            , unigram AS term
            , count(*) AS tf_c
          FROM tokenisation
          GROUP BY 1,2,3
          UNION
          SELECT
              id
            , text_string
            , bigram AS term
            , count(*) AS tf_c
          FROM tokenisation
          GROUP BY 1,2,3
          UNION
          SELECT
              id
            , text_string
            , trigram AS term
            , count(*) AS tf_c
          FROM tokenisation
          GROUP BY 1,2,3
        )
      -- Calculate inverse document frequency
        , tf_idf AS (
          SELECT
              id
            , text_string
            , term
            , CASE WHEN tf_log_true
                   THEN LN(1+tf_c)
                   ELSE tf_c
                   END tf
            , CASE WHEN idf_true
                   THEN udf.idf_fit(id, term)
                   ELSE NULL
                   END idf
            , tf*COALESCE(idf,1) tf_idf
          FROM tf_fit
          WHERE term IS NOT NULL
        )
        SELECT
            *
          , udf.L2_NORM(id, 1, tf*COALESCE(idf,1)) L2_norm_tfidf
        FROM tf_idf
      $$;

GRANT USAGE ON FUNCTION udf.tf_idf(VARCHAR, BOOLEAN, BOOLEAN) TO ROLE SCHEMA_UDF;
                                           
/* ####################
-- CALCULATING COSINE SIMILARITY
#################### */

-- This is just a table created for use in the example below
CREATE TABLE IF NOT EXISTS udf.tfidf_fit AS (
  SELECT *
  FROM TABLE(udf.tf_idf('letter', TRUE, TRUE))
);

CREATE TABLE IF NOT EXISTS udf.tfidf_transform AS (
  SELECT *
  FROM TABLE(udf.tf_idf('letter', TRUE, TRUE))
);

SET TF_IDF_FIT_TABLE = 'udf.tfidf_fit';
SET TF_IDF_TRANSFORM_TABLE = 'udf.tfidf_transform';

CREATE OR REPLACE FUNCTION udf.text_cosine_similarity()
    RETURNS TABLE (id_fit INT, id_transform INT, text_fit VARCHAR, text_transform VARCHAR, cosine_similarity FLOAT)
      AS
      $$
      WITH tf_idf_transform AS (
        SELECT
            b.*
          , udf.L2_NORM(b.id, 1, b.tf * COALESCE(a.idf,1)) l2_norm_tf_idf
        FROM TABLE($TF_IDF_TRANSFORM_TABLE) b
        INNER JOIN (SELECT DISTINCT term, idf FROM TABLE($TF_IDF_FIT_TABLE)) a
          ON a.term = b.term
      )
      SELECT
          a.id id_fit
        , b.id id_transform
        , a.text_string text_fit
        , b.text_string text_transform
        , SUM(a.L2_norm_tfidf * b.L2_norm_tfidf) cosine_similarity
      FROM TABLE($TF_IDF_FIT_TABLE) a
      INNER JOIN tf_idf_transform b
        ON a.term = b.term
      GROUP BY 1,2,3,4
      $$;

GRANT SELECT ON udf.cs_raw_example TO ROLE SCHEMA_UDF;
GRANT SELECT ON udf.tfidf_fit TO ROLE SCHEMA_UDF;
GRANT SELECT ON udf.tfidf_transform TO ROLE SCHEMA_UDF;
GRANT USAGE ON FUNCTION udf.text_cosine_similarity() TO ROLE SCHEMA_UDF;

/* ####################
-- EXAMPLE
#################### */

CREATE OR REPLACE TABLE udf.animal_groups AS (
    SELECT
        COLUMN1 AS id
      , COLUMN2 AS text_string
    FROM(VALUES
        (1,'Crash of rhinoceros'),
        (2,'Troop of baboons'),
        (3,'Muster of peacocks'),
        (4,'Herd of elephants'),
        (5,'Scurry of squirrels'),
        (6,'Squadron of pelicans'),
        (7,'Parliament of owls'),
        (8,'Bask of crocodiles'),
        (9,'Pride of lions'),
        (10,'Caravan of camels')
        ));

CREATE OR REPLACE TABLE udf.animals_mispelt AS (
    SELECT
        COLUMN1 AS id
      , COLUMN2 AS text_string
    FROM(VALUES
        (1,'plican'),
        (2,'croc'),
        (3,'rhino'),
        (4,'lioness'),
        (5,'an owl'),
        (6,'peacocking'),
        (7,'squrel'),
        (8,'babon'),
        (9,'camles'),
        (10,'elephante')
        ));

/* ## TF-IDF ## */
-- Required function variables:
   -- split type (VARCHAR either 'letter' or 'word' for type of separation)
   -- tf_log_true (BOOLEAN whether to log the text frequency)
   -- idf_true (BOOLEAN whether to calculate idf terms)
-- Required parameters:
   -- Raw table name (Required columns: id, text_string);
   -- Length of longest text string;
SET TEXT_TABLE = 'udf.animal_groups';
SET MAX_ROWCOUNT = (SELECT MAX(LENGTH(text_string)) FROM TABLE($TEXT_TABLE));
CREATE TABLE IF NOT EXISTS udf.tfidf_fit AS (
  SELECT *
  FROM TABLE(udf.tf_idf('letter', TRUE, TRUE))
);

SET TEXT_TABLE = 'udf.animals_mispelt';
SET MAX_ROWCOUNT = (SELECT MAX(LENGTH(text_string)) FROM TABLE($TEXT_TABLE));
CREATE TABLE IF NOT EXISTS udf.tfidf_transform AS (
  SELECT *
  FROM TABLE(udf.tf_idf('letter', TRUE, TRUE))
);


/* ## Text Cosine Similarity ## */
-- Required function variables:
   -- split_type (VARCHAR either 'letter' or 'word' for type of separation)
   -- tf_log_true (BOOLEAN whether to log-transform the text frequency)
   -- idf_log_true (BOOLEAN whether to log-transform the inverse document frequency)
-- Required parameters:
   -- TF-IDF table name (Required columns: id, L2_norm_tfidf);
SET TF_IDF_FIT_TABLE = 'udf.tfidf_fit';
SET TF_IDF_TRANSFORM_TABLE = 'udf.tfidf_transform'; -- Note in this example, tfidf_fit == tfidf_transform
CREATE TABLE IF NOT EXISTS udf.cosine_similarity_tfidf AS (
  SELECT *
  FROM TABLE(udf.text_cosine_similarity())
);

GRANT SELECT ON udf.cosine_similarity_tfidf TO ROLE SCHEMA_UDF;