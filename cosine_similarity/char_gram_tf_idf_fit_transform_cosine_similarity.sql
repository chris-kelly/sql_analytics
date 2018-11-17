/*
We need to match non-exact ingredients between suppliers and restaurants in Editions sites.
We do this by finding the cosine-similarity between all the combinations and ranking them
 */


/* ################################################################################
(1) Fit TF-IDF to the 'training' data
 ################################################################################ */

-- ID required for joining in data later
CREATE OR REPLACE TEMPORARY TABLE <TABLE_TO_FIT_W_ID> AS (
  SELECT
      SEQ4() AS id
    , *
  FROM <TABLE_TO_TRAIN>
);

-- Derive the length of the longest string of the supplier data (for fitting)
SET max_rowcount = (SELECT MAX(LENGTH(<COLUMN_TO_TF_IDF>)) FROM <TABLE_TO_FIT_W_ID>);

-- Split the supplier strings into n-gram tokens
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_tokenisation_fit AS (
  SELECT
    id
    , <COLUMN_TO_FIT_TFIDF> AS original_text
    , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS parsed_text
    , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
    , TRIM(SUBSTR(parsed_text, sub_int, 1)) unigram
    , TRIM(SUBSTR(parsed_text, sub_int, 2)) bigram
    , TRIM(SUBSTR(parsed_text, sub_int, 3)) trigram
    , TRIM(SUBSTR(parsed_text, sub_int, 1)) || ' ' || TRIM(SUBSTR(parsed_text, sub_int+2, 1)) bigram_skip
  FROM <TABLE_TO_FIT_W_ID>
  LEFT JOIN TABLE(generator(rowcount => $max_rowcount))
);

-- Calculate the frequency of each n-gram token in the string
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_text_frequency_fit AS (
  SELECT
    id
    , original_text
    , unigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_fit
  WHERE LENGTH(REPLACE(unigram,' ')) = 1
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , bigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_fit
  WHERE LENGTH(REPLACE(bigram,' ')) = 2
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , trigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_fit
  WHERE LENGTH(REPLACE(trigram,' ')) = 3
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , bigram_skip AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_fit
  WHERE LENGTH(REPLACE(bigram_skip,' ')) = 2
  GROUP BY 1,2,3
);

-- Calculate the inverse document frequency of each token, and hence the tf-idf product
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_tf_idf_fit AS (
  SELECT
    *
    , LN(1+text_frequency) log_normal_text_frequency
    , COUNT(DISTINCT id) OVER() AS total_number_docs
    , COUNT(DISTINCT id) OVER(PARTITION BY term) AS num_docs_w_term
    , 1 + LN(total_number_docs/num_docs_w_term) AS inverse_document_frequency
    , log_normal_text_frequency * inverse_document_frequency AS tf_idf
  FROM denormalised.tmp_text_frequency_fit
);

-- run L2 normalisation of the tf-idf
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_tf_idf_l2_fit AS (
  SELECT
    *
    , tf_idf/SQRT(SUM(POW(tf_idf, 2)) OVER(PARTITION BY id)) l2_norm_tf_idf
  FROM denormalised.tmp_tf_idf_fit
);

/* ################################################################################
(2) Transform the 'matching' data using the IDF values fitted to the 'training' data
 ################################################################################ */

-- Optionally just match one query rather than entire table
-- SET query = 'Hello world';
-- SET query2 = (select REGEXP_REPLACE(lower($query), listagg(distinct lower(<location_field>), ' *| *'),  ' ') from <location_table>);


-- ID required for joining in data later
CREATE OR REPLACE TEMPORARY TABLE <TABLE_TO_TRANSFORM_W_ID> AS (
  SELECT
      SEQ4() AS id
    , *
  FROM <TABLE_TO_TRANSFORM>
);

-- Derive the length of the longest string of the supplier data (for transforming)
SET max_rowcount = (SELECT MAX(LENGTH(<COLUMN_TO_TRANSFORM_TFIDF>)) FROM <TABLE_TO_TRANSFORM_W_ID>);

-- Split the restaurant strings into n-gram tokens
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_tokenisation_transform AS (
  SELECT
    id
    , <COLUMN_TO_TRANSFORM_TFIDF> AS original_text
    -- , $query2 AS original_text
    , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS parsed_text
    , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
    , TRIM(SUBSTR(parsed_text, sub_int, 1)) unigram
    , TRIM(SUBSTR(parsed_text, sub_int, 2)) bigram
    , TRIM(SUBSTR(parsed_text, sub_int, 3)) trigram
    , TRIM(SUBSTR(parsed_text, sub_int, 1)) || ' ' || TRIM(SUBSTR(parsed_text, sub_int+2, 1)) bigram_skip
  FROM <TABLE_TO_TRANSFORM_W_ID> -- remove this line if using one query
  LEFT JOIN TABLE(generator(rowcount => $max_rowcount))
);

-- Calculate the frequency of each n-gram token in the string
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_text_frequency_transform AS (
  SELECT
    id
    , original_text
    , unigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_transform
  WHERE LENGTH(REPLACE(unigram,' ')) = 1
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , bigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_transform
  WHERE LENGTH(REPLACE(bigram,' ')) = 2
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , trigram AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_transform
  WHERE LENGTH(REPLACE(trigram,' ')) = 3
  GROUP BY 1,2,3
  UNION
  SELECT
    id
    , original_text
    , bigram_skip AS term
    , count(*) AS text_frequency
  FROM denormalised.tmp_tokenisation_transform
  WHERE LENGTH(REPLACE(bigram_skip,' ')) = 2
  GROUP BY 1,2,3
);

-- Join in the inverse document frequency of each token from the fitted supplier data
-- THen calculate the inverse document frequency of each token, and hence the l2-normalised tf-idf product
CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_all_combinations_transform AS (
  SELECT
      tf.*
    , mt.id id_2
  FROM denormalised.tmp_text_frequency_transform tf
  LEFT JOIN (SELECT DISTINCT id FROM denormalised.tmp_tf_idf_l2_fit) mt
);

CREATE OR REPLACE TEMPORARY TABLE denormalised.tmp_tf_idf_transform AS (
    SELECT
      ti.id
      , ti.term
      , ti.original_text
      , rn.id id_2
      , rn.original_text matched_text
      , LN(1+ti.text_frequency) * rnd.inverse_document_frequency tf_idf_query
      , tf_idf_query/SQRT(SUM(POW(tf_idf_query, 2)) OVER(PARTITION BY ti.id, ti.id_2)) l2_norm_tf_idf_query
      , rn.l2_norm_tf_idf
    FROM denormalised.tmp_all_combinations_transform ti
    LEFT JOIN (SELECT DISTINCT term, inverse_document_frequency FROM denormalised.tmp_tf_idf_l2_fit) rnd
      ON rnd.term = ti.term
    LEFT JOIN denormalised.tmp_tf_idf_l2_fit rn
      ON ti.term = rn.term
      AND ti.id_2 = rn.id
);

/* ################################################################################
(3) Calculate the cosine similarity of these matches
    (linear kernel for l2-normalised data)
################################################################################ */

CREATE OR REPLACE TABLE <COSINE_SIMILARITY_TABLE> AS (
SELECT
  id
  , original_text
  , id_2
  , matched_text
  , SUM(l2_norm_tf_idf_query * l2_norm_tf_idf) cosine_similarity
FROM denormalised.tmp_tf_idf_transform ti
GROUP BY 1,2,3,4
);