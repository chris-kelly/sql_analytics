SET query = 'Hello world';

/* ##################################################
## CREATE l2_NORM TF_IDF MATRIX BY LETTER FROM STRING
##################################################*/

-- ONLY RUN ONCE TO CREATE TF-IDF MATRIX

-- CREATE OR REPLACE TABLE <DESIRED_TABLE_NAME> AS (
--
--   WITH tokenisation AS (
--     SELECT
--       id
--       , original_text
--       , REGEXP_REPLACE(LOWER(original_text), '[^a-zA-Z0-9 ]', '') parsed_text
--       , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
--       , SUBSTR(parsed_text, sub_int, 1) unigram
--       , SUBSTR(parsed_text, sub_int, 2) bigram
--       , SUBSTR(parsed_text, sub_int, 3) trigram
--       , SUBSTR(parsed_text, sub_int, 1) || ' ' || SUBSTR(parsed_text, sub_int, 3) bigram_skip
--     FROM <TABLE_WITH_STRINGS>
--     LEFT JOIN TABLE(generator(rowcount => 105))
--   )
--
--   , term_frequency AS (
--     SELECT
--       id
--       , original_text
--       , unigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(REPLACE(unigram,' ')) = 1
--     GROUP BY 1,2
--     UNION
--     SELECT
--       id
--       , original_text
--       , bigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(REPLACE(bigram,' ')) = 2
--     GROUP BY 1,2,3
--     UNION
--     SELECT
--       id
--       , original_text
--       , trigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(REPLACE(trigram,' ')) = 3
--     GROUP BY 1,2,3
--     UNION
--     SELECT
--       id
--       , original_text
--       , bigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(REPLACE(bigram_skip,' ')) = 2
--     GROUP BY 1,2,3
--   )
--   , tf_idf AS (
--     SELECT
--       *
--       , LN(1+text_frequency) log_normal_text_frequency
--       , COUNT(DISTINCT id) OVER() AS total_number_docs
--       , COUNT(DISTINCT id) OVER(PARTITION BY term) AS num_docs_w_term
--       , 1 + LN(total_number_docs/num_docs_w_term) AS inverse_document_frequency
--       , log_normal_text_frequency * inverse_document_frequency AS tf_idf
--     FROM term_frequency
--   )
--
--   SELECT
--     *
--     , tf_idf/SQRT(SUM(POW(tf_idf, 2)) OVER(PARTITION BY id)) l2_norm_tf_idf
--   FROM tf_idf
--
-- );

/* ##################################################
## CREATE l2_NORM TF_IDF MATRIX BY WORD FROM STRING
##################################################*/

-- CREATE OR REPLACE TABLE <DESIRED_TABLE_NAME> AS (
--
--   WITH tokenisation AS (
--     SELECT
--       id
--       , original_text
--       , SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text), '[^a-zA-Z0-9 ]', ''), ' +', ' '), ' ') parsed_text
--       , REPLACE(f1.value, '"') unigram
--       , TRIM(LAG(REPLACE(f1.value, '"'), 1) OVER(PARTITION BY id ORDER BY f1.index ASC) || ' ' || REPLACE(f1.value, '"')) bigram
--       , TRIM(LAG(REPLACE(f1.value, '"'), 2) OVER(PARTITION BY id ORDER BY f1.index ASC) || ' ' || LAG(REPLACE(f1.value, '"'), 1) OVER(PARTITION BY id ORDER BY f1.index ASC) || ' ' || REPLACE(f1.value, '"')) trigram
--       , TRIM(LAG(REPLACE(f1.value, '"'), 2) OVER(PARTITION BY id ORDER BY f1.index ASC) || ' ' || REPLACE(f1.value, '"')) bigram_skip
--     FROM <TABLE_WITH_STRINGS>
--     , LATERAL flatten(INPUT => SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text), '[^a-zA-Z0-9 ]', ''), ' +', ' '), ' '), OUTER => TRUE, MODE => 'array') f1
--   )
--
--   , term_frequency AS (
--     SELECT
--       id
--       , original_text
--       , unigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE unigram!= ''
--     GROUP BY 1,2,3
--     UNION
--     SELECT
--       id
--       , original_text
--       , bigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(bigram) - LENGTH(REPLACE(bigram,' ')) = 1
--     GROUP BY 1,2,3
--     UNION
--     SELECT
--       id
--       , original_text
--       , trigram AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(trigram) - LENGTH(REPLACE(trigram,' ')) = 2
--     GROUP BY 1,2,3
--     UNION
--     SELECT
--       id
--       , original_text
--       , bigram_skip AS term
--       , count(*) AS text_frequency
--     FROM tokenisation
--     WHERE LENGTH(bigram_skip) - LENGTH(REPLACE(bigram_skip,' ')) = 1
--     GROUP BY 1,2,3
--   )
--   , tf_idf AS (
--     SELECT
--       *
--       , LN(1+text_frequency) log_normal_text_frequency
--       , COUNT(DISTINCT id) OVER() AS total_number_docs
--       , COUNT(DISTINCT id) OVER(PARTITION BY term) AS num_docs_w_term
--       , 1 + LN(total_number_docs/num_docs_w_term) AS inverse_document_frequency
--       , log_normal_text_frequency * inverse_document_frequency AS tf_idf
--     FROM term_frequency
--   )
--
--   SELECT
--     *
--     , tf_idf/SQRT(SUM(POW(tf_idf, 2)) OVER(PARTITION BY id)) l2_norm_tf_idf
--   FROM tf_idf
--
-- );


/* ##################################################
## CALCULATE COSINE SIMILARITY BETWEEN QUERY AND TF-IDF MATRIX
##################################################*/

WITH tokenisation AS (
    SELECT
      1 AS id
      , $query AS original_text
      , REGEXP_REPLACE(LOWER(original_text), '[^a-zA-Z0-9 ]', '') parsed_text
      , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
      , SUBSTR(parsed_text, sub_int, 1) unigram
      , SUBSTR(parsed_text, sub_int, 2) bigram
      , SUBSTR(parsed_text, sub_int, 3) trigram
      , SUBSTR(parsed_text, sub_int, 1) || ' ' || SUBSTR(parsed_text, sub_int, 3) bigram_skip
    FROM TABLE(generator(rowcount => 105))
  )

, term_frequency AS (
    SELECT
      id
      , original_text
      , unigram AS term
      , count(*) AS text_frequency
    FROM tokenisation
    WHERE LENGTH(REPLACE(unigram,' ')) = 1
    GROUP BY 1,2,3
    UNION
    SELECT
      id
      , original_text
      , bigram AS term
      , count(*) AS text_frequency
    FROM tokenisation
    WHERE LENGTH(REPLACE(bigram,' ')) = 2
    GROUP BY 1,2,3
    UNION
    SELECT
      id
      , original_text
      , trigram AS term
      , count(*) AS text_frequency
    FROM tokenisation
    WHERE LENGTH(REPLACE(trigram,' ')) = 3
    GROUP BY 1,2,3
    UNION
    SELECT
      id
      , original_text
      , bigram AS term
      , count(*) AS text_frequency
    FROM tokenisation
    WHERE LENGTH(REPLACE(bigram_skip,' ')) = 2
    GROUP BY 1,2,3
  )

, tf_idf AS (
    SELECT
      *
      , LN(1+text_frequency) log_normal_text_frequency
      , COUNT(DISTINCT id) OVER() AS total_number_docs
      , COUNT(DISTINCT id) OVER(PARTITION BY term) AS num_docs_w_term
      , 1 + LN(total_number_docs/num_docs_w_term) AS inverse_document_frequency
      , log_normal_text_frequency * inverse_document_frequency AS tf_idf
    FROM term_frequency
  )

, l2_norm_tfidf AS (

    SELECT
      *
      , tf_idf/SQRT(SUM(POW(tf_idf, 2)) OVER(PARTITION BY id)) l2_norm_tf_idf
    FROM tf_idf
)

, cosine_similarity AS (
    SELECT
      rn.id
      , rn.original_text
      , SUM(ti.l2_norm_tf_idf * rn.l2_norm_tf_idf) cosine_similarity
    FROM l2_norm_tfidf ti
    LEFT JOIN <DESIRED_TABLE_NAME> rn
      ON ti.term = rn.term
    GROUP BY 1,2
    ORDER BY 1,2
)

SELECT
  *
  , row_number() OVER(ORDER BY cosine_similarity DESC NULLS LAST) rank
FROM cosine_similarity
ORDER BY rank ASC