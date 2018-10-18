/*
Create a TF-IDF matrix in SQL
 */

SET max_rowcount = (SELECT MAX(LENGTH(<RAW_COLUMN>)) FROM <RAW_TABLE>);

CREATE OR REPLACE TABLE  <TF_IDF_MATRIX_TABLE> AS (
  WITH tokenisation AS (
    SELECT
      id
      , <RAW_COLUMN> AS original_text
      , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS parsed_text
      , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
      , TRIM(SUBSTR(parsed_text, sub_int, 1)) unigram
      , TRIM(SUBSTR(parsed_text, sub_int, 2)) bigram
      , TRIM(SUBSTR(parsed_text, sub_int, 3)) trigram
      , TRIM(SUBSTR(parsed_text, sub_int, 1)) || ' ' || TRIM(SUBSTR(parsed_text, sub_int+2, 1)) bigram_skip
    FROM <RAW_TABLE>
    LEFT JOIN TABLE(generator(rowcount => $max_rowcount))
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
      , bigram_skip AS term
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
  SELECT
    *
    , tf_idf/SQRT(SUM(POW(tf_idf, 2)) OVER(PARTITION BY id)) l2_norm_tf_idf
  FROM tf_idf
);

/*
Calculate cosine similarity in SQL
 */

SET max_rowcount = (SELECT MAX(LENGTH(<COLUMN_TO_MATCH>)) FROM <TABLE_TO_MATCH>);

WITH tokenisation AS (
    SELECT
      id
      , <COLUMN_TO_MATCH> AS original_text
      , REGEXP_REPLACE(REGEXP_REPLACE(LOWER(original_text) ,'[^a-zA-Z0-9 \u4E00-\u9FFF]'), ' +', ' ') AS parsed_text
      , ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) sub_int
      , TRIM(SUBSTR(parsed_text, sub_int, 1)) unigram
      , TRIM(SUBSTR(parsed_text, sub_int, 2)) bigram
      , TRIM(SUBSTR(parsed_text, sub_int, 3)) trigram
      , TRIM(SUBSTR(parsed_text, sub_int, 1)) || ' ' || TRIM(SUBSTR(parsed_text, sub_int+2, 1)) bigram_skip
    FROM <TABLE_TO_MATCH>
    LEFT JOIN TABLE(generator(rowcount => $max_rowcount))
    WHERE id <= 500
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
    , bigram_skip AS term
    , count(*) AS text_frequency
  FROM tokenisation
  WHERE LENGTH(REPLACE(bigram_skip,' ')) = 2
  GROUP BY 1,2,3
)

, all_combinations AS (
  SELECT
      tf.*
    , mt.id id_2
  FROM term_frequency tf
  LEFT JOIN (SELECT DISTINCT id FROM <TF_IDF_MATRIX_TABLE>) mt
)

, tf_idf AS (
    SELECT
      ti.id
      , ti.term
      , ti.original_text
      , rn.id id_2
      , rn.original_text matched_text
      , LN(1+ti.text_frequency) * rnd.inverse_document_frequency tf_idf_query
      , tf_idf_query/SQRT(SUM(POW(tf_idf_query, 2)) OVER(PARTITION BY ti.id, ti.id_2)) l2_norm_tf_idf_query
      , rn.l2_norm_tf_idf
    FROM all_combinations ti
    LEFT JOIN (SELECT DISTINCT term, inverse_document_frequency FROM <TF_IDF_MATRIX_TABLE>) rnd
      ON rnd.term = ti.term
    LEFT JOIN <TF_IDF_MATRIX_TABLE> rn
      ON ti.term = rn.term
      AND ti.id_2 = rn.id
)

, cosine_similarity AS (
    SELECT
      id
      , original_text
      , id_2
      , matched_text
      , SUM(l2_norm_tf_idf_query * l2_norm_tf_idf) cosine_similarity
    FROM tf_idf ti
    GROUP BY 1,2,3,4
)

, cosine_similarity_ranked AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY COSINE_SIMILARITY DESC NULLS LAST) AS cosine_ranking
    FROM cosine_similarity
)

SELECT *
FROM cosine_similarity_ranked
WHERE cosine_ranking <= 3