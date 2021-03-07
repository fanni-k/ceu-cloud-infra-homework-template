-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
 CREATE TABLE fannik_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://student-2001405/de4/gold_allviews'
    ) AS SELECT article, SUM(views) AS total_top_views, MIN(rank) AS top_rank, SUM(CASE WHEN rank < 50 THEN 1 ELSE 0 END) AS ranked_days FROM fannik_homework.silver_views GROUP BY article, rank ORDER BY top_rank ASC;