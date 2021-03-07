-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `silver_views` TABLE
 CREATE TABLE fannik_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://student-2001405/de4/views_silver'
    ) AS SELECT article, views, rank, date FROM fannik_homework.bronze_views;