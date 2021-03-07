-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `bronze_views` TABLE
 CREATE EXTERNAL TABLE
fannik_homework.bronze_views (
    article STRING,
  views INT,
  rank INT,
    date DATE,
    retrieved_at TIMESTAMP) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://student-2001405/de4/views/';