Links to parquet files
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet


SQL Queries

SELECT count(*) FROM `project_id.green_trip_records.taxi_data`;
select count(distinct(PULocationID)) from `project_id.green_trip_records.taxi_data`;
select count(distinct(PULocationID)) from `project_id.green_trip_records.taxi_data_materialized`;

select count(*) from `project_id.green_trip_records.taxi_data_materialized` where fare_amount = 0;


SELECT DISTINCT PULocationID
FROM `project_id.green_trip_records.taxi_data_materialized`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01T00:00:00' AND '2022-06-30T23:59:59';

SELECT DISTINCT PULocationID
FROM `project_id.green_trip_records.taxi_data_optimized`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01T00:00:00' AND '2022-06-30T23:59:59';


# For not rated
select count(*) from `project_id.green_trip_records.taxi_data_materialized`;