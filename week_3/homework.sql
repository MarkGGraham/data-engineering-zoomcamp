SET @@dataset_id = 'dtc-de-2022-mg-339910';

select count(*) from dtc-de-2022-mg-339910.trips_data_all.fh_tripdata;

-- Question 1:
-- What is count for fhv vehicles data for year 2019
select count(*) from dtc-de-2022-mg-339910.trips_data_all.fh_partitioned;

-- Question 2:
-- How many distinct dispatching_base_num we have in fhv for 2019
-- Can run a distinct query on the table from question 1
select count(distinct dispatching_base_num)
from dtc-de-2022-mg-339910.trips_data_all.fh_partitioned where pickup_datetime between '2019-01-01' and '2019-12-31';

-- Question 3:
-- Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
-- Review partitioning and clustering video.
-- We need to think what will be the most optimal strategy to improve query performance and reduce cost

--create partition on dropoff_datetime and cluster by dispatching_base_num as follows
create or replace table dtc-de-2022-mg-339910.trips_data_all.fh_tripdata_partitioned_clustered
partition by DATE(pickup_datetime)
cluster by (dispatching_base_num)
as select * from dtc-de-2022-mg-339910.trips_data_all.fh_tripdata;

-- Question 4:
-- What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
-- Create a table with optimized clustering and partitioning, and run a count(*). Estimated data processed can be found in top right corner and actual data processed can be found after the query is executed.
select count(*)
from dtc-de-2022-mg-339910.trips_data_all.fh_tripdata_partitioned_clustered
where  1=1 
and dropoff_datetime between '2019-01-01' and '2019-03-31'
and dispatching_base_num in ('B00987', 'B02060', 'B02279')

-- Question 5:
-- What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
-- Review partitioning and clustering video. Clustering cannot be created on all data types.
I would probably partition dispatching_base_num and ignore SR_Flag due  to cardinality of SR_Flag.
A composite partition could be created.

-- Question 6:
-- What improvements can be seen by partitioning and clustering for data size less than 1 GB
-- Partitioning and clustering also creates extra metadata.
-- Before query execution this metadata needs to be processed.
Very little performance improvements would be gained by partitioning or clustered such a small table.

