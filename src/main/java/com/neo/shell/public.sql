set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;
set hive.exec.max.dynamic.partitions=1000;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

insert overwrite table  ${TABLE_NAME} partition (part_dt)
select
*
from ${TABLE_NAME}
where part_dt > '2017-10-31' and part_dt <'2018-01-31';