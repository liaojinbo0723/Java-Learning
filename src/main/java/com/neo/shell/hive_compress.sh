#!/usr/bin/env bash
#parallel 并行执行命令 支持并发数的控制
#cat xxx.txt | parallel -j 3 "sleep 2;echo {};date '+%Y-%m-%d %H:%M:%S'"
#获取ods库名
rm -f db_name_zx.txt
rm -f table_name_zx.txt
hadoop fs -ls /user/hive/warehouse | grep "_zx_" |grep -v -E "stg_|_id|_od|_rd" | awk '{print $8}' | awk -F '/' '{print $5}' | awk -F '.' '{print $1}' >> db_name_zx.txt
#获取ods表名
cat db_name_zx.txt | while read line
    do
        hadoop fs -ls /user/hive/warehouse/${line}.db | grep -v "_his" |awk '{print $8}' | awk -F '/' '{print $5"."$6}'|awk -F '.' '{print $1"."$3}' >> table_name_zx.txt
        echo $line"----ok"
    done
#获取dwd_data表名
hadoop fs -ls /user/hive/warehouse/dwd_data.db | grep "_zx_" | grep -v "_his" |awk '{print $8}' | awk -F '/' '{print $5"."$6}'|awk -F '.' '{print $1"."$3}' >> table_name_zx.txt
echo "dwd_data-----ok"
sed -i '/^.$/d' table_name_zx.txt
cat table_name_zx.txt | parallel -j 10 "echo {}--begin---;hive --hiveconf  mapreduce.job.queuename=queue_ph  -hivevar TABLE_NAME={}   -f  public.sql"