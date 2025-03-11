--hdfs用户存储分析结果
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;
drop table storage_analyze.hdfs_storage_user_tmp;
create table storage_analyze.hdfs_storage_user_tmp as 
select t1.p2 as user_name,
       --round(t1.user_sto/1024/1024,2) user_sto,
       --t1.user_num,
       --t2.user_all_sto/1024/1024,
       --t2.user_all_num,
       round(t1.user_sto/t2.user_all_sto,2) as user_storage_pro,
       round(t1.user_num/t2.user_all_num,2) as user_dir_pro,
       CAST(date_format(current_date(),'yyyyMMdd') as string) etl_dt
from(
select p2,sum(filesize) user_sto,count(1) user_num 
from storage_analyze.dwd_fsimage
where partition_key='${hiveconf:DATA_DT}' and p1='/user' and p2 is not null 
group by p2
) t1 ,
(
select sum(filesize) user_all_sto,count(1) user_all_num 
from storage_analyze.dwd_fsimage
where partition_key='${hiveconf:DATA_DT}' and p1='/user' and p2 is not null 
) t2 
;

insert overwrite table storage_analyze.hdfs_storage_user partition(partition_key)
select 
     user_name,
     user_storage_pro,
     user_dir_pro,
     etl_dt,
	 ${hiveconf:DATA_DT} as partition_key
from storage_analyze.hdfs_storage_user_tmp 
;
--将结果数据导入临时表
drop table storage_analyze.hdfs_storage_user_export;
create table storage_analyze.hdfs_storage_user_export as 
select 
     user_name,
     user_storage_pro,
     user_dir_pro,
     partition_key as etl_dt
from storage_analyze.hdfs_storage_user
;
