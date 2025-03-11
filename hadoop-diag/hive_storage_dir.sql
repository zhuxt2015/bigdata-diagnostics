--hive存储分析结果
--获取hive数据库用户信息
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;

drop table storage_analyze.dwd_fsimage_tmp1;
create table storage_analyze.dwd_fsimage_tmp1 as 
select p3,p4,p5,file_ide,data_ide,count(1) num,sum(filesize_total/1024/1024/1024) filesize_total
from(
select p3,p4,p5,
      (filesize_total) filesize_total,
case when (filesize/1024/1024)>=1024 then 'big_dir_pro' 
     when 1024>(filesize/1024/1024) and (filesize/1024/1024)>= 128 then 'med_dir_pro'
     when 128>(filesize/1024/1024) and (filesize/1024/1024)> 0 then 'sma_dir_pro' 
     when (filesize/1024/1024) =0 then 'emp_dir_pro' end file_ide,
     accesstime,
 case when datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd')) <=7 then 'heat_data_pro' 
      when 7 < datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd')) 
	        and datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd'))<= 90  then 'warm_data_pro'
      when 365 >= datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd')) 
	       and datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd')) > 90 then 'cold_data_pro' 
      when 365 <= datediff(date_format(current_date(),'yyyy-MM-dd'),date_format(accesstime,'yyyy-MM-dd')) 
      then 'ext_cold_data_pro' end data_ide 
from storage_analyze.dwd_fsimage
where partition_key='${hiveconf:DATA_DT}' and p3='/user/hive/warehouse'
) t2
group by p3,p4,p5,file_ide,data_ide
;

--获取hive相关信息：数据库个数、表个数等
drop table storage_analyze.hive_storage_dir_tmp;
create table storage_analyze.hive_storage_dir_tmp as 
select 
    t7.db_count as db_count ,
	t7.table_count as table_count,
	t7.storage_measure as storage_measure,
	t7.dir_size as dir_size,
	t8.big_dir_num as big_dir_pro,
	t8.med_dir_num as med_dir_pro,
	t8.sma_dir_num as sma_dir_pro,
	t8.emp_dir_num as emp_dir_pro,
	t9.heat_data_num as heat_data_pro,
	t9.warm_data_num as warm_data_pro,
	t9.cold_data_num as cold_data_pro,
	t9.ext_cold_data_num as ext_cold_data_pro,
	CAST(date_format(current_date(),'yyyyMMdd') as string) etl_dt
from(
--库数量、表数据量、空间、文件个数
select p3,sum(db_num) db_count,sum(table_num) table_count,sum(filesize_total) storage_measure,sum(file_cun) dir_size
from (
select p3,db_num,0 table_num,0 file_cun,0 filesize_total
from 
(
select p3,count(distinct p4) db_num 
from storage_analyze.dwd_fsimage_tmp1
group by p3 
) n1 
union all 
select p3,0 db_num,table_num,0 file_cun,0 filesize_total
from 
(
select p3,count(distinct p5) table_num 
from storage_analyze.dwd_fsimage_tmp1
group by p3 
) n2
union all 
select p3,0 db_num,0 table_num,sum(num) file_cun ,sum(filesize_total) filesize_total 
from storage_analyze.dwd_fsimage_tmp1
group by p3 
) n3
group by p3 
) t7
inner join 
(
--文件占比
--select 
--    t2.p3,
--    round(t2.big_dir_num/t3.file_cun,2) big_dir_pro,
--    round(t2.med_dir_num/t3.file_cun,2) med_dir_pro,
--    round(t2.sma_dir_num/t3.file_cun,2) sma_dir_pro,
--    round(t2.emp_dir_num/t3.file_cun,2) emp_dir_pro
--from(
select p3,sum(big_dir_num) big_dir_num ,sum(med_dir_num) med_dir_num ,sum(sma_dir_num) sma_dir_num ,sum(emp_dir_num) emp_dir_num 
from(
select p3,sum(num) big_dir_num ,0 med_dir_num,0 sma_dir_num,0 emp_dir_num
from storage_analyze.dwd_fsimage_tmp1 
where file_ide='big_dir_pro'
group by p3
union all 
select p3,0 big_dir_num,sum(num) med_dir_num ,0 sma_dir_num,0 emp_dir_num
from storage_analyze.dwd_fsimage_tmp1 
where file_ide='med_dir_pro'
group by p3
union all 
select p3,0 big_dir_num,0 med_dir_num, sum(num) sma_dir_num ,0 emp_dir_num
from storage_analyze.dwd_fsimage_tmp1 
where file_ide='sma_dir_pro'
group by p3
union all
select p3,0 big_dir_num,0 med_dir_num,0 sma_dir_num,sum(num) emp_dir_num 
from storage_analyze.dwd_fsimage_tmp1 
where file_ide='emp_dir_pro'
group by p3
) t1 
group by p3
--) t2 
--inner join 
--(
--select p3,sum(num) file_cun 
--from storage_analyze.dwd_fsimage_tmp1
--group by p3 
--) t3 
--on t3.p3=t2.p3
) t8 
on t7.p3=t8.p3
inner join 
(
--冷热数据占比
--select t5.p3,
--       round(t5.heat_data_num/t6.file_cun,2) heat_data_pro,
--       round(t5.warm_data_num/t6.file_cun,2) warm_data_pro,
--       round(t5.cold_data_num/t6.file_cun,2) cold_data_pro,
--       round(t5.ext_cold_data_num/t6.file_cun,2) ext_cold_data_pro
--from(
select p3,
       sum(heat_data_num) heat_data_num ,
       sum(warm_data_num) warm_data_num,
       sum(cold_data_num) cold_data_num,
       sum(ext_cold_data_num) ext_cold_data_num
from(
select p3,sum(num) heat_data_num ,0 warm_data_num,0 cold_data_num,0 ext_cold_data_num
from storage_analyze.dwd_fsimage_tmp1 
where data_ide='heat_data_pro'
group by p3
union all 
select p3,0 heat_data_num ,sum(num) warm_data_num,0 cold_data_num,0 ext_cold_data_num
from storage_analyze.dwd_fsimage_tmp1 
where data_ide='warm_data_pro'
group by p3
union all 
select p3,0 heat_data_num ,0 warm_data_num,sum(num) cold_data_num,0 ext_cold_data_num
from storage_analyze.dwd_fsimage_tmp1 
where data_ide='cold_data_pro'
group by p3
union all 
select p3,0 heat_data_num ,0 warm_data_num,0 cold_data_num,sum(num) ext_cold_data_num
from storage_analyze.dwd_fsimage_tmp1 
where data_ide='ext_cold_data_pro'
group by p3
) t4 
group by p3 
--) t5 
--inner join 
--(
--select p3,sum(num) file_cun 
--from storage_analyze.dwd_fsimage_tmp1
--group by p3 
--) t6 
--on t6.p3=t5.p3
) t9 
on t7.p3=t9.p3
;

insert overwrite table storage_analyze.hive_storage_dir partition(partition_key)
select 
     db_count,
     table_count,
     storage_measure,
     dir_size,
     big_dir_pro,
     med_dir_pro,
     sma_dir_pro,
     emp_dir_pro,
     heat_data_pro,
     warm_data_pro,
     cold_data_pro,
     ext_cold_data_pro,
     etl_dt,
	 ${hiveconf:DATA_DT} as partition_key
from storage_analyze.hive_storage_dir_tmp 
;

--将结果数据导入临时表
drop table storage_analyze.hive_storage_dir_export;
create table storage_analyze.hive_storage_dir_export as 
select 
     db_count,
     table_count,
     storage_measure,
     dir_size,
     big_dir_pro,
     med_dir_pro,
     sma_dir_pro,
     emp_dir_pro,
     heat_data_pro,
     warm_data_pro,
     cold_data_pro,
     ext_cold_data_pro,
     partition_key as etl_dt
from storage_analyze.hive_storage_dir
;
