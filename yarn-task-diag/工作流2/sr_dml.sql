insert into task_fulllink_scan_bigtable
with t1 as (
	select
		application_id ,
		dag_id ,
		sum(input_records_processed) as total_read_records,
		round(sum(hdfs_bytes_read)/ 1024 / 1024 / 1024,2) as total_read_bytes
	from
		yarn_container_counters ycc
	where
		left(counter_time ,
		10) = '${bizdate}'
	group by
		1,
		2
	HAVING
		sum(hdfs_bytes_read)/ 1024 / 1024 / 1024 >= 500
		or sum(input_records_processed) >= 10000000000
),
t2 as (select * from dwd_task_applications  where dt = '${bizdate}' and project_id != 33),
t3 as (
select
	application_id ,
	stage_sqlId,
	sum(task_recordsRead) as total_read_records ,
	round(sum(task_bytesRead)/1024/1024/1024,2) as total_read_bytes
from
	spark_task st
where
	left(stage_submissionTime,
	10) = CURDATE()
group by
	application_id ,
	stage_sqlId HAVING sum(task_bytesRead)/1024/1024/1024 >=500 or sum(task_recordsRead) >= 10000000000 )
select
	t1.application_id,
	b.dt,
	split(t1.dag_id,
	'_')[4] as sql_seq,
	b.project_name ,
	b.process_name ,
	b.task_name ,
	b.task_state ,
	b.start_time ,
	b.end_time ,
	timestampdiff(second, b.start_time, b.end_time),
	t1.total_read_records,
	t1.total_read_bytes
from
	t1
left join t2 b on
	t1.application_id = b.application_id
union all
select
	t3.application_id,
	b.dt,
	t3.stage_sqlId,
	b.project_name ,
	b.process_name ,
	b.task_name ,
	b.task_state ,
	b.start_time ,
	b.end_time ,
	timestampdiff(second, b.start_time,b.end_time),
	t3.total_read_records,
	t3.total_read_bytes
from
	t3
left join t2 b on
	t3.application_id = b.application_id;


insert into task_fulllink_application_container_cost
with yarn_container_state_time_cte as(select application_id,container_id,from_state,to_state,change_time,timestamp_ms from ds.yarn_container_state_time where change_time >= '${today}' and change_time < date_add('${today}', INTERVAL 1 DAY) )
select sum1.application_id,'${today}' as dt,sum1.project_name,sum1.process_name,sum1.process_instance_name,sum1.task_name,sum1.task_state,sum1.yarn_user,sum1.yarn_queue,sum1.finalStatus,sum1.vcoreSeconds,sum1.vcoreSeconds,sum1.task_cost_times
,sum2.task_cost_times as task_avg_cost_times,(sum1.task_cost_times-sum2.task_cost_times) as task_growth_cost_times,round((sum1.task_cost_times-sum2.task_cost_times)/sum2.task_cost_times,4) as task_cost_times_ratio
,sum1.retry_times,sum1.max_retry_times,sum1.task_start_time,sum1.task_end_time,sum1.am_share_change_time,sum1.container_share_change_time
,sum1.am_share_cost_times,sum1.container_share_cost,sum1.am_localize_cost,sum1.container_localize_cost,sum1.container_running_cost,sum1.container_cnt,sum1.input_bytes_read
from(
select min(col1.application_id) as application_id,col6.project_name,col6.process_name,col6.task_name,col6.state as task_state,col6.process_definition_id
,col6.user as yarn_user,col6.queue as yarn_queue,col6.finalStatus ,sum(col6.memorySeconds) as memorySeconds,sum(col6.vcoreSeconds)as vcoreSeconds,avg(col6.cost_times) as task_cost_times
,col6.process_instance_name,max(col6.retry_times) as retry_times,max(col6.max_retry_times) as max_retry_times,col6.start_time as task_start_time,col6.end_time as task_end_time,min(col1.am_share_change_time) as am_share_change_time,min(col2.container_share_change_time) as container_share_change_time
,sum(col1.am_share_cost_times)/1000 as am_share_cost_times,sum(col2.container_share_cost)/1000 as container_share_cost,sum(col7.am_localize_cost)/1000 as am_localize_cost,sum(col3.container_localize_cost)/1000 as container_localize_cost
,sum(col4.container_running_cost)/1000 as container_running_cost,sum(col5.container_cnt) as container_cnt,sum(round(col8.input_bytes_read/1024/1024,3)) as input_bytes_read
from(
	select t1.application_id,t1.change_time as am_share_change_time,t2.change_time,t2.timestamp_ms-t1.timestamp_ms as am_share_cost_times from
    (
	select application_id,container_id,change_time,timestamp_ms
	from yarn_container_state_time_cte 
	where container_id like '%000001'
	and to_state='NEW' 
	)t1
	left join 
	(
	select application_id,container_id,change_time,timestamp_ms
	from yarn_container_state_time_cte 
	where container_id like '%000001' and to_state = 'ACQUIRED' 
	)t2 on t1.application_id = t2.application_id and t1.container_id = t2.container_id
)col1
left join 
(	
	select t1.application_id,t1.change_time as container_share_change_time,t2.change_time,t2.timestamp_ms-t1.timestamp_ms as container_share_cost from 
	(select application_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms ) as rn 
	from yarn_container_state_time_cte
	where container_id not like '%000001' and to_state = 'ALLOCATED'
	)t1
	left join 
	(
	select application_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms desc) as rn  
	from yarn_container_state_time_cte
	where container_id not like '%000001' and to_state = 'ACQUIRED' 
	)t2 on t1.application_id = t2.application_id
	where t1.rn =1 and t2.rn =1
)col2 on col1.application_id = col2.application_id
left join 
(
	select t1.application_id,t2.change_time,t2.timestamp_ms-t1.timestamp_ms as container_localize_cost from 
	(select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms ) as rn 
	from yarn_container_state_time_cte
	where to_state = 'LOCALIZING' and container_id not like '%000001' 
	)t1
	left join 
	(
	select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms desc) as rn  
	from yarn_container_state_time_cte
	where to_state = 'SCHEDULED' and container_id not like '%000001' 
	)t2 on t1.application_id = t2.application_id
	where t1.rn =1 and t2.rn =1
)col3 on col1.application_id = col3.application_id
left join
(
	select t1.application_id,t2.change_time,t2.timestamp_ms-t1.timestamp_ms as container_running_cost from 
	(select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms ) as rn 
	from yarn_container_state_time_cte
	where container_id not like '%000001' and from_state = 'SCHEDULED' 
	)t1
	left join 
	(
	select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms desc) as rn  
	from yarn_container_state_time_cte
	where container_id not like '%000001' and (to_state = 'COMPLETED' or to_state = 'KILLING') 
	)t2 on t1.application_id = t2.application_id
	where t1.rn =1 and t2.rn =1
)col4 on col1.application_id = col4.application_id
left join
(
	select application_id,count(distinct container_id) as container_cnt
	from yarn_container_state_time_cte
	group by application_id
)col5 on col1.application_id = col5.application_id
join 
(
	select a.dt,a.application_id,a.process_definition_id,e.user,e.queue,e.finalStatus ,e.memorySeconds,e.vcoreSeconds ,b.name as project_name,c.name as process_name,d.name as task_name,d.state ,d.start_time ,d.end_time,f.name as process_instance_name,d.retry_times,d.max_retry_times,TIME_TO_SEC(TIMEDIFF(d.end_time,d.start_time)) as cost_times
	from ds.task_applications a
	join ds.mysql_t_ds_project b on a.project_id = b.id 
	join ds.mysql_t_ds_process_definition c on a.process_definition_id = c.id 
	join ds.mysql_t_ds_task_instance d on a.task_id = d.id and DATE_FORMAT(d.start_time,'%Y%m%d') = '${today}'
	join ds.mysql_t_ds_process_instance f on a.process_instance_id = f.id and DATE_FORMAT(f.start_time,'%Y%m%d') = '${today}'
	join ds.yarn_applications e on a.application_id = e.id and DATE_FORMAT(a.dt,'%Y%m%d') = '${today}'
	where DATE_FORMAT(a.dt,'%Y%m%d') = '${today}'
)col6 on col1.application_id = col6.application_id
left join
(
	select t1.application_id,t2.change_time,t2.timestamp_ms-t1.timestamp_ms as am_localize_cost from 
	(select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms ) as rn 
	from yarn_container_state_time_cte
	where to_state = 'LOCALIZING' and container_id like '%000001'  
	)t1
	left join 
	(
	select application_id,container_id,change_time,timestamp_ms,ROW_NUMBER() OVER ( PARTITION BY application_id ORDER BY timestamp_ms desc) as rn  
	from yarn_container_state_time_cte
	where to_state = 'SCHEDULED' and container_id like '%000001' 
	)t2 on t1.application_id = t2.application_id 
	where  t1.rn =1 and t2.rn =1
)col7 on col1.application_id = col7.application_id
left join (
select application_id,sum(if(hdfs_bytes_read is null,0,hdfs_bytes_read)) + sum(if(file_bytes_read is null,0,file_bytes_read)) as input_bytes_read
from ds.yarn_container_counters
where vertex_name like 'Map%' and DATE_FORMAT(counter_time,'%Y%m%d') = '${today}' 
group by application_id
)col8 on col1.application_id = col8.application_id
group by col6.project_name,col6.process_name,col6.task_name,col6.state,col6.process_definition_id
,col6.user,col6.queue,col6.finalStatus,col6.start_time,col6.end_time,col6.process_instance_name
)sum1
join 
(
select 
name as task_name ,process_definition_id,round(avg(tmp.task_cost_times)) as task_cost_times
from (select 
name,process_definition_id ,TIME_TO_SEC(TIMEDIFF(end_time,start_time)) as task_cost_times
,ROW_NUMBER() OVER ( PARTITION BY name ,process_definition_id ORDER BY start_time desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY name ,process_definition_id ORDER BY start_time asc) as rnk2
from mysql_t_ds_task_instance where DATE_FORMAT(start_time,'%Y%m%d') >= '${14lastday}' and state  = 7
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by name ,process_definition_id
)sum2 on sum1.process_definition_id = sum2.process_definition_id and sum1.task_name = sum2.task_name
;


insert into task_fulllink_optimizable_task_info
select col1.application_id,col1.dt,'AM分配耗时检查' as optimizable_task_type,col1.project_name ,col1.process_name ,col1.task_name
,col1.task_state,col1.task_start_time,col1.task_end_time,col1.task_cost_times
,col1.yarn_user,col1.yarn_queue,col1.am_share_cost,col1.avg_am_share_cost,col1.am_share_cost_ratio,null as error_info
,col1.am_used_memory_ratio,col1.am_used_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.am_share_cost,t1.yarn_user,t1.yarn_queue
,t2.am_share_cost as avg_am_share_cost
,round((t1.am_share_cost-t2.am_share_cost)/t2.am_share_cost,4) as am_share_cost_ratio
,t1.am_used_memory_ratio,t1.am_used_vcores_ratio
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times
from 
(select a.application_id ,a.dt,a.yarn_user,a.yarn_queue,
a.project_name ,a.process_name ,a.task_name,a.am_share_cost,a.am_localize_cost,a.container_share_cost,a.container_localize_cost,a.container_running_cost,a.container_num
,a.task_state,a.task_start_time,a.task_end_time,a.task_cost_times
,round((b.am_used_resources_memory+b.used_resources_memory) / b.max_resources_memory,4) as am_used_memory_ratio,round((b.am_used_resources_vcores+b.used_resources_vcores )/b.max_resources_vcores,4) as am_used_vcores_ratio
from task_fulllink_application_container_cost a 
join task_fulllink_yarn_queue_resources b on a.yarn_queue = b.queue_name and DATE_FORMAT(a.dt,'%Y%m%d') = '${today}'  and DATE_FORMAT(a.am_share_change_time,'%Y%m%d %H:%i') = DATE_FORMAT(b.`timestamp`,'%Y%m%d %H:%i')
where a.am_share_cost >= 2 and a.task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.am_share_cost),3) as am_share_cost
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY am_share_cost desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY am_share_cost asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' and DATE_FORMAT(dt,'%Y%m%d') < '${today}'
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col1
;

insert into task_fulllink_optimizable_task_info
select col2.application_id,col2.dt,'AM本地化耗时检查' as optimizable_task_type,col2.project_name ,col2.process_name ,col2.task_name
,col2.task_state,col2.task_start_time,col2.task_end_time,col2.task_cost_times
,col2.yarn_user,col2.yarn_queue,col2.am_localize_cost,col2.avg_am_localize_cost,col2.am_localize_cost_ratio,null as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.am_localize_cost,t1.yarn_user,t1.yarn_queue
,round((t1.am_localize_cost-t2.am_localize_cost)/t2.am_localize_cost,4) as am_localize_cost_ratio
,t2.am_localize_cost as avg_am_localize_cost
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times
from 
(select application_id ,dt,yarn_user,yarn_queue,task_state,task_start_time,task_end_time,task_cost_times
,project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' 
and am_localize_cost >= 2 and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.am_localize_cost),3) as am_localize_cost
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY am_localize_cost desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY am_localize_cost asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}'  and DATE_FORMAT(dt,'%Y%m%d') < '${today}'
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col2
;

insert into task_fulllink_optimizable_task_info
select col3.application_id,col3.dt,'container分配耗时检查' as optimizable_task_type,col3.project_name ,col3.process_name ,col3.task_name
,col3.task_state,col3.task_start_time,col3.task_end_time,col3.task_cost_times
,col3.yarn_user,col3.yarn_queue,col3.container_share_cost,col3.avg_container_share_cost,col3.container_share_cost_ratio,null as error_info
,container_used_memory_ratio,container_used_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_share_cost,t1.yarn_user,t1.yarn_queue
,round((t1.container_share_cost-t2.container_share_cost)/t2.container_share_cost,4) as container_share_cost_ratio
,t2.container_share_cost as avg_container_share_cost,t1.container_used_memory_ratio,t1.container_used_vcores_ratio
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times
from 
(select a.application_id ,a.dt,a.yarn_user,a.yarn_queue
,a.task_state,a.task_start_time,a.task_end_time,a.task_cost_times
,a.project_name ,a.process_name ,a.task_name,a.am_share_cost,a.am_localize_cost,a.container_share_cost,a.container_localize_cost,a.container_running_cost,a.container_num
,round((b.used_resources_memory+b.am_used_resources_memory) / b.max_resources_memory,4) as container_used_memory_ratio,round((b.used_resources_vcores+b.am_used_resources_vcores)/b.max_resources_vcores,4) as container_used_vcores_ratio
from task_fulllink_application_container_cost a
join task_fulllink_yarn_queue_resources b on a.yarn_queue = b.queue_name and  DATE_FORMAT(a.dt,'%Y%m%d') = '${today}' and DATE_FORMAT(a.dt,'%Y%m%d') = '${today}'  and DATE_FORMAT(a.am_share_change_time,'%Y%m%d %H:%i') = DATE_FORMAT(b.`timestamp`,'%Y%m%d %H:%i') 
where a.task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.container_share_cost),3) as container_share_cost
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_share_cost desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_share_cost asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' and DATE_FORMAT(dt,'%Y%m%d') < '${today}'
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col3
;

insert into task_fulllink_optimizable_task_info
select col4.application_id,col4.dt,'container本地化耗时检查' as optimizable_task_type,col4.project_name ,col4.process_name ,col4.task_name
,col4.task_state,col4.task_start_time,col4.task_end_time,col4.task_cost_times
,col4.yarn_user,col4.yarn_queue,col4.container_localize_cost,col4.avg_container_localize_cost,col4.container_localize_cost_ratio,null as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_localize_cost,t1.yarn_user,t1.yarn_queue
,round((t1.container_localize_cost-t2.container_localize_cost)/t2.container_localize_cost,4) as container_localize_cost_ratio
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t2.container_localize_cost as avg_container_localize_cost
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.container_localize_cost),3) as container_localize_cost
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_localize_cost desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_localize_cost asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' and DATE_FORMAT(dt,'%Y%m%d') < '${today}'
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col4
;

insert into task_fulllink_optimizable_task_info
select col5.application_id,col5.dt,'container运行耗时检查' as optimizable_task_type,col5.project_name ,col5.process_name ,col5.task_name
,col5.task_state,col5.task_start_time,col5.task_end_time,col5.task_cost_times
,col5.yarn_user,col5.yarn_queue,col5.container_running_cost,col5.avg_container_running_cost,col5.container_running_cost_ratio,null as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_running_cost,t1.yarn_user,t1.yarn_queue
,round((t1.container_running_cost-t2.container_running_cost)/t2.container_running_cost,4) as container_running_cost_ratio
,t2.container_running_cost as avg_container_running_cost
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.container_running_cost),3) as container_running_cost
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_running_cost desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY container_running_cost asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' and DATE_FORMAT(dt,'%Y%m%d') < '${today}'
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col5
;

insert into task_fulllink_optimizable_task_info
select col6.application_id,col6.dt,'map阶段任务长尾检查' as optimizable_task_type,col6.project_name ,col6.process_name ,col6.task_name
,col6.task_state,col6.task_start_time,col6.task_end_time,col6.task_cost_times
,col6.yarn_user,col6.yarn_queue,col6.map_time_taken,col6.avg_map_median_time_taken,col6.map_time_taken_ratio,col6.sql_index as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_running_cost,t1.yarn_user,t1.yarn_queue
,t2.map_time_taken,round((t2.map_time_taken-t2.map_median_time_taken)/t2.map_median_time_taken,4) as map_time_taken_ratio
,t2.map_median_time_taken as avg_map_median_time_taken
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t2.sql_index
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
  select tmp.application_id,tmp.dt,case when tmp.dag_id like 'dag_%' then split_part(dag_id,'_',4)
when tmp.dag_id like 'task_%' then 1 end as sql_index,map_time_taken,tmp.map_median_time_taken,tmp.map_bytes_read, tmp.map_median_bytes_read
  from (
  select
  a1.application_id,a1.dt,a1.dag_id,round(a1.timeTaken/1000,3) as map_time_taken,round(a2.median_timeTaken/1000,3) as map_median_time_taken
  ,round(a1.hdfs_bytes_read/1024/1024,3) as map_bytes_read,round(a2.median_bytes_read/1024/1024,3) as map_median_bytes_read
  ,ROW_NUMBER() OVER ( PARTITION BY a1.application_id ORDER BY round(a1.timeTaken/1000,3) desc ) as rn
  from (
  select application_id,DATE_FORMAT(counter_time ,'%Y%m%d') as dt,dag_id,timetaken,file_bytes_read,hdfs_bytes_read
  from yarn_container_counters
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 3) = 'Map'
  )a1
  left join (
  SELECT
    application_id ,PERCENTILE_DISC(timeTaken, 0.5) as median_timeTaken,PERCENTILE_DISC(file_bytes_read+hdfs_bytes_read, 0.5) as median_bytes_read
  FROM yarn_container_counters 
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 3) = 'Map'
  group by application_id
  )a2 on a1.application_id = a2.application_id
  )tmp
  where tmp.rn = 1
)t2 on t1.application_id = t2.application_id
)col6
;

insert into task_fulllink_optimizable_task_info
select col7.application_id,col7.dt,'map阶段数据倾斜检查' as optimizable_task_type,col7.project_name ,col7.process_name ,col7.task_name
,col7.task_state,col7.task_start_time,col7.task_end_time,col7.task_cost_times
,col7.yarn_user,col7.yarn_queue,col7.map_bytes_read,col7.avg_map_median_bytes_read,col7.map_bytes_read_ratio,sql_index as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_running_cost,t1.yarn_user,t1.yarn_queue
,t2.map_bytes_read,round((t2.map_bytes_read-t2.map_median_bytes_read)/t2.map_median_bytes_read,4) as map_bytes_read_ratio
,t2.map_median_bytes_read as avg_map_median_bytes_read
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t2.sql_index
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
  select tmp.application_id,tmp.dt,case when tmp.dag_id like 'dag_%' then split_part(dag_id,'_',4)
when tmp.dag_id like 'task_%' then 1 end as sql_index,map_time_taken,tmp.map_median_time_taken,tmp.map_bytes_read, tmp.map_median_bytes_read
  from (
  select
  a1.application_id,a1.dt,a1.dag_id,round(a1.timeTaken/1000,3) as map_time_taken,round(a2.median_timeTaken/1000,3) as map_median_time_taken
  ,round(a1.hdfs_bytes_read/1024/1024,3) as map_bytes_read,round(a2.median_bytes_read/1024/1024,3) as map_median_bytes_read
  ,ROW_NUMBER() OVER ( PARTITION BY a1.application_id ORDER BY round(a1.hdfs_bytes_read/1024/1024,3) desc ) as rn
  from (
  select application_id,DATE_FORMAT(counter_time ,'%Y%m%d') as dt,dag_id,timetaken,file_bytes_read,hdfs_bytes_read
  from yarn_container_counters
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 3) = 'Map'
  )a1
  left join (
  SELECT
    application_id ,PERCENTILE_DISC(timeTaken, 0.5) as median_timeTaken,PERCENTILE_DISC(file_bytes_read+hdfs_bytes_read, 0.5) as median_bytes_read
  FROM yarn_container_counters 
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 3) = 'Map'
  group by application_id
  )a2 on a1.application_id = a2.application_id
  )tmp
  where tmp.rn = 1
)t2 on t1.application_id = t2.application_id
)col7
;

insert into task_fulllink_optimizable_task_info
select col8.application_id,col8.dt,'reduce阶段任务长尾检查' as optimizable_task_type,col8.project_name ,col8.process_name ,col8.task_name
,col8.task_state,col8.task_start_time,col8.task_end_time,col8.task_cost_times
,col8.yarn_user,col8.yarn_queue,col8.reducer_time_taken,col8.avg_reducer_median_time_taken,col8.reducer_time_taken_ratio,col8.sql_index as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_running_cost,t1.yarn_user,t1.yarn_queue
,t2.reducer_time_taken,round((t2.reducer_time_taken-t2.reducer_median_time_taken)/t2.reducer_median_time_taken,4) as reducer_time_taken_ratio
,t2.reducer_median_time_taken as avg_reducer_median_time_taken
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t2.sql_index
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
  select tmp.application_id,tmp.dag_id
  ,case when tmp.dag_id like 'dag_%' then split_part(dag_id,'_',4)
  when tmp.dag_id like 'task_%' then 1 end as sql_index
  ,reducer_time_taken,reducer_median_time_taken,reducer_shuffle_bytes,reducer_median_shuffle_bytes
  from (
  select
  t1.application_id,t1.dag_id,round(t1.timeTaken/1000,3) as reducer_time_taken,round(t2.median_timeTaken/1000,3) as reducer_median_time_taken
  ,round(t1.shuffle_bytes/1024/1024,3) as reducer_shuffle_bytes,round(t2.median_shuffle_bytes/1024/1024,3) as reducer_median_shuffle_bytes
  ,ROW_NUMBER() OVER ( PARTITION BY t1.application_id ORDER BY round(t1.timeTaken/1000,3) desc ) as rn
  from (
  select application_id,dag_id,timetaken,shuffle_bytes
  from yarn_container_counters
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 7) = 'Reducer'
  )t1
  left join (
  SELECT
    application_id ,PERCENTILE_DISC(timeTaken, 0.5) as median_timeTaken,PERCENTILE_DISC(shuffle_bytes, 0.5) as median_shuffle_bytes
  FROM yarn_container_counters 
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 7) = 'Reducer'
  group by application_id
  )t2 on t1.application_id = t2.application_id
  )tmp
  where tmp.rn = 1
)t2 on t1.application_id = t2.application_id
)col8
;

insert into task_fulllink_optimizable_task_info
select col9.application_id,col9.dt,'reduce阶段数据倾斜检查' as optimizable_task_type,col9.project_name ,col9.process_name ,col9.task_name
,col9.task_state,col9.task_start_time,col9.task_end_time,col9.task_cost_times
,col9.yarn_user,col9.yarn_queue,col9.reducer_shuffle_bytes,col9.avg_reducer_median_shuffle_bytes,col9.reducer_shuffle_bytes_ratio,col9.sql_index as error_info
,null as yarn_resource_memory_ratio,null as yarn_resource_vcores_ratio
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.container_running_cost,t1.yarn_user,t1.yarn_queue
,t2.reducer_shuffle_bytes,round((t2.reducer_shuffle_bytes-t2.reducer_median_shuffle_bytes)/t2.reducer_median_shuffle_bytes,4) as reducer_shuffle_bytes_ratio
,t2.reducer_median_shuffle_bytes as avg_reducer_median_shuffle_bytes
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t2.sql_index
from 
(select application_id  ,dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,task_state,task_start_time,task_end_time,task_cost_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
  select tmp.application_id,tmp.dag_id
  ,case when tmp.dag_id like 'dag_%' then split_part(dag_id,'_',4)
  when tmp.dag_id like 'task_%' then 1 end as sql_index
  ,reducer_time_taken,reducer_median_time_taken,reducer_shuffle_bytes,reducer_median_shuffle_bytes
  from (
  select
  t1.application_id,t1.dag_id,round(t1.timeTaken/1000,3) as reducer_time_taken,round(t2.median_timeTaken/1000,3) as reducer_median_time_taken
  ,round(t1.shuffle_bytes/1024/1024,3) as reducer_shuffle_bytes,round(t2.median_shuffle_bytes/1024/1024,3) as reducer_median_shuffle_bytes
  ,ROW_NUMBER() OVER ( PARTITION BY t1.application_id ORDER BY round(t1.shuffle_bytes/1024/1024,3) desc ) as rn
  from (
  select application_id,dag_id,timetaken,shuffle_bytes
  from yarn_container_counters
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 7) = 'Reducer'
  )t1
  left join (
  SELECT
    application_id ,PERCENTILE_DISC(timeTaken, 0.5) as median_timeTaken,PERCENTILE_DISC(shuffle_bytes, 0.5) as median_shuffle_bytes
  FROM yarn_container_counters 
  where DATE_FORMAT(counter_time ,'%Y%m%d') = '${today}' and substr(vertex_name, 1, 7) = 'Reducer'
  group by application_id
  )t2 on t1.application_id = t2.application_id
  )tmp
  where tmp.rn = 1
)t2 on t1.application_id = t2.application_id
)col9
;

insert into task_fulllink_error_task_info 
select col1.application_id,col1.dt,'运行时长增长任务检查' as error_task_type,col1.project_name ,col1.process_name ,col1.task_name
,col1.task_state,col1.task_start_time,col1.task_end_time,col1.task_cost_times,col1.yarn_user,col1.yarn_queue
,col1.data_value,col1.avg_value,col1.growth_value,col1.growth_ratio,null as error_info
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.task_state,t1.task_start_time,t1.task_end_time
,t1.task_cost_times,t1.yarn_user,t1.yarn_queue,t1.task_cost_times as data_value,t2.task_cost_times as avg_value
,(t1.task_cost_times-t2.task_cost_times) as growth_value
,round((t1.task_cost_times-t2.task_cost_times)/t2.task_cost_times,4) as growth_ratio
from 
(select application_id ,dt,yarn_user,yarn_queue,task_state,task_cost_times,task_start_time,task_end_time
,project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.task_cost_times)) as task_cost_times
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,task_cost_times,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' 
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col1
;

insert into task_fulllink_error_task_info 
select col1.application_id,col1.dt,'container申请数量降低检查' as error_task_type,col1.project_name ,col1.process_name ,col1.task_name
,col1.task_state,col1.task_start_time,col1.task_end_time,col1.task_cost_times,col1.yarn_user,col1.yarn_queue
,col1.data_value,col1.avg_value,col1.growth_value,col1.growth_ratio,null as error_info
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.task_state,t1.task_start_time,t1.task_end_time
,t1.task_cost_times,t1.yarn_user,t1.yarn_queue,t1.container_num as data_value,t2.container_num as avg_value
,(t1.container_num-t2.container_num) as growth_value
,round((t1.container_num-t2.container_num)/t2.container_num,4) as growth_ratio
from 
(select application_id ,dt,yarn_user,yarn_queue,task_state,task_cost_times,task_start_time,task_end_time
,project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,round(avg(tmp.container_num)) as container_num
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,task_cost_times,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' 
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col1
;

insert into task_fulllink_error_task_info 
select col1.application_id,col1.dt,'运行失败任务检查' as error_task_type,col1.project_name ,col1.process_name ,col1.task_name
,col1.task_state,col1.task_start_time,col1.task_end_time,col1.task_cost_times,col1.yarn_user,col1.yarn_queue
,col1.data_value,col1.avg_value,col1.growth_value,col1.growth_ratio,null as error_info
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.task_state,t1.task_start_time,t1.task_end_time
,t1.task_cost_times,t1.yarn_user,t1.yarn_queue,t2.retry_times as data_value,null as avg_value
,null as growth_value
,null as growth_ratio
from 
(select application_id ,dt,yarn_user,yarn_queue,task_state,task_cost_times,task_start_time,task_end_time
,project_name ,process_name ,process_instance_name,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' 
and task_state = 6
)t1
left join (
select project_name ,process_name,process_instance_name,task_name,max(retry_times) as retry_times
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}'
group by project_name ,process_name,process_instance_name,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name and t1.process_instance_name = t2.process_instance_name
)col1
;

insert into task_fulllink_error_task_info 
select col1.application_id,col1.dt,'input读取量增加检查' as error_task_type,col1.project_name ,col1.process_name ,col1.task_name
,col1.task_state,col1.task_start_time,col1.task_end_time,col1.task_cost_times,col1.yarn_user,col1.yarn_queue
,col1.data_value,col1.avg_value,col1.growth_value,col1.growth_ratio,null as error_info
from 
(
select 
t1.application_id,t1.dt,t1.project_name ,t1.process_name ,t1.task_name,t1.task_state,t1.task_start_time,t1.task_end_time
,t1.task_cost_times,t1.yarn_user,t1.yarn_queue,t1.input_bytes_read as data_value,t2.input_bytes_read as avg_value
,(t1.input_bytes_read-t2.input_bytes_read) as growth_value
,round((t1.input_bytes_read-t2.input_bytes_read)/t2.input_bytes_read,4) as growth_ratio
from 
(select application_id ,dt,yarn_user,yarn_queue,task_state,task_cost_times,task_start_time,task_end_time
,project_name ,process_name ,task_name,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num,input_bytes_read
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') = '${today}' and task_cost_times > 600
)t1
left join 
(
select 
project_name ,process_name ,task_name,avg(tmp.input_bytes_read) as input_bytes_read
from (select 
dt,yarn_user,yarn_queue,
project_name ,process_name ,task_name,task_cost_times,am_share_cost,am_localize_cost,container_share_cost,container_localize_cost,container_running_cost,container_num,input_bytes_read
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY project_name ,process_name ,task_name  ORDER BY task_start_time asc) as rnk2
from task_fulllink_application_container_cost where DATE_FORMAT(dt,'%Y%m%d') >= '${14lastday}' 
)tmp
where tmp.rnk1<>1 and tmp.rnk2<>1 and tmp.rnk1 <=14
group by project_name ,process_name ,task_name
)t2 on t1.project_name = t2.project_name and t1.process_name = t2.process_name and t1.task_name = t2.task_name
)col1
;

insert into task_fulllink_error_task_info 
select a1.task_id,DATE_FORMAT(a1.start_time ,'%Y%m%d') as dt,'连续失败任务检查' as error_task_type,a1.project_name,a1.process_name ,a1.task_name
,a1.task_state ,a1.start_time ,a1.end_time ,a1.task_cost_times,null as yarn_user,null as yarn_queue
,a2.final_fail_cnt_3,null as avg_value,null as growth_value,null as growth_ratio,null as error_info
from 
(
	select t1.id as task_id,t3.name as project_name,t2.name as process_name ,t4.name as process_instance_name,t1.name as task_name
	,t1.state as task_state ,t1.start_time,t1.end_time,t1.task_type,TIME_TO_SEC(TIMEDIFF(t1.end_time,t1.start_time)) as task_cost_times,t1.retry_times,t1.max_retry_times
	,ROW_NUMBER() OVER ( PARTITION BY t3.name ,t2.name ,t1.name ORDER BY t1.start_time desc) as rnk1
	from ds.t_ds_task_instance t1
	join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
	join ds.t_ds_project t3 on t2.project_id = t3.id  
	join ds.t_ds_process_instance t4 on t1.process_instance_id = t4.id 
	where DATE_FORMAT(t1.start_time ,'%Y%m%d') >= '${today}' and t1.task_type not in ('DEPENDENT','CONDITIONS')
)a1
left join 
(
	select tmp1.project_name,tmp1.process_name,tmp1.task_name
		,sum(case when tmp1.retry_times >= 1 and tmp1.retry_times <= tmp1.max_retry_times and tmp1.task_state =7 then 1
		else 0
		end) as retry_fail_cnt_3
		,sum(case when tmp1.retry_times = tmp1.max_retry_times and task_state = 6 then 1
		else 0
		end) as final_fail_cnt_3
	from 
	(
		select 
		p1.process_instance_id,p1.project_name,p1.process_name,p1.process_instance_name,p2.name as task_name,p1.start_time,p1.rnk1,
		p2.state as task_state,p2.start_time,p2.retry_times,p2.max_retry_times
		from (
		select a1.process_instance_id,a1.project_name,a1.process_name,a1.process_instance_name,a1.start_time,a1.rnk1
		from 
			(select 
			t3.name as project_name,t2.name as process_name ,t1.name as process_instance_name,t1.id as process_instance_id
			,t1.state as task_state ,t1.start_time as start_time
			,ROW_NUMBER() OVER ( PARTITION BY t3.name,t2.name ORDER BY t1.start_time desc) as rnk1
			from ds.t_ds_process_instance t1
			join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
			join ds.t_ds_project t3 on t2.project_id = t3.id  
			where DATE_FORMAT(t1.start_time ,'%Y%m%d') >= '${3lastday}' 
			)a1
		where a1.rnk1 <= 3
		)p1
		left join ds.t_ds_task_instance p2 on p1.process_instance_id = p2.process_instance_id 
		where p2.retry_times >= 1 and p2.retry_times <= p2.max_retry_times
	)tmp1
	group by tmp1.project_name,tmp1.process_name,tmp1.task_name
)a2 on a1.project_name = a2.project_name and a1.process_name = a2.process_name and a1.task_name = a2.task_name
where a1.rnk1 = 1
;

insert into task_fulllink_error_task_info 
select a1.task_id,DATE_FORMAT(a1.start_time ,'%Y%m%d') as dt,'连续重试任务检查' as error_task_type,a1.project_name,a1.process_name ,a1.task_name
,a1.task_state ,a1.start_time ,a1.end_time ,a1.task_cost_times,null as yarn_user,null as yarn_queue
,a2.retry_fail_cnt_3,null as avg_value,null as growth_value,null as growth_ratio,null as error_info
from 
(
	select t1.id as task_id,t3.name as project_name,t2.name as process_name ,t4.name as process_instance_name,t1.name as task_name
	,t1.state as task_state ,t1.start_time,t1.end_time,t1.task_type,TIME_TO_SEC(TIMEDIFF(t1.end_time,t1.start_time)) as task_cost_times,t1.retry_times,t1.max_retry_times
	,ROW_NUMBER() OVER ( PARTITION BY t3.name ,t2.name ,t1.name ORDER BY t1.start_time desc) as rnk1
	from ds.t_ds_task_instance t1
	join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
	join ds.t_ds_project t3 on t2.project_id = t3.id
	join ds.t_ds_process_instance t4 on t1.process_instance_id = t4.id 
	where DATE_FORMAT(t1.start_time ,'%Y%m%d') >= '${today}' and t1.task_type not in ('DEPENDENT','CONDITIONS')
)a1
left join 
(
	select tmp1.project_name,tmp1.process_name,tmp1.task_name
		,sum(case when tmp1.retry_times >= 1 and tmp1.retry_times <= tmp1.max_retry_times and tmp1.task_state =7 then 1
		else 0
		end) as retry_fail_cnt_3
		,sum(case when tmp1.retry_times = tmp1.max_retry_times and task_state = 6 then 1
		else 0
		end) as final_fail_cnt_3
	from 
	(
		select 
		p1.process_instance_id,p1.project_name,p1.process_name,p1.process_instance_name,p2.name as task_name,p1.start_time,p1.rnk1,
		p2.state as task_state,p2.start_time,p2.retry_times,p2.max_retry_times
		from (
		select a1.process_instance_id,a1.project_name,a1.process_name,a1.process_instance_name,a1.start_time,a1.rnk1
		from 
			(select 
			t3.name as project_name,t2.name as process_name ,t1.name as process_instance_name,t1.id as process_instance_id
			,t1.state as task_state ,t1.start_time as start_time
			,ROW_NUMBER() OVER ( PARTITION BY t3.name,t2.name ORDER BY t1.start_time desc) as rnk1
			from ds.t_ds_process_instance t1
			join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
			join ds.t_ds_project t3 on t2.project_id = t3.id  
			where DATE_FORMAT(t1.start_time ,'%Y%m%d') >= '${7lastday}' 
			)a1
		where a1.rnk1 <= 7
		)p1
		left join ds.t_ds_task_instance p2 on p1.process_instance_id = p2.process_instance_id 
		where p2.retry_times >= 1 and p2.retry_times <= p2.max_retry_times
	)tmp1
	group by tmp1.project_name,tmp1.process_name,tmp1.task_name
)a2 on a1.project_name = a2.project_name and a1.process_name = a2.process_name and a1.task_name = a2.task_name
where a1.rnk1 = 1
;

insert into task_fulllink_error_task_info 
select tmp1.task_id,DATE_FORMAT(tmp1.start_time ,'%Y%m%d') as dt,'运行时长减少任务检查' as error_task_type,tmp1.project_name,tmp1.process_name ,tmp1.task_name
,tmp1.state ,tmp1.start_time ,tmp1.end_time ,tmp1.cost_times,null as yarn_user,null as yarn_queue
,tmp1.cost_times as data_value,tmp2.avg_cost_times as avg_value,(tmp1.cost_times-tmp2.avg_cost_times) as growth_value,round((tmp2.avg_cost_times/tmp1.cost_times),4) as growth_ratio,null as error_info
from 
(
select t1.id as task_id,t3.name as project_name,t2.name as process_name ,t4.name as process_instance_name,t1.name as task_name
,t1.state ,t1.start_time ,t1.end_time ,t1.task_type 
,TIME_TO_SEC(TIMEDIFF(t1.end_time , t1.start_time)) AS cost_times
from ds.t_ds_task_instance t1
join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
join ds.t_ds_project t3 on t2.project_id = t3.id  
join ds.t_ds_process_instance t4 on t1.process_instance_id = t4.id 
where DATE_FORMAT(t1.start_time ,'%Y%m%d') = '${today}' and t1.state = 7 and t1.task_type <> 'DEPENDENT' and t1.task_type <> 'CONDITIONS'
)tmp1
left join 
(select project_name,process_name,task_name,round(avg(a1.cost_times)) as avg_cost_times
from (select t3.name as project_name,t2.name as process_name ,t1.name as task_name
,t1.start_time
,TIME_TO_SEC(TIMEDIFF(t1.end_time,t1.start_time)) as cost_times
,ROW_NUMBER() OVER ( PARTITION BY t3.name,t2.name,t1.name ORDER BY TIME_TO_SEC(TIMEDIFF(t1.end_time,t1.start_time)) desc) as rnk1
,ROW_NUMBER() OVER ( PARTITION BY t3.name,t2.name,t1.name ORDER BY TIME_TO_SEC(TIMEDIFF(t1.end_time,t1.start_time)) asc) as rnk2
,ROW_NUMBER() OVER ( PARTITION BY t3.name,t2.name,t1.name ORDER BY t1.start_time desc) as rnk3
from ds.t_ds_task_instance t1
join ds.t_ds_process_definition t2 on t1.process_definition_id = t2.id 
join ds.t_ds_project t3 on t2.project_id = t3.id  
where DATE_FORMAT(t1.start_time ,'%Y%m%d') >= '${14lastday}' and t1.state = 7 and t1.task_type <> 'DEPENDENT' and t1.task_type <> 'CONDITIONS'
)a1
where a1.rnk1<>1 and a1.rnk2<>1 and a1.rnk3 <=14 
group by project_name,process_name,task_name
)tmp2 on tmp1.project_name = tmp2.project_name and tmp1.process_name = tmp2.process_name and tmp1.task_name = tmp2.task_name
;


insert into task_fulllink_error_task_analysis
select 
    tmp.application_id,tmp.dt,tmp.check_type,tmp.project_name,tmp.process_name,tmp.task_name,tmp.task_state,tmp.task_start_time,tmp.task_end_time,tmp.task_cost_times,tmp.yarn_user,tmp.yarn_queue 
    ,tmp.data_value,tmp.avg_value,tmp.growth_value,tmp.growth_ratio,tmp.question_type
    ,case when tmp.check_keyword is not null then 0
    when tmp.check_keyword is null then 1
    end as check_status
    ,tmp.check_keyword,tmp.question,tmp.quota_details
from (
select application_id,dt,optimizable_task_type as check_type,t1.project_name,t1.process_name,t1.task_name
,t1.task_state,t1.task_start_time,t1.task_end_time,t1.task_cost_times,t1.yarn_user,t1.yarn_queue,t1.stage_cost_times as data_value,t1.avg_stage_cost_times as avg_value,(t1.stage_cost_times-t1.avg_stage_cost_times) as growth_value,t1.stage_cost_times_ratio as growth_ratio
,'可优化任务' as question_type
,case when optimizable_task_type = 'AM分配耗时检查' and stage_cost_times_ratio >0.5 then 'AM分配耗时异常'
when optimizable_task_type = 'container分配耗时检查' and stage_cost_times_ratio >0.5 then 'container分配耗时异常'
when optimizable_task_type = 'AM本地化耗时检查' and stage_cost_times_ratio >0.5 then 'AM本地化耗时异常'
when optimizable_task_type = 'container本地化耗时检查' and stage_cost_times_ratio >0.5 then 'container本地化耗时异常'
when optimizable_task_type = 'container运行耗时检查' and stage_cost_times_ratio >0.5 then 'container运行耗时异常'
when optimizable_task_type = 'map阶段数据倾斜检查' and stage_cost_times >= 10 and stage_cost_times_ratio>= 2 then 'map阶段数据倾斜'
when optimizable_task_type = 'map阶段任务长尾检查' and stage_cost_times >= 30  and stage_cost_times_ratio>= 1.5 then 'map阶段任务长尾'
when optimizable_task_type = 'reduce阶段数据倾斜检查' and stage_cost_times >= 10 and stage_cost_times_ratio>= 2 then 'reduce阶段数据倾斜'
when optimizable_task_type = 'reduce阶段任务长尾检查' and stage_cost_times >= 30  and stage_cost_times_ratio>= 1.5 then 'reduce阶段任务长尾'
end as check_keyword
,case when optimizable_task_type = 'AM分配耗时检查' and stage_cost_times_ratio >0.5 and yarn_resource_memory_ratio*100 >= 90 then '队列剩余资源不足，导致AM分配时间过长'
when optimizable_task_type = 'container分配耗时检查' and stage_cost_times_ratio >0.5 and yarn_resource_memory_ratio*100 >= 90 then '队列剩余资源不足，导致container分配时间过长'
when optimizable_task_type = 'AM本地化耗时检查' and stage_cost_times_ratio >0.5 then 'AM本地化耗时超过历史50%'
when optimizable_task_type = 'container本地化耗时检查' and stage_cost_times_ratio >0.5 then 'container本地化耗时超过历史50%'
when optimizable_task_type = 'container运行耗时检查' and stage_cost_times_ratio >0.5 then 'container运行耗时超过历史50%'
when optimizable_task_type = 'map阶段数据倾斜检查' and stage_cost_times >= 10 and stage_cost_times_ratio>= 2 then 'map阶段数据倾斜'
when optimizable_task_type = 'map阶段任务长尾检查' and stage_cost_times >= 30  and stage_cost_times_ratio>= 1.5 then 'map阶段任务长尾'
when optimizable_task_type = 'reduce阶段数据倾斜检查' and stage_cost_times >= 10 and stage_cost_times_ratio>= 2 then 'reduce阶段数据倾斜'
when optimizable_task_type = 'reduce阶段任务长尾检查' and stage_cost_times >= 30  and stage_cost_times_ratio>= 1.5 then 'reduce阶段任务长尾'
end as question
,case when optimizable_task_type = 'AM分配耗时检查' then concat('队列名称:',yarn_queue,'\nAM分配耗时:',round(stage_cost_times,3),'s\n历史平均AM分配耗时:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%\nAM内存资源使用比例:',IFNULL(yarn_resource_memory_ratio*100,''),'%\nAM vcores资源使用比例',IFNULL(yarn_resource_vcores_ratio*100,''),'%')
  when optimizable_task_type = 'container分配耗时检查' then concat('队列名称:',yarn_queue,'\ncontainer分配耗时:',round(stage_cost_times,3),'s\n历史平均container分配耗时:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%\ncontainer内存资源使用比例:',IFNULL(yarn_resource_memory_ratio*100,''),'%\ncontainer vcores资源使用比例',IFNULL(yarn_resource_vcores_ratio*100,''),'%')
  when optimizable_task_type = 'AM本地化耗时检查' then concat('队列名称:',yarn_queue,'\nAM本地化耗时:',round(stage_cost_times,3),'s\n历史平均AM本地化耗时:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
  when optimizable_task_type = 'container本地化耗时检查' then concat('队列名称:',yarn_queue,'\ncontainer本地化耗时:',round(stage_cost_times,3),'s\n历史平均container本地化耗时:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
  when optimizable_task_type = 'container运行耗时检查' then concat('队列名称:',yarn_queue,'\ncontainer运行耗时:',round(stage_cost_times,3),'s\n历史平均container运行耗时:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
  when optimizable_task_type = 'map阶段数据倾斜检查' then concat('队列名称:',yarn_queue,'\nSQL序号:',error_info,'\nmap阶段最大读取数据量:',round(stage_cost_times,3),'MB\nmap阶段中位数读取数据量:',IFNULL(round(avg_stage_cost_times,3),''),'MB\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
when optimizable_task_type = 'map阶段任务长尾检查' then concat('队列名称:',yarn_queue,'\nSQL序号:',error_info,'\nmap阶段最大运行时间:',round(stage_cost_times,3),'s\nmap阶段中位数运行时间:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
when optimizable_task_type = 'reduce阶段数据倾斜检查' then concat('队列名称:',yarn_queue,'\nSQL序号:',error_info,'\nreduce阶段最大读取数据量:',round(stage_cost_times,3),'MB\nreduce阶段中位数读取数据量:',IFNULL(round(avg_stage_cost_times,3),''),'MB\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
when optimizable_task_type = 'reduce阶段任务长尾检查' then concat('队列名称:',yarn_queue,'\nSQL序号:',error_info,'\nreduce阶段最大运行时间:',round(stage_cost_times,3),'s\nreduce阶段中位数运行时间:',IFNULL(round(avg_stage_cost_times,3),''),'s\n增长率:',IFNULL(stage_cost_times_ratio*100,''),'%')
 end as quota_details 
from task_fulllink_optimizable_task_info t1
where DATE_FORMAT(dt,'%Y%m%d') = '${today}' 
)tmp
union all
select 
    tmp.application_id,tmp.dt,tmp.check_type,tmp.project_name,tmp.process_name,tmp.task_name,tmp.task_state,tmp.task_start_time,tmp.task_end_time,tmp.task_cost_times,tmp.yarn_user,tmp.yarn_queue 
    ,tmp.data_value,tmp.avg_value,tmp.growth_value,tmp.growth_ratio,tmp.question_type
    ,case when tmp.check_keyword is not null then 0
    when tmp.check_keyword is null then 1
    end as check_status
    ,tmp.check_keyword,tmp.question,tmp.quota_details
from (
select application_id,dt,error_type as check_type,project_name,process_name,task_name,task_state,task_start_time,task_end_time,task_cost_times,yarn_user,yarn_queue,data_value,avg_value,growth_value,growth_ratio
,'异常任务' as question_type
,case when error_type = 'container申请数量降低检查' and growth_ratio<= -0.5 then 'container申请数量降低任务'
when error_type = '运行时长增长任务检查' and growth_ratio >=0.5 then '运行时长增长任务'
when error_type = '运行失败任务检查' then '运行失败任务'
when error_type = 'input读取量增加检查' and growth_ratio >=0.5 then 'input读取量增加任务'
when error_type = '连续失败任务检查' and data_value >= 2 then '连续失败任务'
when error_type = '连续重试任务检查' and data_value >= 7 then '连续重试任务'
when error_type = '运行时长减少任务检查' and avg_value >= 600 and growth_ratio >= 5 then '运行时长减少任务'
end as check_keyword
,case when error_type = 'container申请数量降低检查' and growth_ratio<= -0.5 then '运行的container数量低于历史的50%'
when error_type = '运行时长增长任务检查' and growth_ratio >=0.5 then '运行时长超过历史的50%'
when error_type = '运行失败任务检查' then '运行失败任务'
when error_type = 'input读取量增加检查' and growth_ratio >=0.5 then 'input读取量超过历史的50%'
when error_type = '连续失败任务检查' and data_value >=2 then '连续失败任务'
when error_type = '连续重试任务检查' and data_value >=7 then '连续重试任务'
when error_type = '运行时长减少任务检查' and avg_value >= 600 and growth_ratio >= 5 then '运行时长减少任务'
end as question
,case when error_type = 'container申请数量降低检查' then concat('container申请数量:',round(data_value),'\n历史平均container申请数量:',IFNULL(round(avg_value),''),'\n增长率:',IFNULL(growth_ratio*100,''),'%')
when error_type = '运行时长增长任务检查' then concat('运行时长:',data_value,'s\n历史平均运行时长:',IFNULL(avg_value,''),'s\n增长率:',IFNULL(growth_ratio*100,''),'%')
when error_type = '运行失败任务检查' then concat('失败任务次数:',round(data_value),'次')
when error_type = 'input读取量增加检查' then concat('input读取量:',data_value,'MB\n历史平均input读取量:',IFNULL(avg_value,''),'MB\n增长率:',IFNULL(growth_ratio*100,''),'%')
when error_type = '连续失败任务检查' then concat('连续失败任务次数:',data_value,'次')
when error_type = '连续重试任务检查' then concat('连续重试任务次数:',data_value,'次')
when error_type = '运行时长减少任务检查' then concat('运行时长:',data_value,'s\n历史平均运行时长:',IFNULL(avg_value,''),'s\n减少比率:',IFNULL(growth_ratio*100,''),'%')
end as quota_details
from task_fulllink_error_task_info t2
where DATE_FORMAT(dt,'%Y%m%d') = '${today}' 
)tmp