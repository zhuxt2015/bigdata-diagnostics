insert into dm_deptasks
select tmp1.project_name,tmp1.process_name,tmp1.task_name,tmp1.dt,tmp1.submit_time,tmp1.start_time,tmp1.end_time,tmp2.lastday_taken_minute,tmp1.taken_minute,round(TIME_TO_SEC(TIMEDIFF(tmp1.start_time,tmp2.start_time))-86400) as start_diff_second
,round((tmp1.taken_minute-tmp2.lastday_taken_minute)/tmp2.lastday_taken_minute,4) as dod,tmp1.retry_times as retry_times
,tmp2.retry_times as lastday_retry_times
from 
(select a1.project_name,a1.process_name,a1.task_name,a1.dt,a2.submit_time,a2.start_time,a2.end_time,round(a2.cost_times_retry/60,2) as taken_minute,a2.retry_times
from ds.mon_cachepc_full_task a1
join 
(select t1.name as task_name,t2.name as process_name,t3.name as project_name,t1.submit_time,t1.start_time,t1.end_time,round(TIME_TO_SEC(TIMEDIFF(t1.end_time , t1.start_time))) as cost_times
,ROW_NUMBER() OVER (PARTITION BY t1.name,t2.name,t3.name ORDER BY t1.start_time asc) as rn
,t1.retry_times 
,FIRST_VALUE(t1.start_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ASC) AS first_start_time
,LAST_VALUE(t1.end_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_time
,TIMESTAMPDIFF(
        SECOND
		,FIRST_VALUE(t1.start_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ASC)
		,LAST_VALUE(t1.end_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ) AS cost_times_retry
from ds.mysql_t_ds_task_instance t1
join ds.mysql_t_ds_process_definition t2 on t1.process_definition_id = t2.id 
join ds.mysql_t_ds_project t3 on t2.project_id = t3.id 
where DATE_FORMAT(t1.start_time,'%Y%m%d') = '${today}' and DATE_FORMAT(t1.start_time, '%H:%i:%s') < '06:00:00'
)a2 on a1.task_name = a2.task_name and a1.process_name = a2.process_name and a1.project_name = a2.project_name
where a1.dt = '${today}' and a2.rn = 1
)tmp1
left join 
(select a1.project_name,a1.process_name,a1.task_name,a1.dt,a2.submit_time,a2.start_time,a2.end_time,round(a2.cost_times_retry/60,2) as lastday_taken_minute,a2.retry_times
from ds.mon_cachepc_full_task a1
join 
(select t1.name as task_name,t2.name as process_name,t3.name as project_name,t1.submit_time,t1.start_time,t1.end_time,round(TIME_TO_SEC(TIMEDIFF(t1.end_time , t1.start_time))) as cost_times
,ROW_NUMBER() OVER (PARTITION BY t1.name,t2.name,t3.name ORDER BY t1.start_time asc) as rn
,t1.retry_times 
,FIRST_VALUE(t1.start_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ASC) AS first_start_time
,LAST_VALUE(t1.end_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_time
,TIMESTAMPDIFF(
        SECOND
		,FIRST_VALUE(t1.start_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ASC)
		,LAST_VALUE(t1.end_time) OVER (PARTITION BY t3.name,t2.name,t1.name,t1.process_instance_id ORDER BY t1.start_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    ) AS cost_times_retry
from ds.mysql_t_ds_task_instance t1
join ds.mysql_t_ds_process_definition t2 on t1.process_definition_id = t2.id 
join ds.mysql_t_ds_project t3 on t2.project_id = t3.id 
where DATE_FORMAT(t1.start_time,'%Y%m%d') = '${lastday}' and DATE_FORMAT(t1.start_time, '%H:%i:%s') <= '06:00:00'
)a2 on a1.task_name = a2.task_name and a1.process_name = a2.process_name and a1.task_name = a2.task_name
where a1.dt = '${lastday}' and a2.rn = 1
)tmp2 on tmp1.task_name = tmp2.task_name and tmp1.process_name = tmp2.process_name and tmp1.project_name = tmp2.project_name
