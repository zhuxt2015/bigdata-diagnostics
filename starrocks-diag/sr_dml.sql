-- 把扫描量超过1千万的SQL记录
with t1 as (
select
	query_id,
start_time,
end_time,
user,
time_used,
state,
error_message,s
sql,
`database`,
	unnest
from
	ds.starrocks_query_record a,
	unnest(split(plan ,
	'ScanNode')) as unnest
where
	start_time BETWEEN '2025-03-25 00:00:00' and '2025-03-25 23:59:59'
	and locate('cardinality', plan)),
t2 as (
select
*
from
	t1
where
	locate('cardinality', unnest)),
t3 as (
select regexp_extract(unnest, 'TABLE: (.*?)\n', 1) as table_name,
regexp_extract(unnest, 'partitions=(.*?)\n', 1) as partitions,
split(regexp_extract(unnest, 'partitions=(.*?)\n', 1),'/')[1]/split(regexp_extract(unnest, 'partitions=(.*?)\n', 1),'/')[2] as partitions_ratio,
regexp_extract(unnest, 'cardinality=(.*?)\n', 1) as cardinality,
*
from t2 ) select * from t3 where  cardinality >= 10000000;