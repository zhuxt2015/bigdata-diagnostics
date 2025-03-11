--将解析后fsimage中路径、数据快、文件大小
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=128000000;
insert overwrite table storage_analyze.dwd_fsimage partition(partition_key)
select 
 path,
 p1,
 p2,
 p3,
 p4,
 p5,
 p6,
 concat(p6,'/',split(path, '/')[7]) as p7,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname,
 ${hiveconf:DATA_DT} as partition_key
from(
select path,
 p2 as p1,
 p3 as p2,
 p4 as p3,
 p5 as p4,
 p6 as p5,
 concat(p6,'/',split(path, '/')[6]) as p6,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
from (
select path,
 p2,
 p3,
 p4,
 p5,
 concat(p5,'/',split(path, '/')[5]) as p6,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
 from
 (select path,
 p2,
 p3,
 p4,
 concat(p4,'/',split(path, '/')[4]) as p5,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
 from 
 (select path,
 p2,
 p3,
 concat(p3,'/',split(path, '/')[3]) as p4,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
 from
 (select path,
 p2,
 concat(p2,'/',split(path, '/')[2] ) as p3,
 blockscount,
 filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
 from 
 (select path,
 concat('/',split(path, '/')[1] ) as p2,
 blockscount,
 case when replication=16020 then sum(filesize) 
      when replication=0 then sum(filesize)
	  when replication is null then sum(filesize)
	  else replication*filesize end filesize_total ,
 accesstime,
 filesize,
 replication,
 modificationtime,
 preferredblocksize,
 nsquota,
 dsquota,
 permission,
 username,
 groupname
 from 
 storage_analyze.ods_fsimage
 where partition_key='${hiveconf:DATA_DT}' and `path` <> 'Path'
 group by path,blockscount,accesstime,filesize,replication,modificationtime,preferredblocksize,nsquota,dsquota,permission,username,groupname
 )tmp
 )tmp1
 )tmp2
 )tmp3
 )tmp4
 )tmp5
;
