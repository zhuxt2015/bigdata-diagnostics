CREATE TABLE `dm_deptasks` (
  `project_name` varchar(65533) NOT NULL COMMENT "",
  `process_name` varchar(65533) NOT NULL COMMENT "",
  `task_name` varchar(65533) NOT NULL COMMENT "",
  `dt` date NOT NULL COMMENT "",
  `submit_time` datetime NULL COMMENT "",
  `start_time` datetime NULL COMMENT "",
  `end_time` datetime NULL COMMENT "",
  `lastday_taken_minute` decimal(38, 2) NULL COMMENT "",
  `taken_minute` decimal(38, 2) NULL COMMENT "耗时分钟数",
  `start_diff_second` int(11) NULL COMMENT "与昨日启动时间相差秒数",
  `dod` decimal(38, 6) NULL COMMENT "与昨日同比",
  `retry_times` varchar(65533) NULL COMMENT "",
  `lastday_retry_times` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`project_name`, `process_name`, `task_name`, `dt`)
COMMENT "OLAP"
PARTITION BY RANGE(`dt`)
(PARTITION p202402 VALUES [("2024-02-01"), ("2024-03-01")),
PARTITION p202403 VALUES [("2024-03-01"), ("2024-04-01")),
PARTITION p202404 VALUES [("2024-04-01"), ("2024-05-01")),
PARTITION p202405 VALUES [("2024-05-01"), ("2024-06-01")),
PARTITION p202406 VALUES [("2024-06-01"), ("2024-07-01")),
PARTITION p202407 VALUES [("2024-07-01"), ("2024-08-01")),
PARTITION p202408 VALUES [("2024-08-01"), ("2024-09-01")),
PARTITION p202409 VALUES [("2024-09-01"), ("2024-10-01")),
PARTITION p202410 VALUES [("2024-10-01"), ("2024-11-01")),
PARTITION p202411 VALUES [("2024-11-01"), ("2024-12-01")),
PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01")),
PARTITION p202504 VALUES [("2025-04-01"), ("2025-05-01")))
DISTRIBUTED BY HASH(`task_name`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"dynamic_partition.enable" = "true",
"dynamic_partition.end" = "1",
"dynamic_partition.history_partition_num" = "0",
"dynamic_partition.prefix" = "p",
"dynamic_partition.start" = "-13",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `mon_cachepc_full_task` (
  `dt` date NOT NULL COMMENT "分区字段",
  `project_id` varchar(65533) NOT NULL COMMENT "项目id",
  `process_id` varchar(65533) NOT NULL COMMENT "工作流id",
  `task_id` varchar(65533) NOT NULL COMMENT "任务id",
  `project_name` varchar(65533) NOT NULL COMMENT "项目名称",
  `process_name` varchar(65533) NOT NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NOT NULL COMMENT "任务名称",
  `insert_time` datetime NULL COMMENT "数据插入时间"
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `project_id`, `process_id`, `task_id`)
PARTITION BY RANGE(`dt`)
(PARTITION p202409 VALUES [("2024-09-01"), ("2024-10-01")),
PARTITION p202410 VALUES [("2024-10-01"), ("2024-11-01")),
PARTITION p202411 VALUES [("2024-11-01"), ("2024-12-01")),
PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01")),
PARTITION p202504 VALUES [("2025-04-01"), ("2025-05-01")))
DISTRIBUTED BY HASH(`dt`) BUCKETS 1 
PROPERTIES (
"compression" = "LZ4",
"dynamic_partition.buckets" = "6",
"dynamic_partition.enable" = "true",
"dynamic_partition.end" = "1",
"dynamic_partition.history_partition_num" = "0",
"dynamic_partition.prefix" = "p",
"dynamic_partition.start" = "-6",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);