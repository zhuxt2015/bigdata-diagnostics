CREATE TABLE `task_fulllink_scan_bigtable` (
  `application_id` varchar(65535) NOT NULL COMMENT "application id",
  `dt` date NOT NULL COMMENT "分区字段",
  `sql_sequence` int(11) NULL COMMENT "sql 序号",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(65533) NULL COMMENT "任务状态",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `read_records` bigint(20) NULL COMMENT "读取数据条数",
  `read_bytes` bigint(20) NULL COMMENT "读取数据字节数"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`)
COMMENT "存在大表扫描问题的任务"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01")),
PARTITION p202504 VALUES [("2025-04-01"), ("2025-05-01")))
DISTRIBUTED BY HASH(`dt`) BUCKETS 3 
PROPERTIES (
"compression" = "LZ4",
"dynamic_partition.buckets" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.end" = "1",
"dynamic_partition.history_partition_num" = "0",
"dynamic_partition.prefix" = "p",
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_application_container_cost` (
  `application_id` varchar(65535) NOT NULL COMMENT "applicationid",
  `dt` date NOT NULL COMMENT "分区字段",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `process_instance_name` varchar(65535) NULL COMMENT "工作流实例名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(1) NULL COMMENT "任务状态",
  `yarn_user` varchar(65535) NULL COMMENT "yarn任务用户",
  `yarn_queue` varchar(65535) NULL COMMENT "yarn任务执行队列",
  `finalStatus` varchar(65535) NULL COMMENT "yarn任务执行最终状态",
  `memorySeconds` bigint(20) NULL COMMENT "内存消耗",
  `vcoreSeconds` bigint(20) NULL COMMENT "cpu消耗",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `task_avg_cost_times` decimal(38, 2) NULL COMMENT "任务近14次平均耗时",
  `task_growth_cost_times` decimal(38, 2) NULL COMMENT "任务耗时增长量",
  `task_cost_times_ratio` decimal(38, 2) NULL COMMENT "任务耗时增长率",
  `retry_times` bigint(20) NULL COMMENT "任务重试次数",
  `max_retry_times` bigint(20) NULL COMMENT "任务最大重试次数",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `am_share_change_time` datetime NULL COMMENT "am分配开始时间",
  `container_share_change_time` datetime NULL COMMENT "container分配开始时间",
  `am_share_cost` decimal(38, 3) NULL COMMENT "am分配耗时",
  `container_share_cost` decimal(38, 3) NULL COMMENT "所有容器分配耗时",
  `am_localize_cost` decimal(38, 3) NULL COMMENT "am本地化耗时",
  `container_localize_cost` decimal(38, 3) NULL COMMENT "所有容器本地化耗时",
  `container_running_cost` decimal(38, 3) NULL COMMENT "容器运行耗时",
  `container_num` bigint(20) NULL COMMENT "容器数量",
  `input_bytes_read` decimal(38, 3) NULL COMMENT "input输入量"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`)
COMMENT "全链路任务诊断yarn各阶段耗时明细表"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
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
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_optimizable_task_info` (
  `application_id` varchar(65535) NOT NULL COMMENT "applicationid",
  `dt` date NOT NULL COMMENT "分区字段",
  `optimizable_task_type` varchar(65533) NOT NULL COMMENT "可优化任务类型",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(65533) NULL COMMENT "任务状态",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `yarn_user` varchar(65533) NULL COMMENT "yarn执行用户",
  `yarn_queue` varchar(65533) NULL COMMENT "yarn执行队列",
  `stage_cost_times` decimal(38, 3) NULL COMMENT "阶段耗时",
  `avg_stage_cost_times` decimal(38, 3) NULL COMMENT "平均阶段耗时",
  `stage_cost_times_ratio` decimal(38, 2) NULL COMMENT "阶段耗时增长比例",
  `error_info` varchar(65533) NULL COMMENT "异常描述",
  `yarn_resource_memory_ratio` decimal(38, 2) NULL COMMENT "yarn内存资源使用率",
  `yarn_resource_vcores_ratio` decimal(38, 2) NULL COMMENT "yarn vcores资源使用率"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`, `optimizable_task_type`)
COMMENT "全链路任务诊断可优化任务明细表"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
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
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_error_task_analysis` (
  `application_id` varchar(65535) NOT NULL COMMENT "applicationid",
  `dt` date NOT NULL COMMENT "分区字段",
  `check_type` varchar(65533) NOT NULL COMMENT "检查项",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(65533) NULL COMMENT "任务状态",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `yarn_user` varchar(65533) NULL COMMENT "yarn执行用户",
  `yarn_queue` varchar(65533) NULL COMMENT "yarn执行队列",
  `data_value` decimal(38, 6) NULL COMMENT "当前值",
  `avg_value` decimal(38, 6) NULL COMMENT "对比值",
  `growth_value` decimal(38, 6) NULL COMMENT "增长值",
  `growth_ratio` decimal(38, 6) NULL COMMENT "增长比例",
  `question_type` varchar(65535) NULL COMMENT "问题类型",
  `check_status` boolean NULL COMMENT "检查状态",
  `check_keyword` varchar(65535) NULL COMMENT "异常类型",
  `question` varchar(65533) NULL COMMENT "问题",
  `quota_details` varchar(65533) NULL COMMENT "指标详情"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`, `check_type`)
COMMENT "全链路任务诊断任务诊断明细表"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
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
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_error_task_info` (
  `application_id` varchar(65535) NOT NULL COMMENT "applicationid",
  `dt` date NOT NULL COMMENT "分区字段",
  `error_type` varchar(65533) NOT NULL COMMENT "任务异常类型",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(65533) NULL COMMENT "任务状态",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `yarn_user` varchar(65533) NULL COMMENT "yarn执行用户",
  `yarn_queue` varchar(65533) NULL COMMENT "yarn执行队列",
  `data_value` decimal(38, 3) NULL COMMENT "当前值",
  `avg_value` decimal(38, 3) NULL COMMENT "最近14次（去掉最大最小值）的平均值",
  `growth_value` decimal(38, 3) NULL COMMENT "增长量(对比最近14次平均值)",
  `growth_ratio` decimal(38, 2) NULL COMMENT "增长率(对比最近14次平均值)",
  `error_info` varchar(65533) NULL COMMENT "异常描述"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`, `error_type`)
COMMENT "全链路任务诊断异常明细表"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
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
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_scan_bigtable` (
  `application_id` varchar(65535) NOT NULL COMMENT "application id",
  `dt` date NOT NULL COMMENT "分区字段",
  `sql_sequence` int(11) NULL COMMENT "sql 序号",
  `project_name` varchar(65535) NULL COMMENT "项目名称",
  `process_name` varchar(65535) NULL COMMENT "工作流名称",
  `task_name` varchar(65533) NULL COMMENT "任务名称",
  `task_state` varchar(65533) NULL COMMENT "任务状态",
  `task_start_time` datetime NULL COMMENT "任务开始时间",
  `task_end_time` datetime NULL COMMENT "任务结束时间",
  `task_cost_times` decimal(38, 2) NULL COMMENT "任务耗时",
  `read_records` bigint(20) NULL COMMENT "读取数据条数",
  `read_bytes` bigint(20) NULL COMMENT "读取数据字节数"
) ENGINE=OLAP 
PRIMARY KEY(`application_id`, `dt`)
COMMENT "存在大表扫描问题的任务"
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01")),
PARTITION p202504 VALUES [("2025-04-01"), ("2025-05-01")))
DISTRIBUTED BY HASH(`dt`) BUCKETS 3 
PROPERTIES (
"compression" = "LZ4",
"dynamic_partition.buckets" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.end" = "1",
"dynamic_partition.history_partition_num" = "0",
"dynamic_partition.prefix" = "p",
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

CREATE TABLE `task_fulllink_yarn_queue_resources` (
  `dt` date NOT NULL COMMENT "分区字段",
  `timestamp` datetime NOT NULL COMMENT "时间",
  `queue_name` varchar(65533) NOT NULL COMMENT "队列名称",
  `min_resources_memory` bigint(20) NULL COMMENT "最小可用内存资源",
  `min_resources_vcores` bigint(20) NULL COMMENT "最小可用vcores资源",
  `max_resources_memory` bigint(20) NULL COMMENT "最大可用内存资源",
  `max_resources_vcores` bigint(20) NULL COMMENT "最大可用vcores资源",
  `used_resources_memory` bigint(20) NULL COMMENT "正在使用内存资源",
  `used_resources_vcores` bigint(20) NULL COMMENT "正在使用vcores资源",
  `am_used_resources_memory` bigint(20) NULL COMMENT "AM使用内存资源",
  `am_used_resources_vcores` bigint(20) NULL COMMENT "AM使用vcores资源",
  `am_max_resources_memory` bigint(20) NULL COMMENT "AM最大内存资源",
  `am_max_resources_vcores` bigint(20) NULL COMMENT "AM最大vcores资源",
  `demand_resources_memory` bigint(20) NULL COMMENT "正在等待内存资源",
  `demand_resources_vcores` bigint(20) NULL COMMENT "正在等待vcores资源",
  `steady_fair_resources_memory` bigint(20) NULL COMMENT "长期调度可用内存资源",
  `steady_fair_resources_vcores` bigint(20) NULL COMMENT "长期调度可用vcores资源",
  `fair_resources_memory` bigint(20) NULL COMMENT "短期调度可用内存资源",
  `fair_resources_vcores` bigint(20) NULL COMMENT "短期调度可用vcores资源",
  `cluster_resources_memory` bigint(20) NULL COMMENT "集群内存资源",
  `cluster_resources_vcores` bigint(20) NULL COMMENT "集群vcores资源",
  `reserved_resources_memory` bigint(20) NULL COMMENT "被保留的内存资源",
  `reserved_resources_vcores` bigint(20) NULL COMMENT "被保留的vcores资源",
  `max_container_allocation_memory` bigint(20) NULL COMMENT "每个容器最大内存资源",
  `max_container_allocation_vcores` bigint(20) NULL COMMENT "每个容器最大vcores资源",
  `num_pending_apps` bigint(20) NULL COMMENT "",
  `num_active_apps` bigint(20) NULL COMMENT "",
  `preemptable` boolean NULL COMMENT "",
  `insert_time` datetime NULL COMMENT "数据插入时间"
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `timestamp`, `queue_name`)
PARTITION BY RANGE(`dt`)
(PARTITION p202412 VALUES [("2024-12-01"), ("2025-01-01")),
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
"dynamic_partition.start" = "-3",
"dynamic_partition.start_day_of_month" = "1",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"replication_num" = "3"
);

