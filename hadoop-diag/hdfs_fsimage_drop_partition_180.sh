#!/bin/bash
source ~/.bash_profile
echo "运行时间:"`date "+%Y-%m-%d %H:%M:%S"` "脚本执行开始********************************************"
before_day=`date -d "today 60 days ago" +"%Y%m%d"`

echo "storage_analyze.ods_fsimage start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.ods_fsimage DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.ods_fsimage table is finish"
echo "****************************************************************************"



echo "storage_analyze.dwd_fsimage start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.dwd_fsimage DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.dwd_fsimage table is finish"
echo "****************************************************************************"



echo "storage_analyze.hdfs_storage_user start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.hdfs_storage_user DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.hdfs_storage_user table is finish"
echo "****************************************************************************"



echo "storage_analyze.hive_storage_dir start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.hive_storage_dir DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.hive_storage_dir table is finish"
echo "****************************************************************************"




echo "storage_analyze.hdfs_storage start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.hdfs_storage DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.hdfs_storage table is finish"
echo "****************************************************************************"




echo "storage_analyze.hdfs_dir_fir start to drop partitions"
sudo -u hive hive -e "ALTER TABLE storage_analyze.hdfs_dir_fir DROP IF EXISTS PARTITION (partition_key < '${before_day}');"
echo "storage_analyze.hdfs_dir_fir table is finish"



echo "运行时间:"`date "+%Y-%m-%d %H:%M:%S"` "脚本执行结束********************************************"
