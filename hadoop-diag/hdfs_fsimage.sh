source ~/.bash_profile
source /etc/profile

dt=`date -d "today 0 days ago" +"%Y%m%d"`

echo $dt
echo "*******fsimage解析开始******"
hdfs oiv -i /DATA00/fsimage_res/`ls -t /DATA00/fsimage_res/ | grep fsimage_0 | head -n 1` -o /DATA00/fsimage_res/fsimage_rest.csv -p Delimited -delimiter ','  

su hive <<EOF
echo "*******hive load数据********"
hive -e "load data local inpath '/DATA00/fsimage_res/fsimage_rest.csv' overwrite into table storage_analyze.ods_fsimage partition(partition_key='${dt}') ";
exit;
EOF


if [ $? -eq 0 ]
then
    echo "数据已load表" 
    rm -rf /DATA00/fsimage_res/fsimage_0* 	
else
    echo "fsimage解析报错，请及时查看" 
    exit 1;
fi
