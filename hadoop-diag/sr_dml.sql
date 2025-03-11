--超过5年的表的还在被读写
SELECT 
    path,
    username,
    groupname,
    filesize,
    accesstime,
    modificationtime,
    permission,
    split(split(p5, '/')[5], '.')[1] AS db,
    split(p5, '/')[6] AS tbl
FROM 
    dwd_fsimage
WHERE 
    partition_key = DATE_SUB(CURDATE(), INTERVAL 1 day)
    and 
    -- 限制只查hive的表
    p3 = '/user/hive/warehouse'
    -- 排除accesstime为1970-01-01 08:00的记录
    AND accesstime != '1970-01-01 08:00:00' 
    AND
    (
        -- 文件的最后访问时间超过5年
        (
            accesstime IS NOT NULL 
            AND accesstime != '' 
            AND UNIX_TIMESTAMP(accesstime) < UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR))
        )
        OR
        -- 文件的最后修改时间超过5年
        (
            modificationtime IS NOT NULL 
            AND modificationtime != '' 
            AND UNIX_TIMESTAMP(modificationtime) < UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR))
        )
    )
    -- 确保是文件而不是目录
    AND filesize > 0
ORDER BY 
    db, tbl, accesstime ASC;