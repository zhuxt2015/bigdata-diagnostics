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

select
	path,
    username,
    groupname,
    filesize,
    accesstime,
    modificationtime,
	REGEXP_EXTRACT(p6,
	'/([^/]+)=([1|2].*)',
	2) AS partition_key,
	-- 转换分区值为统一的日期格式（年-月-日）
    CASE 
        -- 处理6位数字格式 (202406) - 正确转为年月日格式 2024-06-01
        WHEN REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1) IS NOT NULL 
            AND LENGTH(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1)) = 6
        THEN CONCAT(
            SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1), 1, 4), '-',
            SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1), 5, 2), '-01'
        )
        -- 处理7位带连字符的格式 (2025-01) - 转为年月日格式，日默认为01
        WHEN REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2})$', 1) IS NOT NULL
        and LENGTH(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2})$', 1)) = 7
        THEN CONCAT(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2})$', 1), '-01')
        -- 处理8位数字格式 (20250101)
        WHEN REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1) IS NOT NULL 
        and LENGTH(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1)) = 8
        THEN CONCAT(
            SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 1, 4), '-',
            SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 5, 2), '-',
            SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 7, 2)
        )
        -- 处理10位带连字符的日期格式 (2025-01-01)
        WHEN REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2}-[0-9]{2})$', 1) IS NOT NULL
        and LENGTH(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2}-[0-9]{2})$', 1)) = 10
        THEN REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2}-[0-9]{2})$', 1)
        ELSE NULL
    END AS partition_date
from
	dwd_fsimage df
where
	partition_key = DATE_SUB(CURDATE(), INTERVAL 1 day)
	and p3 = '/user/hive/warehouse'
	and accesstime != '1970-01-01 08:00:00'
	and filesize > 0
    AND REGEXP_EXTRACT(p6,
	'/([^/]+)=(2.*)',
	2) != ''
   -- 访问时间或修改时间是最近7天
    AND (
        (
            accesstime IS NOT NULL 
            AND accesstime != '' 
            AND UNIX_TIMESTAMP(accesstime) > UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
        )
        OR
        (
            modificationtime IS NOT NULL 
            AND modificationtime != '' 
            AND UNIX_TIMESTAMP(modificationtime) > UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
        )
    )
    -- 文件所属分区是5年前的分区（支持四种格式）
    AND (
        -- 匹配6位数字格式的年月 (202501)
        (
            p6 REGEXP '/[^/]+=((?:1|2)[0-9]{5})$'
            AND CONCAT(
                SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1), 1, 4), '-',
                SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{5})$', 1), 5, 2), '-01'
            ) < DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        )
        OR
        -- 匹配带连字符的年月格式 (2025-01)
        (
            p6 REGEXP '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2})$'
            AND CONCAT(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2})$', 1), '-01') 
            < DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        )
        OR
        -- 匹配8位数字格式的日期 (20250101)
        (
            p6 REGEXP '/[^/]+=((?:1|2)[0-9]{7})$'
            AND CONCAT(
                SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 1, 4), '-',
                SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 5, 2), '-',
                SUBSTRING(REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{7})$', 1), 7, 2)
            ) < DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        )
        OR
        -- 匹配带连字符的日期格式 (2025-01-01)
        (
            p6 REGEXP '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2}-[0-9]{2})$'
            AND REGEXP_EXTRACT(p6, '/[^/]+=((?:1|2)[0-9]{3}-[0-9]{2}-[0-9]{2})$', 1)
            < DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
        )
    )
    ;