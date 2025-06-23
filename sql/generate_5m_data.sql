-- 生成 500 万测试数据的 SQL 脚本
-- 注意：此脚本适用于小规模测试，对于 500 万数据建议使用 Python 脚本
-- 推荐使用: python3 scripts/generate_5m_data.py

USE datax_source;

-- 方法1: 使用递归CTE生成数据（MySQL 8.0+）
-- 清空表
TRUNCATE TABLE users;

-- 生成 500 万条数据（分批执行以避免内存问题）
-- 注意：这个方法可能会很慢，建议使用 Python 脚本

-- 创建一个临时的数字生成表
DROP TABLE IF EXISTS temp_numbers;
CREATE TEMPORARY TABLE temp_numbers (num INT PRIMARY KEY);

-- 插入数字序列（这里先生成到100万，然后通过笛卡尔积扩展）
INSERT INTO temp_numbers
SELECT @row := @row + 1 AS num
FROM
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t4,
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t5,
    (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL
     SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t6,
    (SELECT @row := 0) r
LIMIT 1000000;

-- 分批插入数据（每次100万条，共5批）
SELECT '开始插入第1批数据 (1-1000000)...' AS info;
INSERT INTO users (username, email, password, full_name, phone, status)
SELECT
    CONCAT('user_', LPAD(num, 10, '0')) AS username,
    CONCAT('user_', LPAD(num, 10, '0'), '@example.com') AS email,
    CONCAT('password_', MD5(RAND())) AS password,
    CONCAT('User ', LPAD(num, 10, '0')) AS full_name,
    CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0')) AS phone,
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END AS status
FROM temp_numbers
WHERE num <= 1000000;

SELECT '开始插入第2批数据 (1000001-2000000)...' AS info;
INSERT INTO users (username, email, password, full_name, phone, status)
SELECT
    CONCAT('user_', LPAD(num + 1000000, 10, '0')) AS username,
    CONCAT('user_', LPAD(num + 1000000, 10, '0'), '@example.com') AS email,
    CONCAT('password_', MD5(RAND())) AS password,
    CONCAT('User ', LPAD(num + 1000000, 10, '0')) AS full_name,
    CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0')) AS phone,
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END AS status
FROM temp_numbers
WHERE num <= 1000000;

SELECT '开始插入第3批数据 (2000001-3000000)...' AS info;
INSERT INTO users (username, email, password, full_name, phone, status)
SELECT
    CONCAT('user_', LPAD(num + 2000000, 10, '0')) AS username,
    CONCAT('user_', LPAD(num + 2000000, 10, '0'), '@example.com') AS email,
    CONCAT('password_', MD5(RAND())) AS password,
    CONCAT('User ', LPAD(num + 2000000, 10, '0')) AS full_name,
    CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0')) AS phone,
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END AS status
FROM temp_numbers
WHERE num <= 1000000;

SELECT '开始插入第4批数据 (3000001-4000000)...' AS info;
INSERT INTO users (username, email, password, full_name, phone, status)
SELECT
    CONCAT('user_', LPAD(num + 3000000, 10, '0')) AS username,
    CONCAT('user_', LPAD(num + 3000000, 10, '0'), '@example.com') AS email,
    CONCAT('password_', MD5(RAND())) AS password,
    CONCAT('User ', LPAD(num + 3000000, 10, '0')) AS full_name,
    CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0')) AS phone,
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END AS status
FROM temp_numbers
WHERE num <= 1000000;

SELECT '开始插入第5批数据 (4000001-5000000)...' AS info;
INSERT INTO users (username, email, password, full_name, phone, status)
SELECT
    CONCAT('user_', LPAD(num + 4000000, 10, '0')) AS username,
    CONCAT('user_', LPAD(num + 4000000, 10, '0'), '@example.com') AS email,
    CONCAT('password_', MD5(RAND())) AS password,
    CONCAT('User ', LPAD(num + 4000000, 10, '0')) AS full_name,
    CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0')) AS phone,
    CASE WHEN RAND() > 0.1 THEN 1 ELSE 0 END AS status
FROM temp_numbers
WHERE num <= 1000000;

-- 清理临时表
DROP TEMPORARY TABLE temp_numbers;

-- 显示结果
SELECT
    COUNT(*) AS total_records,
    COUNT(CASE WHEN status = 1 THEN 1 END) AS active_users,
    COUNT(CASE WHEN status = 0 THEN 1 END) AS inactive_users,
    MIN(created_at) AS earliest_record,
    MAX(created_at) AS latest_record
FROM users;

SELECT '数据生成完成！建议使用 Python 脚本获得更好的性能。' AS result;
    
    -- 最终提交
    COMMIT;
    
    -- 恢复自动提交
    SET autocommit = 1;
    
    -- 显示结果
    SELECT COUNT(*) AS total_inserted_records FROM users;
    SELECT '数据生成完成！' AS result;
    
END$$

DELIMITER ;

-- 执行存储过程
CALL GenerateTestData();

-- 显示一些统计信息
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN status = 1 THEN 1 END) AS active_users,
    COUNT(CASE WHEN status = 0 THEN 1 END) AS inactive_users,
    MIN(created_at) AS earliest_record,
    MAX(created_at) AS latest_record
FROM users;

-- 显示前 5 条记录作为示例
SELECT * FROM users ORDER BY id LIMIT 5;

-- 清理存储过程
DROP PROCEDURE GenerateTestData;
