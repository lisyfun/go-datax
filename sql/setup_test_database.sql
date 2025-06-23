-- DataX 测试数据库结构创建脚本
-- 基于 config/job/test_mysql2mysql.json 配置文件
-- 使用标准的 MySQL 3306 端口和 test 数据库

-- 创建或使用 test 数据库
CREATE DATABASE IF NOT EXISTS `test` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `test`;

-- 创建源表 users
CREATE TABLE IF NOT EXISTS `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '用户ID',
  `username` varchar(50) NOT NULL COMMENT '用户名',
  `email` varchar(100) NOT NULL COMMENT '邮箱',
  `password` varchar(255) NOT NULL COMMENT '密码',
  `full_name` varchar(100) DEFAULT NULL COMMENT '全名',
  `phone` varchar(20) DEFAULT NULL COMMENT '电话',
  `status` tinyint(1) DEFAULT 1 COMMENT '状态：1-活跃，0-禁用',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_username` (`username`),
  UNIQUE KEY `uk_email` (`email`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表（源）';

-- 创建目标表 users_backup
CREATE TABLE IF NOT EXISTS `users_backup` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '用户ID',
  `username` varchar(50) NOT NULL COMMENT '用户名',
  `email` varchar(100) NOT NULL COMMENT '邮箱',
  `password` varchar(255) NOT NULL COMMENT '密码',
  `full_name` varchar(100) DEFAULT NULL COMMENT '全名',
  `phone` varchar(20) DEFAULT NULL COMMENT '电话',
  `status` tinyint(1) DEFAULT 1 COMMENT '状态：1-活跃，0-禁用',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_username` (`username`),
  KEY `idx_email` (`email`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表（备份目标）';

-- 清空表并插入测试数据到源表
TRUNCATE TABLE `users`;
INSERT INTO `users` (`username`, `email`, `password`, `full_name`, `phone`, `status`) VALUES
('admin', 'admin@test.com', 'admin123', '系统管理员', '13800138000', 1),
('testuser1', 'test1@test.com', 'test123', '测试用户1', '13800138001', 1),
('testuser2', 'test2@test.com', 'test123', '测试用户2', '13800138002', 1),
('testuser3', 'test3@test.com', 'test123', '测试用户3', '13800138003', 0),
('testuser4', 'test4@test.com', 'test123', '测试用户4', '13800138004', 1);

-- 清空目标表
TRUNCATE TABLE `users_backup`;

-- 显示创建结果
SELECT 'Test database setup completed successfully' AS result;
SELECT COUNT(*) AS source_users_count FROM users;
SELECT COUNT(*) AS target_users_backup_count FROM users_backup;

-- 显示源表数据
SELECT 'Source table data:' AS info;
SELECT id, username, email, full_name, status, created_at FROM users ORDER BY id;
