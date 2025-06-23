-- DataX 测试数据库和表结构创建脚本
-- 基于 config/job/mysql2mysql.json 配置文件

-- 创建源数据库
CREATE DATABASE IF NOT EXISTS `datax_source` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建目标数据库  
CREATE DATABASE IF NOT EXISTS `datax_target` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 使用源数据库
USE `datax_source`;

-- 创建用户表
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表';

-- 插入测试数据
INSERT INTO `users` (`username`, `email`, `password`, `full_name`, `phone`, `status`) VALUES
('admin', 'admin@example.com', 'hashed_password_1', '系统管理员', '13800138000', 1),
('john_doe', 'john.doe@example.com', 'hashed_password_2', 'John Doe', '13800138001', 1),
('jane_smith', 'jane.smith@example.com', 'hashed_password_3', 'Jane Smith', '13800138002', 1),
('bob_wilson', 'bob.wilson@example.com', 'hashed_password_4', 'Bob Wilson', '13800138003', 1),
('alice_brown', 'alice.brown@example.com', 'hashed_password_5', 'Alice Brown', '13800138004', 0),
('charlie_davis', 'charlie.davis@example.com', 'hashed_password_6', 'Charlie Davis', '13800138005', 1),
('diana_miller', 'diana.miller@example.com', 'hashed_password_7', 'Diana Miller', '13800138006', 1),
('edward_jones', 'edward.jones@example.com', 'hashed_password_8', 'Edward Jones', '13800138007', 0),
('fiona_garcia', 'fiona.garcia@example.com', 'hashed_password_9', 'Fiona Garcia', '13800138008', 1),
('george_martinez', 'george.martinez@example.com', 'hashed_password_10', 'George Martinez', '13800138009', 1);

-- 使用目标数据库
USE `datax_target`;

-- 创建相同结构的用户表（目标表）
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表（目标）';

-- 显示创建结果
SELECT 'datax_source database and users table created successfully' AS result;
USE `datax_source`;
SELECT COUNT(*) AS source_users_count FROM users;

USE `datax_target`;  
SELECT COUNT(*) AS target_users_count FROM users;
