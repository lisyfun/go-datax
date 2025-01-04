-- 创建源数据库
DROP DATABASE IF EXISTS source_db;
CREATE DATABASE source_db;

-- 创建目标数据库
DROP DATABASE IF EXISTS target_db;
CREATE DATABASE target_db;

-- 连接到源数据库创建表
\c source_db;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建一些测试数据
INSERT INTO users (username, email, age)
SELECT
    'user' || i AS username,
    'user' || i || '@example.com' AS email,
    (random() * 50 + 18)::integer AS age
FROM generate_series(1, 500000) i;

-- 连接到目标数据库创建表
\c target_db;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
