-- =============================================================================
-- 共用库初始化：与 project-wizard-feishu-github 使用同一库、同一账号，两端均可读写 projects 表。
-- 表名：projects（首次在应用内保存时由程序自动创建；本脚本只负责「库 + 账号 + 权限」）
--
-- 使用方式（在能管理 MySQL 的账号下执行，例如 root）：
--   mysql -h 主机 -P 3306 -u root -p < mysql_setup_shared_projects.sql
-- 或在 mysql 客户端里 source 本文件。
--
-- 执行前请修改：库名、用户名、密码（两项目 Secrets / .env 必须一致）
-- =============================================================================

CREATE DATABASE IF NOT EXISTS elderly_projects
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'app203_shared'@'%' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';

GRANT ALL PRIVILEGES ON elderly_projects.* TO 'app203_shared'@'%';

FLUSH PRIVILEGES;

-- 两项目相同配置示例见同目录 env_example_app203.txt / .env.example
