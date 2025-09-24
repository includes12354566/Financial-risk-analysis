-- 金融风险分析系统数据库初始化脚本
-- 创建数据库
CREATE DATABASE IF NOT EXISTS risk_analysis_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE risk_analysis_system;

-- 账户表
CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    account_type VARCHAR(50) DEFAULT 'personal',
    phone VARCHAR(20),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    INDEX idx_account_type (account_type),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- 登录记录表
CREATE TABLE IF NOT EXISTS logins (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    account_id BIGINT NOT NULL,
    login_at TIMESTAMP NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    login_type VARCHAR(20) DEFAULT 'web',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    INDEX idx_account_login (account_id, login_at),
    INDEX idx_login_time (login_at),
    INDEX idx_account_id (account_id)
);

-- 交易记录表
CREATE TABLE IF NOT EXISTS transactions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    sender_account_id BIGINT NOT NULL,
    receiver_account_id BIGINT NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    transaction_type VARCHAR(50) DEFAULT 'transfer',
    status VARCHAR(20) DEFAULT 'posted',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT,
    reference_id VARCHAR(100),
    FOREIGN KEY (sender_account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    FOREIGN KEY (receiver_account_id) REFERENCES accounts(id) ON DELETE CASCADE,
    INDEX idx_sender (sender_account_id),
    INDEX idx_receiver (receiver_account_id),
    INDEX idx_amount (amount),
    INDEX idx_created_at (created_at),
    INDEX idx_status (status),
    INDEX idx_sender_time (sender_account_id, created_at),
    INDEX idx_receiver_time (receiver_account_id, created_at),
    INDEX idx_amount_time (amount, created_at)
);

-- 创建存储过程：查询风险交易
DELIMITER //
CREATE PROCEDURE GetRiskTransactions(
    IN time_range_hours INT,
    IN min_metric_a INT,
    IN min_metric_b INT,
    IN max_metric_c DECIMAL(15,2)
)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE end_time TIMESTAMP;
    
    SET end_time = NOW();
    SET start_time = DATE_SUB(end_time, INTERVAL time_range_hours HOUR);
    
    -- 指标A：账户在指定时间范围内在一笔大额（>=5万）以后2分钟内发起大额转出的次数
    WITH metric_a_data AS (
        SELECT 
            t_out.sender_account_id,
            COUNT(DISTINCT t_out.id) as metric_a_count
        FROM transactions t_out
        JOIN transactions t_in ON t_in.receiver_account_id = t_out.sender_account_id
        WHERE t_out.amount >= 50000
          AND t_out.status = 'posted'
          AND t_out.created_at >= start_time
          AND t_out.created_at < end_time
          AND t_in.amount >= 50000
          AND t_in.status = 'posted'
          AND t_in.created_at >= start_time
          AND t_in.created_at < end_time
          AND t_out.created_at >= t_in.created_at
          AND t_out.created_at <= DATE_ADD(t_in.created_at, INTERVAL 2 MINUTE)
        GROUP BY t_out.sender_account_id
    ),
    
    -- 指标B：账户在指定时间范围内登录后5分钟内发起大额转出的次数
    metric_b_data AS (
        SELECT 
            t.sender_account_id,
            COUNT(DISTINCT t.id) as metric_b_count
        FROM transactions t
        JOIN logins l ON l.account_id = t.sender_account_id
        WHERE t.amount >= 50000
          AND t.status = 'posted'
          AND t.created_at >= start_time
          AND t.created_at < end_time
          AND l.login_at >= start_time
          AND l.login_at < end_time
          AND l.login_at <= t.created_at
          AND t.created_at <= DATE_ADD(l.login_at, INTERVAL 5 MINUTE)
        GROUP BY t.sender_account_id
    ),
    
    -- 指标C：收款方在指定时间范围内累计入账总金额
    metric_c_data AS (
        SELECT 
            t.receiver_account_id,
            COALESCE(SUM(t.amount), 0) as metric_c_sum
        FROM transactions t
        WHERE t.status = 'posted'
          AND t.created_at >= start_time
          AND t.created_at < end_time
        GROUP BY t.receiver_account_id
    )
    
    SELECT DISTINCT
        t.id as transaction_id,
        t.created_at as transaction_time,
        t.amount,
        t.description,
        
        -- 转出账户信息（可能受害者）
        t.sender_account_id as victim_account_id,
        sa.name as victim_name,
        sa.phone as victim_phone,
        sa.email as victim_email,
        sa.account_type as victim_type,
        
        -- 收款账户信息（可疑账户）
        t.receiver_account_id as suspicious_account_id,
        ra.name as suspicious_name,
        ra.phone as suspicious_phone,
        ra.email as suspicious_email,
        ra.account_type as suspicious_type,
        
        -- 风险指标
        COALESCE(ma.metric_a_count, 0) as metric_a,
        COALESCE(mb.metric_b_count, 0) as metric_b,
        COALESCE(mc.metric_c_sum, 0) as metric_c,
        
        -- 风险等级评估
        CASE 
            WHEN COALESCE(ma.metric_a_count, 0) > 0 AND COALESCE(mb.metric_b_count, 0) > 0 AND COALESCE(mc.metric_c_sum, 0) = 0 THEN 'HIGH'
            WHEN COALESCE(ma.metric_a_count, 0) > 0 OR COALESCE(mb.metric_b_count, 0) > 0 THEN 'MEDIUM'
            ELSE 'LOW'
        END as risk_level
        
    FROM transactions t
    JOIN accounts sa ON sa.id = t.sender_account_id
    JOIN accounts ra ON ra.id = t.receiver_account_id
    LEFT JOIN metric_a_data ma ON ma.sender_account_id = t.sender_account_id
    LEFT JOIN metric_b_data mb ON mb.sender_account_id = t.sender_account_id
    LEFT JOIN metric_c_data mc ON mc.receiver_account_id = t.receiver_account_id
    WHERE t.amount >= 50000
      AND t.status = 'posted'
      AND t.created_at >= start_time
      AND t.created_at < end_time
      AND COALESCE(ma.metric_a_count, 0) >= min_metric_a
      AND COALESCE(mb.metric_b_count, 0) >= min_metric_b
      AND COALESCE(mc.metric_c_sum, 0) <= max_metric_c
    ORDER BY t.created_at DESC, t.amount DESC;
END //
DELIMITER ;

-- 创建函数：获取时间范围的小时数
DELIMITER //
CREATE FUNCTION GetTimeRangeHours(time_range VARCHAR(10)) 
RETURNS INT
READS SQL DATA
DETERMINISTIC
BEGIN
    CASE time_range
        WHEN '24h' THEN RETURN 24;
        WHEN '3d' THEN RETURN 72;
        WHEN '7d' THEN RETURN 168;
        WHEN '30d' THEN RETURN 720;
        WHEN '6m' THEN RETURN 4320;
        WHEN '1y' THEN RETURN 8760;
        ELSE RETURN 24;
    END CASE;
END //
DELIMITER ;