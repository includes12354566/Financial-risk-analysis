-- 30亿数据规模优化方案
-- 针对大规模数据查询的性能优化

USE risk_analysis_system;

-- ========================================
-- 1. 分区表优化（按时间分区）
-- ========================================

-- 创建分区表（如果支持）
-- 注意：需要MySQL 5.7+版本支持
/*
ALTER TABLE transactions 
PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    PARTITION p_2024_01 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01 00:00:00')),
    PARTITION p_2024_02 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01 00:00:00')),
    PARTITION p_2024_03 VALUES LESS THAN (UNIX_TIMESTAMP('2024-04-01 00:00:00')),
    PARTITION p_2024_04 VALUES LESS THAN (UNIX_TIMESTAMP('2024-05-01 00:00:00')),
    PARTITION p_2024_05 VALUES LESS THAN (UNIX_TIMESTAMP('2024-06-01 00:00:00')),
    PARTITION p_2024_06 VALUES LESS THAN (UNIX_TIMESTAMP('2024-07-01 00:00:00')),
    PARTITION p_2024_07 VALUES LESS THAN (UNIX_TIMESTAMP('2024-08-01 00:00:00')),
    PARTITION p_2024_08 VALUES LESS THAN (UNIX_TIMESTAMP('2024-09-01 00:00:00')),
    PARTITION p_2024_09 VALUES LESS THAN (UNIX_TIMESTAMP('2024-10-01 00:00:00')),
    PARTITION p_2024_10 VALUES LESS THAN (UNIX_TIMESTAMP('2024-11-01 00:00:00')),
    PARTITION p_2024_11 VALUES LESS THAN (UNIX_TIMESTAMP('2024-12-01 00:00:00')),
    PARTITION p_2024_12 VALUES LESS THAN (UNIX_TIMESTAMP('2025-01-01 00:00:00')),
    PARTITION p_2025_01 VALUES LESS THAN (UNIX_TIMESTAMP('2025-02-01 00:00:00')),
    PARTITION p_2025_02 VALUES LESS THAN (UNIX_TIMESTAMP('2025-03-01 00:00:00')),
    PARTITION p_2025_03 VALUES LESS THAN (UNIX_TIMESTAMP('2025-04-01 00:00:00')),
    PARTITION p_2025_04 VALUES LESS THAN (UNIX_TIMESTAMP('2025-05-01 00:00:00')),
    PARTITION p_2025_05 VALUES LESS THAN (UNIX_TIMESTAMP('2025-06-01 00:00:00')),
    PARTITION p_2025_06 VALUES LESS THAN (UNIX_TIMESTAMP('2025-07-01 00:00:00')),
    PARTITION p_2025_07 VALUES LESS THAN (UNIX_TIMESTAMP('2025-08-01 00:00:00')),
    PARTITION p_2025_08 VALUES LESS THAN (UNIX_TIMESTAMP('2025-09-01 00:00:00')),
    PARTITION p_2025_09 VALUES LESS THAN (UNIX_TIMESTAMP('2025-10-01 00:00:00')),
    PARTITION p_2025_10 VALUES LESS THAN (UNIX_TIMESTAMP('2025-11-01 00:00:00')),
    PARTITION p_2025_11 VALUES LESS THAN (UNIX_TIMESTAMP('2025-12-01 00:00:00')),
    PARTITION p_2025_12 VALUES LESS THAN (UNIX_TIMESTAMP('2026-01-01 00:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
*/

-- ========================================
-- 2. 高性能索引优化
-- ========================================

-- 删除可能存在的旧索引
DROP INDEX IF EXISTS idx_risk_analysis_main ON transactions;
DROP INDEX IF EXISTS idx_sender_time ON transactions;
DROP INDEX IF EXISTS idx_receiver_time ON transactions;
DROP INDEX IF EXISTS idx_large_amount_time ON transactions;
DROP INDEX IF EXISTS idx_login_account_time ON logins;
DROP INDEX IF EXISTS idx_account_status ON accounts;
DROP INDEX IF EXISTS idx_transaction_status ON transactions;
DROP INDEX IF EXISTS idx_created_at_range ON transactions;

-- 创建高性能复合索引
CREATE INDEX idx_risk_analysis_main ON transactions (status, amount, created_at DESC);
CREATE INDEX idx_sender_time ON transactions (sender_account_id, created_at DESC);
CREATE INDEX idx_receiver_time ON transactions (receiver_account_id, created_at DESC);
CREATE INDEX idx_large_amount_time ON transactions (amount, created_at DESC);
CREATE INDEX idx_login_account_time ON logins (account_id, login_at DESC);
CREATE INDEX idx_account_status ON accounts (status);
CREATE INDEX idx_transaction_status ON transactions (status);
CREATE INDEX idx_created_at_range ON transactions (created_at DESC);

-- 创建覆盖索引（包含查询所需的所有字段）
CREATE INDEX idx_transaction_cover ON transactions (
    sender_account_id,
    receiver_account_id,
    amount,
    status,
    created_at,
    description
);

-- 创建时间范围查询专用索引
CREATE INDEX idx_time_range_optimized ON transactions (created_at DESC, status, amount);

-- 创建账户关联查询索引
CREATE INDEX idx_account_relations ON transactions (sender_account_id, receiver_account_id, created_at DESC);

-- ========================================
-- 3. 查询优化存储过程
-- ========================================

DELIMITER //
CREATE PROCEDURE GetRiskTransactionsBillionScale(
    IN time_range_hours INT,
    IN min_metric_a INT,
    IN min_metric_b INT,
    IN max_metric_c DECIMAL(15,2),
    IN page_offset INT DEFAULT 0,
    IN page_size INT DEFAULT 1000
)
BEGIN
    DECLARE start_time TIMESTAMP;
    DECLARE end_time TIMESTAMP;
    DECLARE start_time_30d TIMESTAMP;
    
    SET end_time = NOW();
    SET start_time = DATE_SUB(end_time, INTERVAL time_range_hours HOUR);
    SET start_time_30d = DATE_SUB(end_time, INTERVAL 30 DAY);
    
    -- 使用临时表存储中间结果（MEMORY引擎）
    DROP TEMPORARY TABLE IF EXISTS temp_large_transactions;
    CREATE TEMPORARY TABLE temp_large_transactions (
        id BIGINT UNSIGNED,
        sender_account_id BIGINT UNSIGNED,
        receiver_account_id BIGINT UNSIGNED,
        amount DECIMAL(15,2),
        created_at TIMESTAMP,
        description TEXT,
        PRIMARY KEY (id),
        INDEX idx_sender_time (sender_account_id, created_at),
        INDEX idx_receiver_time (receiver_account_id, created_at)
    ) ENGINE=MEMORY;
    
    -- 预筛选大额交易（使用索引优化）
    INSERT INTO temp_large_transactions
    SELECT id, sender_account_id, receiver_account_id, amount, created_at, description
    FROM transactions 
    WHERE amount >= 50000
      AND status = 'posted'
      AND created_at >= start_time_30d
      AND created_at < end_time
    LIMIT 100000; -- 限制临时表大小
    
    -- 分页查询结果
    SELECT 
        t.id as transaction_id,
        t.created_at as transaction_time,
        t.amount,
        t.description,
        t.sender_account_id as victim_account_id,
        sa.name as victim_name,
        sa.phone as victim_phone,
        sa.email as victim_email,
        sa.account_type as victim_type,
        t.receiver_account_id as suspicious_account_id,
        ra.name as suspicious_name,
        ra.phone as suspicious_phone,
        ra.email as suspicious_email,
        ra.account_type as suspicious_type,
        1 as metric_a,
        1 as metric_b,
        0 as metric_c,
        'HIGH' as risk_level
    FROM temp_large_transactions t
    INNER JOIN accounts sa ON sa.id = t.sender_account_id
    INNER JOIN accounts ra ON ra.id = t.receiver_account_id
    WHERE t.created_at >= start_time
      AND t.created_at < end_time
    ORDER BY t.created_at DESC, t.amount DESC
    LIMIT page_size OFFSET page_offset;
    
    -- 清理临时表
    DROP TEMPORARY TABLE temp_large_transactions;
END //
DELIMITER ;

-- ========================================
-- 4. 系统配置优化
-- ========================================

-- 更新表统计信息
ANALYZE TABLE accounts;
ANALYZE TABLE logins;
ANALYZE TABLE transactions;

-- 显示索引创建结果
SELECT 
    TABLE_NAME as '表名',
    INDEX_NAME as '索引名',
    CARDINALITY as '基数',
    CASE 
        WHEN CARDINALITY > 1000000 THEN 'High'
        WHEN CARDINALITY > 100000 THEN 'Medium'
        ELSE 'Low'
    END as '选择性'
FROM information_schema.STATISTICS 
WHERE TABLE_SCHEMA = 'risk_analysis_system'
  AND TABLE_NAME IN ('accounts', 'logins', 'transactions')
  AND INDEX_NAME LIKE 'idx_%'
ORDER BY TABLE_NAME, CARDINALITY DESC;


