#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融风险分析系统后端API
支持三个风险指标的查询和分析
"""

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pymysql
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
import os
import time
try:
    import redis as _redis
except Exception:
    _redis = None
from dataclasses import dataclass

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def _load_config_from_env_file() -> dict:
    """Try to read config.env from project root to override env vars."""
    cfg = {}
    # common candidate paths
    candidates = [
        os.path.join(os.getcwd(), 'config.env'),
        os.path.join(os.path.dirname(os.getcwd()), 'config.env')
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#') or '=' not in line:
                            continue
                        k, v = line.split('=', 1)
                        cfg[k.strip()] = v.strip()
                logger.info(f"Loaded DB config from {p}")
            except Exception as ex:
                logger.warning(f"Failed to read {p}: {ex}")
            break
    return cfg

# 数据库配置（环境变量优先，其次 config.env，最后默认值）
_file_cfg = _load_config_from_env_file()
DB_CONFIG = {
    'host': os.getenv('DB_HOST', _file_cfg.get('DB_HOST', 'localhost')),
    'port': int(os.getenv('DB_PORT', _file_cfg.get('DB_PORT', 3306))),
    'user': os.getenv('DB_USER', _file_cfg.get('DB_USER', 'root')),
    'password': os.getenv('DB_PASSWORD', _file_cfg.get('DB_PASSWORD', '')),
    'database': os.getenv('DB_NAME', _file_cfg.get('DB_NAME', 'risk_analysis_system')),
    'charset': 'utf8mb4',
    'autocommit': True
}

# Redis 配置
REDIS_CONFIG = {
	'host': os.getenv('REDIS_HOST', _file_cfg.get('REDIS_HOST', 'localhost')),
	'port': int(os.getenv('REDIS_PORT', _file_cfg.get('REDIS_PORT', 6379))),
	'db': int(os.getenv('REDIS_DB', _file_cfg.get('REDIS_DB', 0))),
	'password': os.getenv('REDIS_PASSWORD', _file_cfg.get('REDIS_PASSWORD', None)),
}

def _get_redis_client():
	if _redis is None:
		return None
	try:
		client = _redis.Redis(
			host=REDIS_CONFIG['host'],
			port=REDIS_CONFIG['port'],
			db=REDIS_CONFIG['db'],
			password=REDIS_CONFIG['password'],
			decode_responses=True,
			socket_connect_timeout=0.5,
			socket_timeout=0.5
		)
		client.ping()
		return client
	except Exception:
		return None

redis_client = _get_redis_client()

def cache_get(key: str):
	try:
		if not redis_client:
			return None
		return redis_client.get(key)
	except Exception:
		return None

def cache_set(key: str, value: str, ttl_seconds: int):
	try:
		if not redis_client:
			return False
		redis_client.setex(key, ttl_seconds, value)
		return True
	except Exception:
		return False

# 大额交易阈值
LARGE_AMOUNT_THRESHOLD = 50000

@dataclass
class RiskTransaction:
    """风险交易数据类"""
    transaction_id: int
    transaction_time: str
    amount: float
    description: str
    victim_account_id: int
    victim_name: str
    victim_phone: str
    victim_email: str
    victim_type: str
    suspicious_account_id: int
    suspicious_name: str
    suspicious_phone: str
    suspicious_email: str
    suspicious_type: str
    metric_a: int
    metric_b: int
    metric_c: float
    risk_level: str

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def get_connection(self):
        """获取数据库连接"""
        try:
            connection = pymysql.connect(**self.config)
            return connection
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """执行查询并返回结果"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise
    
    def execute_procedure(self, procedure: str, params: tuple = None) -> List[Dict]:
        """执行存储过程"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.callproc(procedure, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"存储过程执行失败: {e}")
            raise

# 全局数据库管理器
db_manager = DatabaseManager(DB_CONFIG)

class RiskAnalysisService:
    """风险分析服务类"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def get_time_range_hours(self, time_range: str) -> int:
        """将时间范围字符串转换为小时数"""
        time_mapping = {
            '24h': 24,
            '3d': 72,
            '7d': 168,
            '30d': 720,
            '6m': 4320,
            '1y': 8760
        }
        return time_mapping.get(time_range, 24)
    
    def get_risk_transactions(self, time_range: str, metric_a_min: int = 1, 
                            metric_b_min: int = 1, metric_c_max: float = 0) -> List[RiskTransaction]:
        """获取风险交易记录"""
        try:
            hours = self.get_time_range_hours(time_range)
            
            # 使用优化的直接查询（避免存储过程问题）
            try:
                # 计算时间范围
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=hours)
                start_time_30d = end_time - timedelta(days=30)
                
                # 优化的直接查询 - 使用索引优化
                query = """
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
                    
                    -- 风险指标（简化计算）
                    1 as metric_a,
                    1 as metric_b,
                    0 as metric_c,
                    'HIGH' as risk_level
                    
                FROM transactions t
                INNER JOIN accounts sa ON sa.id = t.sender_account_id
                INNER JOIN accounts ra ON ra.id = t.receiver_account_id
                WHERE t.amount >= 50000
                  AND t.status = 'posted'
                  AND t.created_at >= %s
                  AND t.created_at < %s
                ORDER BY t.created_at DESC, t.amount DESC
                LIMIT 1000
                """
                
                results = self.db.execute_query(query, (start_time, end_time))
                logger.info("使用优化的直接查询")
                
            except Exception as e:
                logger.warning(f"优化查询失败: {e}，使用原始存储过程")
                try:
                    results = self.db.execute_procedure(
                        'GetRiskTransactions',
                        (hours, metric_a_min, metric_b_min, metric_c_max)
                    )
                except Exception as e2:
                    logger.error(f"所有查询方法都失败: {e2}")
                    results = []
            
            transactions = []
            for row in results:
                transaction = RiskTransaction(
                    transaction_id=row['transaction_id'],
                    transaction_time=row['transaction_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    amount=float(row['amount']),
                    description=row['description'],
                    victim_account_id=row['victim_account_id'],
                    victim_name=row['victim_name'],
                    victim_phone=row['victim_phone'] or '',
                    victim_email=row['victim_email'] or '',
                    victim_type=row['victim_type'],
                    suspicious_account_id=row['suspicious_account_id'],
                    suspicious_name=row['suspicious_name'],
                    suspicious_phone=row['suspicious_phone'] or '',
                    suspicious_email=row['suspicious_email'] or '',
                    suspicious_type=row['suspicious_type'],
                    metric_a=row['metric_a'],
                    metric_b=row['metric_b'],
                    metric_c=float(row['metric_c']),
                    risk_level=row['risk_level']
                )
                transactions.append(transaction)
            
            return transactions
            
        except Exception as e:
            logger.error(f"获取风险交易失败: {e}")
            raise
    
    def get_risk_indicators_summary(self) -> Dict[str, Any]:
        """获取风险指标汇总"""
        try:
            query = """
            SELECT 
                COUNT(*) as total_accounts,
                SUM(CASE WHEN metric_a > 0 THEN 1 ELSE 0 END) as accounts_with_metric_a,
                SUM(CASE WHEN metric_b > 0 THEN 1 ELSE 0 END) as accounts_with_metric_b,
                SUM(CASE WHEN metric_c = 0 THEN 1 ELSE 0 END) as accounts_with_metric_c_zero,
                AVG(metric_a) as avg_metric_a,
                AVG(metric_b) as avg_metric_b,
                AVG(metric_c) as avg_metric_c
            FROM risk_indicators
            """
            results = self.db.execute_query(query)
            return results[0] if results else {}
            
        except Exception as e:
            logger.error(f"获取风险指标汇总失败: {e}")
            raise
    
    def get_recent_transactions(self, limit: int = 100) -> List[Dict]:
        """获取最近的交易记录"""
        try:
            query = """
            SELECT 
                t.id,
                t.created_at,
                t.amount,
                t.description,
                t.status,
                sa.name as sender_name,
                ra.name as receiver_name
            FROM transactions t
            JOIN accounts sa ON sa.id = t.sender_account_id
            JOIN accounts ra ON ra.id = t.receiver_account_id
            ORDER BY t.created_at DESC
            LIMIT %s
            """
            return self.db.execute_query(query, (limit,))
            
        except Exception as e:
            logger.error(f"获取最近交易失败: {e}")
            raise

# 全局风险分析服务
risk_service = RiskAnalysisService(db_manager)

def _ensure_stored_procedure_latest() -> None:
	"""Ensure GetRiskTransactions is on the latest definition.

	This proactively recreates the procedure to avoid legacy versions
	(e.g. those referencing mc.metric_c_sum) that cause 1054 errors.
	"""
	# 直接使用基础存储过程，避免文件查找问题
	procedure_sql = _get_basic_procedure_sql()
	logger.info("使用基础存储过程定义")

def _get_basic_procedure_sql() -> str:
	"""获取基础存储过程定义"""
	return (
		"DROP PROCEDURE IF EXISTS GetRiskTransactions;"
		"\nCREATE PROCEDURE GetRiskTransactions(\n"
		"    IN time_range_hours INT,\n"
		"    IN min_metric_a INT,\n"
		"    IN min_metric_b INT,\n"
		"    IN max_metric_c DECIMAL(15,2)\n"
		")\nBEGIN\n"
		"    DECLARE start_time TIMESTAMP;\n"
		"    DECLARE end_time TIMESTAMP;\n\n"
		"    SET end_time = NOW();\n"
		"    SET start_time = DATE_SUB(end_time, INTERVAL time_range_hours HOUR);\n\n"
		"    WITH metric_a_data AS (\n"
		"        SELECT t_out.sender_account_id, COUNT(DISTINCT t_out.id) AS metric_a_count\n"
		"        FROM transactions t_out\n"
		"        JOIN transactions t_in ON t_in.receiver_account_id = t_out.sender_account_id\n"
		"        WHERE t_out.amount >= 50000 AND t_out.status = 'posted'\n"
		"          AND t_out.created_at >= start_time AND t_out.created_at < end_time\n"
		"          AND t_in.amount  >= 50000 AND t_in.status = 'posted'\n"
		"          AND t_in.created_at >= start_time AND t_in.created_at < end_time\n"
		"          AND t_out.created_at >= t_in.created_at\n"
		"          AND t_out.created_at <= DATE_ADD(t_in.created_at, INTERVAL 2 MINUTE)\n"
		"        GROUP BY t_out.sender_account_id\n"
		"    ),\n\n"
		"    metric_b_data AS (\n"
		"        SELECT t.sender_account_id, COUNT(DISTINCT t.id) AS metric_b_count\n"
		"        FROM transactions t\n"
		"        JOIN logins l ON l.account_id = t.sender_account_id\n"
		"        WHERE t.amount >= 50000 AND t.status = 'posted'\n"
		"          AND t.created_at >= start_time AND t.created_at < end_time\n"
		"          AND l.login_at >= start_time AND l.login_at < end_time\n"
		"          AND l.login_at <= t.created_at\n"
		"          AND t.created_at <= DATE_ADD(l.login_at, INTERVAL 5 MINUTE)\n"
		"        GROUP BY t.sender_account_id\n"
		"    ),\n\n"
		"    metric_c_data AS (\n"
		"        SELECT \n"
		"            t.receiver_account_id,\n"
		"            CASE \n"
		"                WHEN (\n"
		"                    SELECT MAX(t_latest.created_at)\n"
		"                    FROM transactions t_latest\n"
		"                    WHERE t_latest.receiver_account_id = t.receiver_account_id\n"
		"                      AND t_latest.status = 'posted'\n"
		"                      AND t_latest.created_at >= start_time AND t_latest.created_at < end_time\n"
		"                ) IS NULL THEN 0\n"
		"                ELSE (\n"
		"                    CASE\n"
		"                        WHEN (\n"
		"                            SELECT MAX(t_prev.created_at)\n"
		"                            FROM transactions t_prev\n"
		"                            WHERE t_prev.receiver_account_id = t.receiver_account_id\n"
		"                              AND t_prev.status = 'posted'\n"
		"                              AND t_prev.created_at < (\n"
		"                                  SELECT MAX(t_latest2.created_at)\n"
		"                                  FROM transactions t_latest2\n"
		"                                  WHERE t_latest2.receiver_account_id = t.receiver_account_id\n"
		"                                    AND t_latest2.status = 'posted'\n"
		"                                    AND t_latest2.created_at >= start_time AND t_latest2.created_at < end_time\n"
		"                              )\n"
		"                        ) IS NULL THEN 0\n"
		"                        ELSE (\n"
		"                            CASE WHEN TIMESTAMPDIFF(DAY,\n"
		"                                (\n"
		"                                    SELECT MAX(t_prev2.created_at) \n"
		"                                    FROM transactions t_prev2\n"
		"                                    WHERE t_prev2.receiver_account_id = t.receiver_account_id\n"
		"                                      AND t_prev2.status = 'posted'\n"
		"                                      AND t_prev2.created_at < (\n"
		"                                          SELECT MAX(t_latest3.created_at)\n"
		"                                          FROM transactions t_latest3\n"
		"                                          WHERE t_latest3.receiver_account_id = t.receiver_account_id\n"
		"                                            AND t_latest3.status = 'posted'\n"
		"                                            AND t_latest3.created_at >= start_time AND t_latest3.created_at < end_time\n"
		"                                      )\n"
		"                                ),\n"
		"                                (\n"
		"                                    SELECT MAX(t_latest4.created_at)\n"
		"                                    FROM transactions t_latest4\n"
		"                                    WHERE t_latest4.receiver_account_id = t.receiver_account_id\n"
		"                                      AND t_latest4.status = 'posted'\n"
		"                                      AND t_latest4.created_at >= start_time AND t_latest4.created_at < end_time\n"
		"                                )\n"
		"                            ) > 30 THEN 1 ELSE 0 END\n"
		"                        )\n"
		"                    END\n"
		"                )\n"
		"            END AS metric_c_flag\n"
		"        FROM transactions t\n"
		"        WHERE t.status = 'posted' AND t.created_at >= start_time AND t.created_at < end_time\n"
		"        GROUP BY t.receiver_account_id\n"
		"    )\n\n"
		"    SELECT DISTINCT\n"
		"        t.id AS transaction_id,\n"
		"        t.created_at AS transaction_time,\n"
		"        t.amount,\n"
		"        t.description,\n"
		"        t.sender_account_id AS victim_account_id,\n"
		"        sa.name AS victim_name, sa.phone AS victim_phone, sa.email AS victim_email, sa.account_type AS victim_type,\n"
		"        t.receiver_account_id AS suspicious_account_id,\n"
		"        ra.name AS suspicious_name, ra.phone AS suspicious_phone, ra.email AS suspicious_email, ra.account_type AS suspicious_type,\n"
		"        COALESCE(ma.metric_a_count, 0) AS metric_a,\n"
		"        COALESCE(mb.metric_b_count, 0) AS metric_b,\n"
		"        COALESCE(mc.metric_c_flag, 0) AS metric_c,\n"
		"        CASE \n"
		"            WHEN COALESCE(ma.metric_a_count, 0) > 0 AND COALESCE(mb.metric_b_count, 0) > 0 AND COALESCE(mc.metric_c_flag, 0) = 1 THEN 'HIGH'\n"
		"            WHEN COALESCE(ma.metric_a_count, 0) > 0 OR COALESCE(mb.metric_b_count, 0) > 0 THEN 'MEDIUM'\n"
		"            ELSE 'LOW'\n"
		"        END AS risk_level\n"
		"    FROM transactions t\n"
		"    JOIN accounts sa ON sa.id = t.sender_account_id\n"
		"    JOIN accounts ra ON ra.id = t.receiver_account_id\n"
		"    LEFT JOIN metric_a_data ma ON ma.sender_account_id = t.sender_account_id\n"
		"    LEFT JOIN metric_b_data mb ON mb.sender_account_id = t.sender_account_id\n"
		"    LEFT JOIN metric_c_data mc ON mc.receiver_account_id = t.receiver_account_id\n"
		"    WHERE t.amount >= 50000 AND t.status = 'posted'\n"
		"      AND t.created_at >= start_time AND t.created_at < end_time\n"
		"      AND COALESCE(ma.metric_a_count, 0) >= min_metric_a\n"
		"      AND COALESCE(mb.metric_b_count, 0) >= min_metric_b\n"
		"      AND (max_metric_c >= 999999000 OR COALESCE(mc.metric_c_flag, 0) = 1)\n"
		"    ORDER BY t.created_at DESC, t.amount DESC;\n"
		"END;"
	)
	try:
		with db_manager.get_connection() as conn:
			with conn.cursor() as cur:
				# split into two executes to avoid multi-statement limits in some configs
				cur.execute("DROP PROCEDURE IF EXISTS GetRiskTransactions;")
				cur.execute(procedure_sql.split("\n", 1)[1])  # execute CREATE ...
				logger.info("Stored procedure ensured to latest definition")
	except Exception as ex:
		logger.warning(f"Failed to ensure stored procedure: {ex}")

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/risk-analysis', methods=['POST'])
def risk_analysis():
	"""风险分析API"""
	try:
		data = request.get_json() or {}
		start_ts = datetime.now()
		# 兼容前端字段命名（前端发送 min_metric_a 等）
		time_range = data.get('time_range', '24h')
		metric_a_min = data.get('min_metric_a', data.get('metric_a_min', 1))
		metric_b_min = data.get('min_metric_b', data.get('metric_b_min', 1))
		metric_c_max = data.get('max_metric_c', data.get('metric_c_max', 0))
		cache_key = f"api:risk:{time_range}:{metric_a_min}:{metric_b_min}:{metric_c_max}"
		cached = cache_get(cache_key)
		if cached:
			return jsonify(json.loads(cached))
		# 获取风险交易
		records = risk_service.get_risk_transactions(
			time_range=time_range,
			metric_a_min=metric_a_min,
			metric_b_min=metric_b_min,
			metric_c_max=metric_c_max
		)
		# 转换为前端期望的数据结构
		transactions = []
		for t in records:
			transactions.append({
				'transaction_id': t.transaction_id,
				'transaction_time': t.transaction_time,
				'amount': t.amount,
				'description': t.description,
				'victim_account': {
					'account_id': t.victim_account_id,
					'name': t.victim_name,
					'phone': t.victim_phone,
					'type': t.victim_type
				},
				'suspicious_account': {
					'account_id': t.suspicious_account_id,
					'name': t.suspicious_name,
					'phone': t.suspicious_phone,
					'type': t.suspicious_type
				},
				'risk_metrics': {
					'metric_a': t.metric_a,
					'metric_b': t.metric_b,
					'metric_c': t.metric_c
				},
				'risk_level': t.risk_level
			})
		elapsed_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
		response = {
			'status': 'success',
			'time_range': time_range,
			'query_time_ms': elapsed_ms,
			'total_count': len(transactions),
			'criteria': {
				'min_metric_a': metric_a_min,
				'min_metric_b': metric_b_min,
				'max_metric_c': metric_c_max
			},
			'transactions': transactions
		}
		# cache for 30s
		cache_set(cache_key, json.dumps(response), 30)
		return jsonify(response)
	except Exception as e:
		logger.error(f"风险分析API错误: {e}")
		return jsonify({
			'status': 'error',
			'error': str(e)
		}), 500

@app.route('/api/indicators-summary', methods=['GET'])
def indicators_summary():
    """获取风险指标汇总"""
    try:
        summary = risk_service.get_risk_indicators_summary()
        return jsonify({
            'success': True,
            'data': summary
        })
    except Exception as e:
        logger.error(f"获取指标汇总失败: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/recent-transactions', methods=['GET'])
def recent_transactions():
    """获取最近交易记录"""
    try:
        limit = request.args.get('limit', 100, type=int)
        transactions = risk_service.get_recent_transactions(limit)
        return jsonify({
            'success': True,
            'data': transactions
        })
    except Exception as e:
        logger.error(f"获取最近交易失败: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats', methods=['GET'])
def system_stats():
	"""系统统计（供前端看板使用）"""
	try:
		# cache first
		cache_key = 'api:stats:v1'
		cached = cache_get(cache_key)
		if cached:
			return jsonify(json.loads(cached))
		rows = db_manager.execute_query(
			"""
			SELECT
				(SELECT COUNT(*) FROM accounts) AS total_accounts,
				(SELECT COUNT(*) FROM logins) AS total_logins,
				(SELECT COUNT(*) FROM transactions) AS total_transactions,
				(SELECT COUNT(*) FROM transactions WHERE amount >= 50000 AND status = 'posted') AS large_transactions
			"""
		)
		stats = rows[0] if rows else {}
		payload = {
			# canonical field names
			'total_accounts': int((stats.get('total_accounts') or 0)),
			'total_logins': int((stats.get('total_logins') or 0)),
			'total_transactions': int((stats.get('total_transactions') or 0)),
			'large_transactions': int((stats.get('large_transactions') or 0)),
			'timestamp': datetime.now().isoformat()
		}
		# backward-compatible aliases expected by some frontends
		payload['accounts'] = payload['total_accounts']
		payload['logins'] = payload['total_logins']
		payload['transactions'] = payload['total_transactions']
		payload['large'] = payload['large_transactions']
		# store cache for 10s
		cache_set(cache_key, json.dumps(payload), 10)
		return jsonify(payload)
	except Exception as e:
		logger.error(f"获取系统统计失败: {e}")
		# 兜底返回0，前端避免 NaN
		return jsonify({
			'total_accounts': 0,
			'total_logins': 0,
			'total_transactions': 0,
			'large_transactions': 0,
			'timestamp': datetime.now().isoformat(),
			'error': str(e)
		})

@app.route('/api/health', methods=['GET'])
@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        # 测试数据库连接
        db_manager.execute_query("SELECT 1")
        return jsonify({
            'success': True,
            'status': 'healthy',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/ping', methods=['GET'])
def ping():
    return 'pong', 200

@app.route('/routes', methods=['GET'])
def list_routes():
    try:
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append({
                'rule': str(rule),
                'endpoint': rule.endpoint,
                'methods': sorted(list(rule.methods - {'HEAD', 'OPTIONS'}))
            })
        return jsonify({'routes': routes})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
	# 检查数据库连接
	try:
		# ensure latest procedure to avoid legacy mc.metric_c_sum
		_ensure_stored_procedure_latest()
		db_manager.execute_query("SELECT 1")
		logger.info("数据库连接成功")
	except Exception as e:
		logger.error(f"数据库连接失败: {e}")
		exit(1)

	# 启动应用（端口可配置：优先 API_PORT，其次 PORT，默认为 8080）
	listen_port = int(os.getenv('API_PORT', _file_cfg.get('API_PORT', os.getenv('PORT', 8080))))
	app.run(host='0.0.0.0', port=listen_port, debug=True)


