import argparse
import sys
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
import pyodbc
from rich.console import Console
from rich.table import Table

# ---------------- Configuration ----------------
LARGE_AMOUNT_THRESHOLD = 50000
# Table and column names
TABLE_ACCOUNTS = "accounts"
TABLE_LOGINS = "logins"
TABLE_TX = "transactions"
# transactions columns: id, sender_account_id, receiver_account_id, amount, created_at, status
# logins columns: id, account_id, login_at
# accounts columns: id, name

console = Console()

# ---------------- Helpers ----------------

def parse_range_to_hours(token: str) -> timedelta:
	mapping = {
		"24h": timedelta(hours=24),
		"3d": timedelta(days=3),
		"7d": timedelta(days=7),
		"30d": timedelta(days=30),
		"6m": timedelta(days=30*6),
		"1y": timedelta(days=365),
	}
	if token not in mapping:
		raise ValueError("Invalid --range. Use one of: 24h, 3d, 7d, 30d, 6m, 1y")
	return mapping[token]


def compute_time_window(range_token: str):
	now = datetime.now(tzlocal())
	start = now - parse_range_to_hours(range_token)
	return start, now


# ---------------- SQL Builders ----------------

def build_sql_postgres() -> str:
	return f"""
WITH large_in AS (
  SELECT
    t.id AS in_tx_id,
    t.receiver_account_id AS account_id,
    t.amount,
    t.created_at AS in_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL '30 days'
),
large_out AS (
  SELECT
    t.id AS out_tx_id,
    t.sender_account_id AS account_id,
    t.receiver_account_id AS payee_account_id,
    t.amount,
    t.created_at AS out_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL '30 days'
),
a_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS a_count
  FROM large_out o
  JOIN large_in i
    ON i.account_id = o.account_id
   AND o.out_time >= i.in_time
   AND o.out_time <= i.in_time + INTERVAL '2 minutes'
  GROUP BY 1
),
b_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS b_count
  FROM large_out o
  JOIN {TABLE_LOGINS} l
    ON l.account_id = o.account_id
   AND l.login_at <= o.out_time
   AND o.out_time <= l.login_at + INTERVAL '5 minutes'
  GROUP BY 1
),
c_sums AS (
  SELECT
    t.receiver_account_id AS payee_account_id,
    COALESCE(SUM(t.amount), 0) AS c_sum
  FROM {TABLE_TX} t
  WHERE t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL '30 days'
  GROUP BY 1
),
candidate_outs AS (
  SELECT
    t.*
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= ?
    AND t.created_at <  ?
)
SELECT
  t.id AS tx_id,
  t.created_at AS tx_time,
  t.amount,
  t.sender_account_id AS victim_account_id,
  sa.name AS victim_name,
  t.receiver_account_id AS suspicious_account_id,
  ra.name AS suspicious_name,
  COALESCE(a.a_count, 0) AS metric_a,
  COALESCE(b.b_count, 0) AS metric_b,
  COALESCE(c.c_sum, 0) AS metric_c
FROM candidate_outs t
LEFT JOIN a_counts a ON a.account_id = t.sender_account_id
LEFT JOIN b_counts b ON b.account_id = t.sender_account_id
LEFT JOIN c_sums c ON c.payee_account_id = t.receiver_account_id
LEFT JOIN {TABLE_ACCOUNTS} sa ON sa.id = t.sender_account_id
LEFT JOIN {TABLE_ACCOUNTS} ra ON ra.id = t.receiver_account_id
WHERE COALESCE(a.a_count, 0) > 0
  AND COALESCE(b.b_count, 0) > 0
  AND COALESCE(c.c_sum, 0) = 0
ORDER BY t.created_at DESC
	"""


def build_sql_mysql() -> str:
	return f"""
WITH large_in AS (
  SELECT
    t.id AS in_tx_id,
    t.receiver_account_id AS account_id,
    t.amount,
    t.created_at AS in_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL 30 DAY
),
large_out AS (
  SELECT
    t.id AS out_tx_id,
    t.sender_account_id AS account_id,
    t.receiver_account_id AS payee_account_id,
    t.amount,
    t.created_at AS out_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL 30 DAY
),
a_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS a_count
  FROM large_out o
  JOIN large_in i
    ON i.account_id = o.account_id
   AND o.out_time >= i.in_time
   AND o.out_time <= i.in_time + INTERVAL 2 MINUTE
  GROUP BY 1
),
b_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS b_count
  FROM large_out o
  JOIN {TABLE_LOGINS} l
    ON l.account_id = o.account_id
   AND l.login_at <= o.out_time
   AND o.out_time <= l.login_at + INTERVAL 5 MINUTE
  GROUP BY 1
),
c_sums AS (
  SELECT
    t.receiver_account_id AS payee_account_id,
    COALESCE(SUM(t.amount), 0) AS c_sum
  FROM {TABLE_TX} t
  WHERE t.status = 'posted'
    AND t.created_at >= NOW() - INTERVAL 30 DAY
  GROUP BY 1
),
candidate_outs AS (
  SELECT
    t.*
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= ?
    AND t.created_at <  ?
)
SELECT
  t.id AS tx_id,
  t.created_at AS tx_time,
  t.amount,
  t.sender_account_id AS victim_account_id,
  sa.name AS victim_name,
  t.receiver_account_id AS suspicious_account_id,
  ra.name AS suspicious_name,
  COALESCE(a.a_count, 0) AS metric_a,
  COALESCE(b.b_count, 0) AS metric_b,
  COALESCE(c.c_sum, 0) AS metric_c
FROM candidate_outs t
LEFT JOIN a_counts a ON a.account_id = t.sender_account_id
LEFT JOIN b_counts b ON b.account_id = t.sender_account_id
LEFT JOIN c_sums c ON c.payee_account_id = t.receiver_account_id
LEFT JOIN {TABLE_ACCOUNTS} sa ON sa.id = t.sender_account_id
LEFT JOIN {TABLE_ACCOUNTS} ra ON ra.id = t.receiver_account_id
WHERE COALESCE(a.a_count, 0) > 0
  AND COALESCE(b.b_count, 0) > 0
  AND COALESCE(c.c_sum, 0) = 0
ORDER BY t.created_at DESC
	"""


def build_sql_sqlserver() -> str:
	# SQL Server uses DATEADD and GETDATE(); supports CTE; uses ISNULL
	return f"""
WITH large_in AS (
  SELECT
    t.id AS in_tx_id,
    t.receiver_account_id AS account_id,
    t.amount,
    t.created_at AS in_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= DATEADD(DAY, -30, GETDATE())
),
large_out AS (
  SELECT
    t.id AS out_tx_id,
    t.sender_account_id AS account_id,
    t.receiver_account_id AS payee_account_id,
    t.amount,
    t.created_at AS out_time
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= DATEADD(DAY, -30, GETDATE())
),
a_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS a_count
  FROM large_out o
  JOIN large_in i
    ON i.account_id = o.account_id
   AND o.out_time >= i.in_time
   AND o.out_time <= DATEADD(MINUTE, 2, i.in_time)
  GROUP BY o.account_id
),
b_counts AS (
  SELECT
    o.account_id,
    COUNT(DISTINCT o.out_tx_id) AS b_count
  FROM large_out o
  JOIN {TABLE_LOGINS} l
    ON l.account_id = o.account_id
   AND l.login_at <= o.out_time
   AND o.out_time <= DATEADD(MINUTE, 5, l.login_at)
  GROUP BY o.account_id
),
c_sums AS (
  SELECT
    t.receiver_account_id AS payee_account_id,
    ISNULL(SUM(t.amount), 0) AS c_sum
  FROM {TABLE_TX} t
  WHERE t.status = 'posted'
    AND t.created_at >= DATEADD(DAY, -30, GETDATE())
  GROUP BY t.receiver_account_id
),
candidate_outs AS (
  SELECT
    t.*
  FROM {TABLE_TX} t
  WHERE t.amount >= {LARGE_AMOUNT_THRESHOLD}
    AND t.status = 'posted'
    AND t.created_at >= ?
    AND t.created_at <  ?
)
SELECT
  t.id AS tx_id,
  t.created_at AS tx_time,
  t.amount,
  t.sender_account_id AS victim_account_id,
  sa.name AS victim_name,
  t.receiver_account_id AS suspicious_account_id,
  ra.name AS suspicious_name,
  ISNULL(a.a_count, 0) AS metric_a,
  ISNULL(b.b_count, 0) AS metric_b,
  ISNULL(c.c_sum, 0) AS metric_c
FROM candidate_outs t
LEFT JOIN a_counts a ON a.account_id = t.sender_account_id
LEFT JOIN b_counts b ON b.account_id = t.sender_account_id
LEFT JOIN c_sums c ON c.payee_account_id = t.receiver_account_id
LEFT JOIN {TABLE_ACCOUNTS} sa ON sa.id = t.sender_account_id
LEFT JOIN {TABLE_ACCOUNTS} ra ON ra.id = t.receiver_account_id
WHERE ISNULL(a.a_count, 0) > 0
  AND ISNULL(b.b_count, 0) > 0
  AND ISNULL(c.c_sum, 0) = 0
ORDER BY t.created_at DESC
	"""


# ---------------- Execution ----------------

def get_connection(args: argparse.Namespace) -> pyodbc.Connection:
	if args.dsn:
		conn_str = f"DSN={args.dsn};UID={args.user};PWD={args.password}"
		return pyodbc.connect(conn_str, autocommit=False)
	else:
		parts = []
		if args.driver:
			parts.append(f"DRIVER={{{args.driver}}}")
		if args.server:
			parts.append(f"SERVER={args.server}")
		if args.port:
			parts.append(f"PORT={args.port}")
		if args.database:
			parts.append(f"DATABASE={args.database}")
		if args.user:
			parts.append(f"UID={args.user}")
		if args.password:
			parts.append(f"PWD={args.password}")
		conn_str = ";".join(parts)
		return pyodbc.connect(conn_str, autocommit=False)


def main():
	parser = argparse.ArgumentParser(description="过往数据分析任务 (A>0, B>0, C=0)")
	conn_grp = parser.add_mutually_exclusive_group(required=False)
	conn_grp.add_argument('--dsn', help='ODBC DSN 名称')
	parser.add_argument('--driver', help='ODBC Driver 名称，例如 {PostgreSQL Unicode(x64)}')
	parser.add_argument('--server', help='数据库服务器地址，例如 127.0.0.1')
	parser.add_argument('--port', help='端口，例如 5432/3306/1433')
	parser.add_argument('--database', help='数据库名')
	parser.add_argument('--user', help='用户名')
	parser.add_argument('--password', help='密码')
	parser.add_argument('--dialect', choices=['postgres', 'mysql', 'sqlserver'], required=True, help='数据库类型')
	parser.add_argument('--range', dest='range_token', choices=['24h','3d','7d','30d','6m','1y'], required=True, help='时间范围')
	args = parser.parse_args()

	start_ts, end_ts = compute_time_window(args.range_token)

	if args.dialect == 'postgres':
		sql = build_sql_postgres()
	elif args.dialect == 'mysql':
		sql = build_sql_mysql()
	else:
		sql = build_sql_sqlserver()

	console.print(f"连接数据库，执行查询，范围: {start_ts} ~ {end_ts}")
	try:
		with get_connection(args) as conn:
			with conn.cursor() as cur:
				cur.execute(sql, (start_ts, end_ts))
				columns = [d[0] for d in cur.description]
				rows = cur.fetchall()
				table = Table(show_lines=False)
				for col in columns:
					table.add_column(col)
				for r in rows:
					table.add_row(*[str(v) if v is not None else '' for v in r])
				console.print(table)
	except pyodbc.Error as e:
		console.print(f"[red]数据库错误：[/red]{e}")
		sys.exit(1)


if __name__ == '__main__':
	main() 