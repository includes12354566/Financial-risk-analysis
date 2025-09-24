#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>

// Simple RAII for ODBC handles
struct OdbcHandle {
	SQLSMALLINT type;
	SQLHANDLE handle;
	OdbcHandle(SQLSMALLINT t) : type(t), handle(SQL_NULL_HANDLE) {}
	~OdbcHandle() {
		if (handle != SQL_NULL_HANDLE) {
			SQLFreeHandle(type, handle);
		}
	}
};

static const long long LARGE_AMOUNT_THRESHOLD = 50000;

static std::string build_sql(const std::string &dialect) {
	if (dialect == "postgres") {
		return std::string(
			"WITH large_in AS (\n"
			"  SELECT t.id AS in_tx_id, t.receiver_account_id AS account_id, t.amount, t.created_at AS in_time\n"
			"  FROM transactions t\n"
			"  WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= NOW() - INTERVAL '30 days'\n"
			"),\nlarge_out AS (\n"
			"  SELECT t.id AS out_tx_id, t.sender_account_id AS account_id, t.receiver_account_id AS payee_account_id, t.amount, t.created_at AS out_time\n"
			"  FROM transactions t\n"
			"  WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= NOW() - INTERVAL '30 days'\n"
			"),\n"
			"a_counts AS (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS a_count\n"
			"  FROM large_out o JOIN large_in i ON i.account_id = o.account_id AND o.out_time >= i.in_time AND o.out_time <= i.in_time + INTERVAL '2 minutes'\n"
			"  GROUP BY 1\n"
			"),\n"
			"b_counts AS (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS b_count\n"
			"  FROM large_out o JOIN logins l ON l.account_id = o.account_id AND l.login_at <= o.out_time AND o.out_time <= l.login_at + INTERVAL '5 minutes'\n"
			"  GROUP BY 1\n"
			"),\n"
			"c_sums AS (\n"
			"  SELECT t.receiver_account_id AS payee_account_id, COALESCE(SUM(t.amount), 0) AS c_sum\n"
			"  FROM transactions t WHERE t.status = 'posted' AND t.created_at >= NOW() - INTERVAL '30 days'\n"
			"  GROUP BY 1\n"
			"),\n"
			"candidate_outs AS (\n"
			"  SELECT t.* FROM transactions t WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= ? AND t.created_at < ?\n"
			")\n"
			"SELECT t.id AS tx_id, t.created_at AS tx_time, t.amount, t.sender_account_id AS victim_account_id, sa.name AS victim_name,\n"
			"       t.receiver_account_id AS suspicious_account_id, ra.name AS suspicious_name,\n"
			"       COALESCE(a.a_count, 0) AS metric_a, COALESCE(b.b_count, 0) AS metric_b, COALESCE(c.c_sum, 0) AS metric_c\n"
			"FROM candidate_outs t\n"
			"LEFT JOIN a_counts a ON a.account_id = t.sender_account_id\n"
			"LEFT JOIN b_counts b ON b.account_id = t.sender_account_id\n"
			"LEFT JOIN c_sums c ON c.payee_account_id = t.receiver_account_id\n"
			"LEFT JOIN accounts sa ON sa.id = t.sender_account_id\n"
			"LEFT JOIN accounts ra ON ra.id = t.receiver_account_id\n"
			"WHERE COALESCE(a.a_count, 0) > 0 AND COALESCE(b.b_count, 0) > 0 AND COALESCE(c.c_sum, 0) = 0\n"
			"ORDER BY t.created_at DESC\n");
	} else if (dialect == "mysql") {
		return std::string(
			"SELECT t.id AS tx_id, t.created_at AS tx_time, t.amount, t.sender_account_id AS victim_account_id, sa.name AS victim_name,\n"
			"       t.receiver_account_id AS suspicious_account_id, ra.name AS suspicious_name,\n"
			"       COALESCE(a.a_count, 0) AS metric_a, COALESCE(b.b_count, 0) AS metric_b, COALESCE(c.c_sum, 0) AS metric_c\n"
			"FROM (\n"
			"  SELECT t.* FROM transactions t WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= ? AND t.created_at < ?\n"
			") t\n"
			"LEFT JOIN (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS a_count\n"
			"  FROM (\n"
			"    SELECT t.id AS out_tx_id, t.sender_account_id AS account_id, t.created_at AS out_time\n"
			"    FROM transactions t\n"
			"    WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= NOW() - INTERVAL 30 DAY\n"
			"  ) o\n"
			"  JOIN (\n"
			"    SELECT t.receiver_account_id AS account_id, t.created_at AS in_time\n"
			"    FROM transactions t\n"
			"    WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= NOW() - INTERVAL 30 DAY\n"
			"  ) i ON i.account_id = o.account_id AND o.out_time >= i.in_time AND o.out_time <= i.in_time + INTERVAL 2 MINUTE\n"
			"  GROUP BY o.account_id\n"
			") a ON a.account_id = t.sender_account_id\n"
			"LEFT JOIN (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS b_count\n"
			"  FROM (\n"
			"    SELECT t.id AS out_tx_id, t.sender_account_id AS account_id, t.created_at AS out_time\n"
			"    FROM transactions t\n"
			"    WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= NOW() - INTERVAL 30 DAY\n"
			"  ) o\n"
			"  JOIN logins l ON l.account_id = o.account_id AND l.login_at <= o.out_time AND o.out_time <= l.login_at + INTERVAL 5 MINUTE\n"
			"  GROUP BY o.account_id\n"
			") b ON b.account_id = t.sender_account_id\n"
			"LEFT JOIN (\n"
			"  SELECT t.receiver_account_id AS payee_account_id, COALESCE(SUM(t.amount), 0) AS c_sum\n"
			"  FROM transactions t WHERE t.status = 'posted' AND t.created_at >= NOW() - INTERVAL 30 DAY\n"
			"  GROUP BY t.receiver_account_id\n"
			") c ON c.payee_account_id = t.receiver_account_id\n"
			"LEFT JOIN accounts sa ON sa.id = t.sender_account_id\n"
			"LEFT JOIN accounts ra ON ra.id = t.receiver_account_id\n"
			"WHERE COALESCE(a.a_count, 0) > 0 AND COALESCE(b.b_count, 0) > 0 AND COALESCE(c.c_sum, 0) = 0\n"
			"ORDER BY t.created_at DESC\n");
	} else { // sqlserver
		return std::string(
			"WITH large_in AS (\n"
			"  SELECT t.id AS in_tx_id, t.receiver_account_id AS account_id, t.amount, t.created_at AS in_time\n"
			"  FROM transactions t\n"
			"  WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= DATEADD(DAY, -30, GETDATE())\n"
			"),\nlarge_out AS (\n"
			"  SELECT t.id AS out_tx_id, t.sender_account_id AS account_id, t.receiver_account_id AS payee_account_id, t.amount, t.created_at AS out_time\n"
			"  FROM transactions t\n"
			"  WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= DATEADD(DAY, -30, GETDATE())\n"
			"),\n"
			"a_counts AS (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS a_count\n"
			"  FROM large_out o JOIN large_in i ON i.account_id = o.account_id AND o.out_time >= i.in_time AND o.out_time <= DATEADD(MINUTE, 2, i.in_time)\n"
			"  GROUP BY o.account_id\n"
			"),\n"
			"b_counts AS (\n"
			"  SELECT o.account_id, COUNT(DISTINCT o.out_tx_id) AS b_count\n"
			"  FROM large_out o JOIN logins l ON l.account_id = o.account_id AND l.login_at <= o.out_time AND o.out_time <= DATEADD(MINUTE, 5, l.login_at)\n"
			"  GROUP BY o.account_id\n"
			"),\n"
			"c_sums AS (\n"
			"  SELECT t.receiver_account_id AS payee_account_id, ISNULL(SUM(t.amount), 0) AS c_sum\n"
			"  FROM transactions t WHERE t.status = 'posted' AND t.created_at >= DATEADD(DAY, -30, GETDATE())\n"
			"  GROUP BY t.receiver_account_id\n"
			"),\n"
			"candidate_outs AS (\n"
			"  SELECT t.* FROM transactions t WHERE t.amount >= ") + std::to_string(LARGE_AMOUNT_THRESHOLD) + std::string(
			" AND t.status = 'posted' AND t.created_at >= ? AND t.created_at < ?\n"
			")\n"
			"SELECT t.id AS tx_id, t.created_at AS tx_time, t.amount, t.sender_account_id AS victim_account_id, sa.name AS victim_name,\n"
			"       t.receiver_account_id AS suspicious_account_id, ra.name AS suspicious_name,\n"
			"       ISNULL(a.a_count, 0) AS metric_a, ISNULL(b.b_count, 0) AS metric_b, ISNULL(c.c_sum, 0) AS metric_c\n"
			"FROM candidate_outs t\n"
			"LEFT JOIN a_counts a ON a.account_id = t.sender_account_id\n"
			"LEFT JOIN b_counts b ON b.account_id = t.sender_account_id\n"
			"LEFT JOIN c_sums c ON c.payee_account_id = t.receiver_account_id\n"
			"LEFT JOIN accounts sa ON sa.id = t.sender_account_id\n"
			"LEFT JOIN accounts ra ON ra.id = t.receiver_account_id\n"
			"WHERE ISNULL(a.a_count, 0) > 0 AND ISNULL(b.b_count, 0) > 0 AND ISNULL(c.c_sum, 0) = 0\n"
			"ORDER BY t.created_at DESC\n");
	}
}

static std::tm to_utc_tm(std::time_t t) {
	std::tm out{};
	gmtime_s(&out, &t);
	return out;
}

static std::string format_timestamp_for_odbc(std::time_t t) {
	std::tm tm = to_utc_tm(t);
	char buf[32];
	std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
		1900 + tm.tm_year, 1 + tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
	return std::string(buf);
}

static bool parse_args(int argc, char **argv, std::string &dsn, std::string &driver, std::string &server,
	std::string &port, std::string &database, std::string &user, std::string &password,
	std::string &dialect, std::string &range_token) {
	for (int i = 1; i < argc; ++i) {
		std::string a = argv[i];
		auto getv = [&](const char *name, std::string &out)->bool{
			if (a == name && i + 1 < argc) { out = argv[++i]; return true; }
			return false;
		};
		if (getv("--dsn", dsn)) continue;
		if (getv("--driver", driver)) continue;
		if (getv("--server", server)) continue;
		if (getv("--port", port)) continue;
		if (getv("--database", database)) continue;
		if (getv("--user", user)) continue;
		if (getv("--password", password)) continue;
		if (getv("--dialect", dialect)) continue;
		if (getv("--range", range_token)) continue;
		if (a == "-h" || a == "--help") {
			return false;
		}
	}
	if (dialect != "postgres" && dialect != "mysql" && dialect != "sqlserver") {
		std::cerr << "--dialect 必须为 postgres/mysql/sqlserver" << std::endl;
		return false;
	}
	if (range_token.empty()) {
		std::cerr << "--range 必填: 24h, 3d, 7d, 30d, 6m, 1y" << std::endl;
		return false;
	}
	return true;
}

static std::chrono::seconds range_to_duration(const std::string &token) {
	if (token == "24h") return std::chrono::hours(24);
	if (token == "3d") return std::chrono::hours(24 * 3);
	if (token == "7d") return std::chrono::hours(24 * 7);
	if (token == "30d") return std::chrono::hours(24 * 30);
	if (token == "6m") return std::chrono::hours(24 * 30 * 6);
	if (token == "1y") return std::chrono::hours(24 * 365);
	return std::chrono::seconds(0);
}

static bool odbc_check(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle, const char *msg) {
	if (SQL_SUCCEEDED(ret)) return true;
	SQLCHAR state[6];
	SQLINTEGER native;
	SQLCHAR text[256];
	SQLSMALLINT len;
	if (SQLGetDiagRecA(handleType, handle, 1, state, &native, text, sizeof(text), &len) == SQL_SUCCESS) {
		std::cerr << msg << ": [" << state << "] " << text << std::endl;
	} else {
		std::cerr << msg << ": ODBC error" << std::endl;
	}
	return false;
}

int main(int argc, char **argv) {
	std::string dsn, driver, server, port, database, user, password, dialect, range_token;
	if (!parse_args(argc, argv, dsn, driver, server, port, database, user, password, dialect, range_token)) {
		std::cout << "用法: Project3.exe [--dsn DSN] | [--driver {Driver} --server HOST --port PORT --database DB --user USER --password PASS] --dialect postgres|mysql|sqlserver --range 24h|3d|7d|30d|6m|1y" << std::endl;
		return 1;
	}

	// Compute time window (local now -> UTC text)
	auto now = std::chrono::system_clock::now();
	auto dur = range_to_duration(range_token);
	auto start = now - dur;
	time_t t_start = std::chrono::system_clock::to_time_t(start);
	time_t t_end = std::chrono::system_clock::to_time_t(now);
	std::string start_ts = format_timestamp_for_odbc(t_start);
	std::string end_ts = format_timestamp_for_odbc(t_end);

	// Build connection string
	std::string conn_str;
	if (!dsn.empty()) {
		conn_str = std::string("DSN=") + dsn + ";UID=" + user + ";PWD=" + password;
	} else {
		if (!driver.empty()) conn_str += std::string("DRIVER=") + driver + ";";
		if (!server.empty()) conn_str += std::string("SERVER=") + server + ";";
		if (!port.empty()) conn_str += std::string("PORT=") + port + ";";
		if (!database.empty()) conn_str += std::string("DATABASE=") + database + ";";
		if (!user.empty()) conn_str += std::string("UID=") + user + ";";
		if (!password.empty()) conn_str += std::string("PWD=") + password + ";";
	}

	// ODBC Env and Connection
	OdbcHandle env(SQL_HANDLE_ENV);
	if (!odbc_check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env.handle), SQL_HANDLE_ENV, env.handle, "SQLAllocHandle ENV")) return 1;
	if (!odbc_check(SQLSetEnvAttr(env.handle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0), SQL_HANDLE_ENV, env.handle, "SQLSetEnvAttr ODBC3")) return 1;

	OdbcHandle dbc(SQL_HANDLE_DBC);
	if (!odbc_check(SQLAllocHandle(SQL_HANDLE_DBC, env.handle, &dbc.handle), SQL_HANDLE_DBC, dbc.handle, "SQLAllocHandle DBC")) return 1;

	SQLRETURN ret = SQLDriverConnectA(dbc.handle, NULL, (SQLCHAR*)conn_str.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
	if (!odbc_check(ret, SQL_HANDLE_DBC, dbc.handle, "SQLDriverConnect")) return 1;

	OdbcHandle stmt(SQL_HANDLE_STMT);
	if (!odbc_check(SQLAllocHandle(SQL_HANDLE_STMT, dbc.handle, &stmt.handle), SQL_HANDLE_STMT, stmt.handle, "SQLAllocHandle STMT")) return 1;

	std::string sql = build_sql(dialect);
	ret = SQLPrepareA(stmt.handle, (SQLCHAR*)sql.c_str(), SQL_NTS);
	if (!odbc_check(ret, SQL_HANDLE_STMT, stmt.handle, "SQLPrepare")) return 1;

	// Bind parameters: both are timestamps as strings
	SQLBindParameter(stmt.handle, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_TYPE_TIMESTAMP, 0, 0, (SQLPOINTER)start_ts.c_str(), 0, NULL);
	SQLBindParameter(stmt.handle, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_TYPE_TIMESTAMP, 0, 0, (SQLPOINTER)end_ts.c_str(), 0, NULL);

	ret = SQLExecute(stmt.handle);
	if (!odbc_check(ret, SQL_HANDLE_STMT, stmt.handle, "SQLExecute")) return 1;

	// Fetch and print results as TSV
	SQLSMALLINT col_count = 0;
	SQLNumResultCols(stmt.handle, &col_count);
	std::vector<SQLCHAR> buf(1024);

	// Print header
	for (SQLSMALLINT i = 1; i <= col_count; ++i) {
		SQLCHAR name[128]; SQLSMALLINT name_len = 0; SQLSMALLINT data_type = 0; SQLULEN col_size = 0; SQLSMALLINT dec_digits = 0; SQLSMALLINT nullable = 0;
		SQLDescribeColA(stmt.handle, i, name, sizeof(name), &name_len, &data_type, &col_size, &dec_digits, &nullable);
		std::cout << name;
		if (i < col_count) std::cout << "\t";
	}
	std::cout << "\n";

	while ((ret = SQLFetch(stmt.handle)) != SQL_NO_DATA) {
		for (SQLSMALLINT i = 1; i <= col_count; ++i) {
			SQLLEN ind = 0;
			ret = SQLGetData(stmt.handle, i, SQL_C_CHAR, buf.data(), (SQLLEN)buf.size(), &ind);
			if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
				if (ind == SQL_NULL_DATA) std::cout << "";
				else std::cout << (const char*)buf.data();
			} else {
				std::cout << "";
			}
			if (i < col_count) std::cout << "\t";
		}
		std::cout << "\n";
	}

	return 0;
} 