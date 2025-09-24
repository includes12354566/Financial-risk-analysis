#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <chrono>
#include <mysql/mysql.h>
#include <json/json.h>
#include <httplib.h>
#include <iomanip>
#include <sstream>

using namespace httplib;

class RiskAnalysisServer {
private:
    MYSQL* mysql_conn;
    std::string db_host;
    std::string db_user;
    std::string db_password;
    std::string db_name;
    int db_port;
    
    struct RiskTransaction {
        long long transaction_id;
        std::string transaction_time;
        double amount;
        long long victim_account_id;
        std::string victim_name;
        std::string victim_phone;
        std::string victim_email;
        std::string victim_type;
        long long suspicious_account_id;
        std::string suspicious_name;
        std::string suspicious_phone;
        std::string suspicious_email;
        std::string suspicious_type;
        int metric_a;
        int metric_b;
        double metric_c;
        std::string risk_level;
        std::string description;
    };
    
public:
    RiskAnalysisServer(const std::string& host, const std::string& user, 
                      const std::string& password, const std::string& database, int port = 3306)
        : db_host(host), db_user(user), db_password(password), db_name(database), db_port(port) {
        mysql_conn = nullptr;
    }
    
    ~RiskAnalysisServer() {
        if (mysql_conn) {
            mysql_close(mysql_conn);
        }
    }
    
    bool connectDatabase() {
        mysql_conn = mysql_init(nullptr);
        if (!mysql_conn) {
            std::cerr << "MySQL初始化失败" << std::endl;
            return false;
        }
        
        if (!mysql_real_connect(mysql_conn, db_host.c_str(), db_user.c_str(), 
                               db_password.c_str(), db_name.c_str(), db_port, nullptr, 0)) {
            std::cerr << "数据库连接失败: " << mysql_error(mysql_conn) << std::endl;
            return false;
        }
        
        mysql_set_character_set(mysql_conn, "utf8mb4");
        std::cout << "数据库连接成功" << std::endl;
        return true;
    }
    
    std::string getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto tm = *std::localtime(&time_t);
        
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
    
    int getTimeRangeHours(const std::string& timeRange) {
        if (timeRange == "24h") return 24;
        if (timeRange == "3d") return 72;
        if (timeRange == "7d") return 168;
        if (timeRange == "30d") return 720;
        if (timeRange == "6m") return 4320;
        if (timeRange == "1y") return 8760;
        return 24;
    }
    
    std::vector<RiskTransaction> queryRiskTransactions(const std::string& timeRange, 
                                                      int minMetricA, int minMetricB, double maxMetricC) {
        std::vector<RiskTransaction> results;
        
        int hours = getTimeRangeHours(timeRange);
        
        // 构建查询SQL
        std::string sql = R"(
            WITH metric_a_data AS (
                SELECT 
                    t_out.sender_account_id,
                    COUNT(DISTINCT t_out.id) as metric_a_count
                FROM transactions t_out
                JOIN transactions t_in ON t_in.receiver_account_id = t_out.sender_account_id
                WHERE t_out.amount >= 50000
                  AND t_out.status = 'posted'
                  AND t_out.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND t_in.amount >= 50000
                  AND t_in.status = 'posted'
                  AND t_in.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND t_out.created_at >= t_in.created_at
                  AND t_out.created_at <= DATE_ADD(t_in.created_at, INTERVAL 2 MINUTE)
                GROUP BY t_out.sender_account_id
            ),
            metric_b_data AS (
                SELECT 
                    t.sender_account_id,
                    COUNT(DISTINCT t.id) as metric_b_count
                FROM transactions t
                JOIN logins l ON l.account_id = t.sender_account_id
                WHERE t.amount >= 50000
                  AND t.status = 'posted'
                  AND t.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND l.login_at <= t.created_at
                  AND t.created_at <= DATE_ADD(l.login_at, INTERVAL 5 MINUTE)
                GROUP BY t.sender_account_id
            ),
            metric_c_data AS (
                SELECT 
                    t.receiver_account_id,
                    COALESCE(SUM(t.amount), 0) as metric_c_sum
                FROM transactions t
                WHERE t.status = 'posted'
                  AND t.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY t.receiver_account_id
            )
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
                COALESCE(ma.metric_a_count, 0) as metric_a,
                COALESCE(mb.metric_b_count, 0) as metric_b,
                COALESCE(mc.metric_c_sum, 0) as metric_c,
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
              AND t.created_at >= DATE_SUB(NOW(), INTERVAL )" + std::to_string(hours) + R"( HOUR)
              AND t.created_at < NOW()
              AND COALESCE(ma.metric_a_count, 0) >= )" + std::to_string(minMetricA) + R"(
              AND COALESCE(mb.metric_b_count, 0) >= )" + std::to_string(minMetricB) + R"(
              AND COALESCE(mc.metric_c_sum, 0) <= )" + std::to_string(maxMetricC) + R"(
            ORDER BY t.created_at DESC, t.amount DESC
            LIMIT 1000
        )";
        
        if (mysql_query(mysql_conn, sql.c_str())) {
            std::cerr << "查询失败: " << mysql_error(mysql_conn) << std::endl;
            return results;
        }
        
        MYSQL_RES* result = mysql_store_result(mysql_conn);
        if (!result) {
            std::cerr << "获取结果失败: " << mysql_error(mysql_conn) << std::endl;
            return results;
        }
        
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            RiskTransaction rt;
            rt.transaction_id = std::stoll(row[0] ? row[0] : "0");
            rt.transaction_time = row[1] ? row[1] : "";
            rt.amount = std::stod(row[2] ? row[2] : "0");
            rt.description = row[3] ? row[3] : "";
            rt.victim_account_id = std::stoll(row[4] ? row[4] : "0");
            rt.victim_name = row[5] ? row[5] : "";
            rt.victim_phone = row[6] ? row[6] : "";
            rt.victim_email = row[7] ? row[7] : "";
            rt.victim_type = row[8] ? row[8] : "";
            rt.suspicious_account_id = std::stoll(row[9] ? row[9] : "0");
            rt.suspicious_name = row[10] ? row[10] : "";
            rt.suspicious_phone = row[11] ? row[11] : "";
            rt.suspicious_email = row[12] ? row[12] : "";
            rt.suspicious_type = row[13] ? row[13] : "";
            rt.metric_a = std::stoi(row[14] ? row[14] : "0");
            rt.metric_b = std::stoi(row[15] ? row[15] : "0");
            rt.metric_c = std::stod(row[16] ? row[16] : "0");
            rt.risk_level = row[17] ? row[17] : "";
            
            results.push_back(rt);
        }
        
        mysql_free_result(result);
        return results;
    }
    
    Json::Value riskTransactionToJson(const RiskTransaction& rt) {
        Json::Value json;
        json["transaction_id"] = rt.transaction_id;
        json["transaction_time"] = rt.transaction_time;
        json["amount"] = rt.amount;
        json["description"] = rt.description;
        
        Json::Value victim;
        victim["account_id"] = rt.victim_account_id;
        victim["name"] = rt.victim_name;
        victim["phone"] = rt.victim_phone;
        victim["email"] = rt.victim_email;
        victim["type"] = rt.victim_type;
        json["victim_account"] = victim;
        
        Json::Value suspicious;
        suspicious["account_id"] = rt.suspicious_account_id;
        suspicious["name"] = rt.suspicious_name;
        suspicious["phone"] = rt.suspicious_phone;
        suspicious["email"] = rt.suspicious_email;
        suspicious["type"] = rt.suspicious_type;
        json["suspicious_account"] = suspicious;
        
        Json::Value metrics;
        metrics["metric_a"] = rt.metric_a;
        metrics["metric_b"] = rt.metric_b;
        metrics["metric_c"] = rt.metric_c;
        json["risk_metrics"] = metrics;
        
        json["risk_level"] = rt.risk_level;
        
        return json;
    }
    
    void startServer(int port = 8080) {
        Server server;
        
        // 设置CORS头
        server.set_default_headers({
            {"Access-Control-Allow-Origin", "*"},
            {"Access-Control-Allow-Methods", "GET, POST, OPTIONS"},
            {"Access-Control-Allow-Headers", "Content-Type"}
        });
        
        // 健康检查接口
        server.Get("/health", [](const Request& req, Response& res) {
            Json::Value response;
            response["status"] = "ok";
            response["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            Json::StreamWriterBuilder builder;
            res.set_content(Json::writeString(builder, response), "application/json");
        });
        
        // 风险分析查询接口
        server.Post("/api/risk-analysis", [this](const Request& req, Response& res) {
            try {
                Json::Value request;
                Json::CharReaderBuilder builder;
                std::string errors;
                
                if (!Json::parseFromStream(builder, req.body, &request, &errors)) {
                    res.status = 400;
                    res.set_content("{\"error\": \"Invalid JSON\"}", "application/json");
                    return;
                }
                
                // 解析请求参数
                std::string timeRange = request.get("time_range", "24h").asString();
                int minMetricA = request.get("min_metric_a", 1).asInt();
                int minMetricB = request.get("min_metric_b", 1).asInt();
                double maxMetricC = request.get("max_metric_c", 0.0).asDouble();
                
                // 执行查询
                auto start = std::chrono::high_resolution_clock::now();
                auto transactions = queryRiskTransactions(timeRange, minMetricA, minMetricB, maxMetricC);
                auto end = std::chrono::high_resolution_clock::now();
                
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                // 构建响应
                Json::Value response;
                response["status"] = "success";
                response["query_time_ms"] = duration.count();
                response["total_count"] = static_cast<int>(transactions.size());
                response["time_range"] = timeRange;
                response["criteria"] = Json::Value(Json::objectValue);
                response["criteria"]["min_metric_a"] = minMetricA;
                response["criteria"]["min_metric_b"] = minMetricB;
                response["criteria"]["max_metric_c"] = maxMetricC;
                
                Json::Value transactionsArray(Json::arrayValue);
                for (const auto& rt : transactions) {
                    transactionsArray.append(riskTransactionToJson(rt));
                }
                response["transactions"] = transactionsArray;
                
                Json::StreamWriterBuilder writerBuilder;
                res.set_content(Json::writeString(writerBuilder, response), "application/json");
                
            } catch (const std::exception& e) {
                res.status = 500;
                Json::Value error;
                error["error"] = e.what();
                Json::StreamWriterBuilder builder;
                res.set_content(Json::writeString(builder, error), "application/json");
            }
        });
        
        // 获取统计信息接口
        server.Get("/api/stats", [this](const Request& req, Response& res) {
            try {
                Json::Value stats;
                
                // 获取账户总数
                if (mysql_query(mysql_conn, "SELECT COUNT(*) FROM accounts")) {
                    throw std::runtime_error("查询账户总数失败");
                }
                MYSQL_RES* result = mysql_store_result(mysql_conn);
                if (result) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (row) {
                        stats["total_accounts"] = std::stoll(row[0] ? row[0] : "0");
                    }
                    mysql_free_result(result);
                }
                
                // 获取登录记录总数
                if (mysql_query(mysql_conn, "SELECT COUNT(*) FROM logins")) {
                    throw std::runtime_error("查询登录记录总数失败");
                }
                result = mysql_store_result(mysql_conn);
                if (result) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (row) {
                        stats["total_logins"] = std::stoll(row[0] ? row[0] : "0");
                    }
                    mysql_free_result(result);
                }
                
                // 获取转账记录总数
                if (mysql_query(mysql_conn, "SELECT COUNT(*) FROM transactions")) {
                    throw std::runtime_error("查询转账记录总数失败");
                }
                result = mysql_store_result(mysql_conn);
                if (result) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (row) {
                        stats["total_transactions"] = std::stoll(row[0] ? row[0] : "0");
                    }
                    mysql_free_result(result);
                }
                
                // 获取大额交易数量
                if (mysql_query(mysql_conn, "SELECT COUNT(*) FROM transactions WHERE amount >= 50000")) {
                    throw std::runtime_error("查询大额交易数量失败");
                }
                result = mysql_store_result(mysql_conn);
                if (result) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    if (row) {
                        stats["large_transactions"] = std::stoll(row[0] ? row[0] : "0");
                    }
                    mysql_free_result(result);
                }
                
                stats["timestamp"] = getCurrentTimestamp();
                
                Json::StreamWriterBuilder builder;
                res.set_content(Json::writeString(builder, stats), "application/json");
                
            } catch (const std::exception& e) {
                res.status = 500;
                Json::Value error;
                error["error"] = e.what();
                Json::StreamWriterBuilder builder;
                res.set_content(Json::writeString(builder, error), "application/json");
            }
        });
        
        std::cout << "风险分析服务器启动在端口 " << port << std::endl;
        std::cout << "API端点:" << std::endl;
        std::cout << "  GET  /health - 健康检查" << std::endl;
        std::cout << "  POST /api/risk-analysis - 风险分析查询" << std::endl;
        std::cout << "  GET  /api/stats - 统计信息" << std::endl;
        
        if (!server.listen("0.0.0.0", port)) {
            std::cerr << "服务器启动失败" << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    std::string db_host = "localhost";
    std::string db_user = "root";
    std::string db_password = "password";
    std::string db_name = "risk_analysis_system";
    int db_port = 3306;
    int server_port = 8080;
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--db-host" && i + 1 < argc) {
            db_host = argv[++i];
        } else if (arg == "--db-user" && i + 1 < argc) {
            db_user = argv[++i];
        } else if (arg == "--db-password" && i + 1 < argc) {
            db_password = argv[++i];
        } else if (arg == "--db-name" && i + 1 < argc) {
            db_name = argv[++i];
        } else if (arg == "--db-port" && i + 1 < argc) {
            db_port = std::stoi(argv[++i]);
        } else if (arg == "--port" && i + 1 < argc) {
            server_port = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "用法: " << argv[0] << " [选项]" << std::endl;
            std::cout << "选项:" << std::endl;
            std::cout << "  --db-host HOST     数据库主机 (默认: localhost)" << std::endl;
            std::cout << "  --db-user USER     数据库用户名 (默认: root)" << std::endl;
            std::cout << "  --db-password PASS 数据库密码 (默认: password)" << std::endl;
            std::cout << "  --db-name NAME     数据库名 (默认: risk_analysis_system)" << std::endl;
            std::cout << "  --db-port PORT     数据库端口 (默认: 3306)" << std::endl;
            std::cout << "  --port PORT        服务器端口 (默认: 8080)" << std::endl;
            std::cout << "  --help             显示帮助信息" << std::endl;
            return 0;
        }
    }
    
    RiskAnalysisServer server(db_host, db_user, db_password, db_name, db_port);
    
    if (!server.connectDatabase()) {
        std::cerr << "无法连接到数据库" << std::endl;
        return 1;
    }
    
    server.startServer(server_port);
    
    return 0;
}











