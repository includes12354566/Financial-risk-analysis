#include <windows.h>
#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <thread>

// 禁用不安全函数警告
#pragma warning(disable: 4996)

// 最简单的HTTP服务器实现
class MinimalHTTPServer {
private:
    SOCKET server_socket;
    int port;
    
public:
    MinimalHTTPServer(int p) : port(p), server_socket(INVALID_SOCKET) {}
    
    ~MinimalHTTPServer() {
        if (server_socket != INVALID_SOCKET) {
            closesocket(server_socket);
        }
        WSACleanup();
    }
    
    bool start() {
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            std::cerr << "WSAStartup failed" << std::endl;
            return false;
        }
        
        server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (server_socket == INVALID_SOCKET) {
            std::cerr << "Socket creation failed" << std::endl;
            return false;
        }
        
        sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port);
        
        if (bind(server_socket, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
            std::cerr << "Bind failed" << std::endl;
            return false;
        }
        
        if (listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
            std::cerr << "Listen failed" << std::endl;
            return false;
        }
        
        std::cout << "HTTP服务器启动在端口 " << port << std::endl;
        return true;
    }
    
    void run() {
        while (true) {
            sockaddr_in client_addr;
            int client_addr_len = sizeof(client_addr);
            SOCKET client_socket = accept(server_socket, (sockaddr*)&client_addr, &client_addr_len);
            
            if (client_socket == INVALID_SOCKET) {
                continue;
            }
            
            std::thread([this, client_socket]() {
                handleRequest(client_socket);
                closesocket(client_socket);
            }).detach();
        }
    }
    
private:
    void handleRequest(SOCKET client_socket) {
        char buffer[4096];
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) return;
        
        buffer[bytes_received] = '\0';
        std::string request(buffer);
        
        // 解析HTTP请求
        std::string method, path;
        size_t first_space = request.find(' ');
        size_t second_space = request.find(' ', first_space + 1);
        
        if (first_space != std::string::npos && second_space != std::string::npos) {
            method = request.substr(0, first_space);
            path = request.substr(first_space + 1, second_space - first_space - 1);
        }
        
        std::string response;
        if (path == "/health") {
            response = handleHealth();
        } else if (path == "/api/stats") {
            response = handleStats();
        } else if (path == "/api/risk-analysis" && method == "POST") {
            response = handleRiskAnalysis();
        } else {
            response = createErrorResponse("404", "Not Found");
        }
        
        send(client_socket, response.c_str(), response.length(), 0);
    }
    
    std::string handleHealth() {
        std::string json = "{\"status\":\"ok\",\"timestamp\":\"" + getCurrentTimestamp() + "\"}";
        return createJSONResponse(json);
    }
    
    std::string handleStats() {
        std::string json = "{";
        json += "\"total_accounts\":30000000,";
        json += "\"total_logins\":1000000000,";
        json += "\"total_transactions\":2000000000,";
        json += "\"large_transactions\":50000000,";
        json += "\"timestamp\":\"" + getCurrentTimestamp() + "\"";
        json += "}";
        return createJSONResponse(json);
    }
    
    std::string handleRiskAnalysis() {
        std::string json = "{";
        json += "\"status\":\"success\",";
        json += "\"query_time_ms\":1250,";
        json += "\"total_count\":3,";
        json += "\"time_range\":\"30d\",";
        json += "\"transactions\":[";
        
        // 生成3条模拟交易数据
        for (int i = 0; i < 3; i++) {
            if (i > 0) json += ",";
            json += "{";
            json += "\"transaction_id\":" + std::to_string(1000 + i) + ",";
            json += "\"amount\":" + std::to_string(80000 + i * 10000) + ",";
            json += "\"victim_name\":\"受害者" + std::to_string(i + 1) + "\",";
            json += "\"suspicious_name\":\"可疑账户" + std::to_string(i + 1) + "\",";
            json += "\"risk_level\":\"HIGH\"";
            json += "}";
        }
        
        json += "]";
        json += "}";
        
        return createJSONResponse(json);
    }
    
    std::string createJSONResponse(const std::string& json) {
        std::string response = "HTTP/1.1 200 OK\r\n";
        response += "Content-Type: application/json\r\n";
        response += "Access-Control-Allow-Origin: *\r\n";
        response += "Content-Length: " + std::to_string(json.length()) + "\r\n";
        response += "\r\n";
        response += json;
        return response;
    }
    
    std::string createErrorResponse(const std::string& code, const std::string& message) {
        std::string json = "{\"error\":\"" + message + "\"}";
        std::string response = "HTTP/1.1 " + code + " " + message + "\r\n";
        response += "Content-Type: application/json\r\n";
        response += "Content-Length: " + std::to_string(json.length()) + "\r\n";
        response += "\r\n";
        response += json;
        return response;
    }
    
    std::string getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        struct tm tm;
        localtime_s(&tm, &time_t);
        
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
};

int main(int argc, char* argv[]) {
    int server_port = 8080;
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            server_port = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "用法: " << argv[0] << " [选项]" << std::endl;
            std::cout << "选项:" << std::endl;
            std::cout << "  --port PORT        服务器端口 (默认: 8080)" << std::endl;
            std::cout << "  --help             显示帮助信息" << std::endl;
            return 0;
        }
    }
    
    MinimalHTTPServer server(server_port);
    
    if (!server.start()) {
        std::cerr << "服务器启动失败" << std::endl;
        return 1;
    }
    
    std::cout << "风险分析服务器启动成功" << std::endl;
    std::cout << "API端点:" << std::endl;
    std::cout << "  GET  /health - 健康检查" << std::endl;
    std::cout << "  POST /api/risk-analysis - 风险分析查询" << std::endl;
    std::cout << "  GET  /api/stats - 统计信息" << std::endl;
    std::cout << "访问地址: http://localhost:" << server_port << std::endl;
    
    server.run();
    
    return 0;
}





















