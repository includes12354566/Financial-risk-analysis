#include <windows.h>
#include <iostream>
#include <string>

#pragma warning(disable: 4996)

class SimpleServer {
private:
    SOCKET server_socket;
    int port;
    
public:
    SimpleServer(int p) : port(p), server_socket(INVALID_SOCKET) {}
    
    ~SimpleServer() {
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
        
        std::cout << "Server started on port " << port << std::endl;
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
            
            handleRequest(client_socket);
            closesocket(client_socket);
        }
    }
    
private:
    void handleRequest(SOCKET client_socket) {
        char buffer[4096];
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) return;
        
        buffer[bytes_received] = '\0';
        std::string request(buffer);
        
        std::string response;
        if (request.find("/health") != std::string::npos) {
            response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\"}";
        } else if (request.find("/api/stats") != std::string::npos) {
            response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"total_accounts\":30000000}";
        } else if (request.find("/api/risk-analysis") != std::string::npos) {
            response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"success\",\"total_count\":3}";
        } else {
            response = "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\r\n{\"error\":\"Not Found\"}";
        }
        
        send(client_socket, response.c_str(), response.length(), 0);
    }
};

int main(int argc, char* argv[]) {
    int server_port = 8080;
    
    if (argc > 2 && std::string(argv[1]) == "--port") {
        server_port = std::stoi(argv[2]);
    }
    
    SimpleServer server(server_port);
    
    if (!server.start()) {
        std::cerr << "Server startup failed" << std::endl;
        return 1;
    }
    
    std::cout << "Risk Analysis Server started successfully" << std::endl;
    std::cout << "Access URL: http://localhost:" << server_port << std::endl;
    
    server.run();
    
    return 0;
}





















