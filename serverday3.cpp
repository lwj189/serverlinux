#include <iostream>
#include <cstring>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8888
#define BUFFER_SIZE 4096

// =========================== 1. 数据结构 ===========================
struct HttpRequest {
    std::string method;          // GET / POST
    std::string uri;             // /  /submit
    std::string version;         // HTTP/1.1
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// =========================== 2. 工具函数（处理TCP半包） ===========================

// 从 socket 读数据，直到遇到 "\r\n\r\n"（HTTP头部结束标志）
// 作用：解决“头部半包”问题 —— 如果客户端一次只发来一半的头部，这里会一直循环读，直到拼完整。
std::string readUntilDoubleCRLF(int sock) {
    std::string buffer;
    char ch;
    int n;
    
    // 逐字节读取（效率低但逻辑清晰，便于理解"半包"本质）
    while ((n = recv(sock, &ch, 1, 0)) > 0) {
        buffer.push_back(ch);
        // 检测末尾是否是 \r\n\r\n
        if (buffer.size() >= 4 &&
            buffer[buffer.size() - 4] == '\r' &&
            buffer[buffer.size() - 3] == '\n' &&
            buffer[buffer.size() - 2] == '\r' &&
            buffer[buffer.size() - 1] == '\n') 
            {
            return buffer;   // 找到了，返回完整头部
            }
    }
    return buffer; // 连接断开或出错，返回已读到的部分
}

// 从 socket 读取指定长度的 Body
// 作用：解决“Body半包”问题 —— 比如 Content-Length: 100，但第一次只读到了 50 字节，
//       这个函数会继续循环读，直到凑够 100 字节。
std::string readBody(int sock, size_t contentLength) {
    if (contentLength == 0) return "";
    
    std::string body;
    body.resize(contentLength);   // 提前开辟好空间
    size_t totalRead = 0;
    
    while (totalRead < contentLength) {
        ssize_t n = read(sock, &body[totalRead], contentLength - totalRead);
        if (n <= 0) {
            // 对端断开或出错，无法继续读
            break;
        }
        totalRead += n;
    }
    body.resize(totalRead); // 万一没读满，截断到实际大小（防止内存污染）
    return body;
}

// =========================== 3. HTTP 解析器 ===========================

bool parseRequest(const std::string& rawHeader, HttpRequest& req) {
    std::istringstream stream(rawHeader);
    std::string line;
    
    // 3.1 解析请求行 (Request Line)
    if (!std::getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back(); // 去掉 \r
    
    std::istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) {
        return false;
    }

    // 3.2 解析请求头 (Headers)
    while (std::getline(stream, line)) {
        // 遇到空行（或只有 \r），表示头部结束
        if (line == "\r" || line.empty()) break;
        
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string key = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            // 去掉值前面的空格 (如 "Content-Length: 100" -> "100")
            size_t start = value.find_first_not_of(" ");
            if (start != std::string::npos) value = value.substr(start);
            req.headers[key] = value;
        }
    }
    return true;
}

// =========================== 4. 响应生成器 ===========================

std::string buildResponse(int statusCode, const std::string& body) {
    std::string statusLine;
    switch (statusCode) {
        case 200: statusLine = "HTTP/1.1 200 OK\r\n"; break;
        case 400: statusLine = "HTTP/1.1 400 Bad Request\r\n"; break;
        case 404: statusLine = "HTTP/1.1 404 Not Found\r\n"; break;
        default:  statusLine = "HTTP/1.1 500 Internal Server Error\r\n"; break;
    }

    std::string response = statusLine;
    response += "Content-Type: text/html; charset=utf-8\r\n";
    response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    response += "Connection: close\r\n";  // 告诉客户端，发完就断开
    response += "\r\n";                  // 头体分隔空行
    response += body;
    return response;
}

// =========================== 5. 主函数 ===========================

int main() {
    // 忽略 SIGPIPE：防止客户端断开时 send() 触发信号导致进程终止
    signal(SIGPIPE, SIG_IGN);

    int server_fd, client_fd;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);
    
    // ---------- 5.1 创建、绑定、监听 (标准六步曲前三步) ----------
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    
    std::cout << "🚀 Day 3 Server running on port " << PORT << std::endl;
    std::cout << "📌 Test GET:  curl http://127.0.0.1:" << PORT << "/" << std::endl;
    std::cout << "📌 Test POST: curl -X POST -d \"hello\" http://127.0.0.1:" << PORT << "/submit" << std::endl;

    // ---------- 5.2 主循环：accept -> 解析 -> 路由 -> 回复 ----------
    while (true) {
        std::cout << "\n⏳ Waiting for client..." << std::endl;
        
        if ((client_fd = accept(server_fd, (struct sockaddr *)&address, &addrlen)) < 0) {
            perror("accept");
            continue; // 出错了继续等下一个，不退出
        }

        // --- 步骤 A: 读取请求头 (解决头部半包) ---
        std::string headerData = readUntilDoubleCRLF(client_fd);
        if (headerData.empty()) {
            close(client_fd);
            continue;
        }

        // --- 步骤 B: 解析请求头和请求行 ---
        HttpRequest req;
        if (!parseRequest(headerData, req)) {
            std::string errResp = buildResponse(400, "<h1>400 Bad Request</h1>");
            send(client_fd, errResp.c_str(), errResp.size(), 0);
            close(client_fd);
            continue;
        }

        // --- 步骤 C: 读取 Body (解决 Body 半包, 仅 POST 有) ---
        size_t contentLen = 0;
        auto it = req.headers.find("Content-Length");
        if (it != req.headers.end()) {
            try {
                contentLen = std::stoul(it->second);
                req.body = readBody(client_fd, contentLen);
            } catch (const std::exception& e) {
                // 如果 Content-Length 不是合法数字，返回 400
                std::string errResp = buildResponse(400, "<h1>400 Invalid Content-Length</h1>");
                send(client_fd, errResp.c_str(), errResp.size(), 0);
                close(client_fd);
                continue;
            }
        }

        // 打印日志（方便调试）
        std::cout << "📩 " << req.method << " " << req.uri << " (Body size: " << req.body.size() << ")" << std::endl;
        if (!req.body.empty()) {
            std::cout << "📦 Body content: " << req.body << std::endl;
        }

        // --- 步骤 D: 路由分发 (Router) ---
        std::string response;
        if (req.method == "GET" && req.uri == "/") {
            response = buildResponse(200, "<h1>🏠 Home Page</h1><p>Day 3 服务器跑通了！</p>");
        } 
        else if (req.method == "POST" && req.uri == "/submit") {
            // 核心验收标准：回显客户端发来的 body
            std::string echoMsg = "✅ Server received: " + req.body;
            response = buildResponse(200, "<h1>POST Success</h1><p>" + echoMsg + "</p>");
        } 
        else {
            response = buildResponse(404, "<h1>❌ 404 Not Found</h1><p>路径不存在，试试 GET / 或 POST /submit</p>");
        }

        // --- 步骤 E: 发送响应并关闭连接 ---
        send(client_fd, response.c_str(), response.size(), 0);
        close(client_fd);
        std::cout << "✅ Response sent, connection closed." << std::endl;
    }

    close(server_fd);
    return 0;
}