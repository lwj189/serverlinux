#include <iostream>
#include <cstring>
#include <string>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8888  // 服务器监听的端口

int main() {
    // 忽略 SIGPIPE：客户端提前断开时 send 不会杀掉整个进程，而是返回 -1
    signal(SIGPIPE, SIG_IGN);

    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);
    
    // 1. 准备要返回给浏览器的 HTTP 响应内容
    // 注意：HTTP 协议的换行符是 \r\n，头部和正文中间必须有一个空行（即 \r\n\r\n）
    // body 单独存放，Content-Length 用代码计算，避免手数长度出错
    std::string body = "<html><body><h1>Hello, Socket Server!</h1><p>Day 2 Milestone Achieved!</p></body></html>";
    std::string http_response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n"
        "Connection: close\r\n"
        "\r\n" +
        body;

    // 2. 创建 Socket (TCP)
    // AF_INET: IPv4, SOCK_STREAM: TCP
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 3. 设置套接字选项 (这是一个极其重要的细节！)
    // SO_REUSEADDR 允许服务器在重启时复用端口，否则你关掉服务器后立刻重启会报 "Address already in use"
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // 4. 绑定 IP 和端口 (Bind)
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // 监听本机所有网卡 IP
    address.sin_port = htons(PORT);       // 转换为网络字节序

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 5. 进入监听状态 (Listen)
    // 第二个参数 3 是最大挂起连接数
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "🚀 Server is listening on port " << PORT << std::endl;
    std::cout << "🟢 Test with: curl http://127.0.0.1:" << PORT << std::endl;

    // 6. 进入主循环，循环接受客户端请求
    while (true) {
        std::cout << "⏳ Waiting for a client to connect... (阻塞在 accept 阶段)" << std::endl;

        // 阻塞在这里，等待客户端连接 (Accept)
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        // 打印连接进来的客户端 IP
        std::cout << "✅ Client connected from: " << inet_ntoa(address.sin_addr) << std::endl;

        // 7. 读取客户端发来的请求 (Read)
        char buffer[1024] = {0};
        int valread = read(new_socket, buffer, 1024);
        if (valread > 0) {
            std::cout << "📩 Received Request:\n" << buffer << std::endl;
        }

        // 8. 返回固定的 HTTP 响应 (Write/Send)
        // 将我们准备好的 HTTP 字符串发送给客户端
        ssize_t sent = send(new_socket, http_response.c_str(), http_response.length(), 0);
        if (sent < 0) {
            perror("send");  // 客户端断开等原因导致发送失败，记录错误但不终止服务器
        } else {
            std::cout << "📤 Sent HTTP 200 OK response (" << sent << " bytes)." << std::endl;
        }

        // 关闭当前客户端的连接 (因为 HTTP 是无状态的，处理完就断开)
        close(new_socket);
        
        std::cout << "----------------------------------------" << std::endl;
    }

    // 实际上这个程序会一直死循环运行，Ctrl+C 终止即可
    close(server_fd);
    return 0;
}