#include <iostream>
#include <cstring>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// ======= Day 5 新增：C++11 并发库头文件 =======
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <vector>

#define PORT 8888
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

// =========================== 1. 数据结构 ===========================
struct HttpRequest {
    std::string method;
    std::string uri;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// =========================== 2. 完整的解析器（从 Day 3 复制） ===========================
bool parseRequest(const std::string& rawHeader, HttpRequest& req) {
    std::istringstream stream(rawHeader);
    std::string line;

    // 解析请求行
    if (!std::getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back();

    std::istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) {
        return false;
    }

    // 解析请求头
    while (std::getline(stream, line)) {
        if (line == "\r" || line.empty()) break;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string key = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            size_t start = value.find_first_not_of(" ");
            if (start != std::string::npos) value = value.substr(start);
            req.headers[key] = value;
        }
    }
    return true;
}

// =========================== 3. 完整的响应生成器（从 Day 3 复制） ===========================
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
    response += "Connection: close\r\n";
    response += "\r\n";
    response += body;
    return response;
}

// =========================== 4. 线程池类 ===========================
class ThreadPool {
public:
    ThreadPool(size_t numThreads) : stop(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });
                        if (this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

// =========================== 5. 全局变量 ===========================
int epoll_fd_global;
ThreadPool* g_pool = nullptr;

// =========================== 6. 业务处理（在子线程中执行） ===========================
void handleBusiness(int fd, const HttpRequest& req) {
    // 模拟耗时业务（睡眠3秒）
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::string response;
    if (req.method == "GET" && req.uri == "/") {
        response = buildResponse(200, "<h1>🏠 Day5 Home</h1><p>Processed by thread: " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "</p>");
    } else if (req.method == "POST" && req.uri == "/submit") {
        response = buildResponse(200, "<h1>POST OK</h1><p>Received: " + req.body + " (handled by thread)</p>");
    } else {
        response = buildResponse(404, "<h1>404 Not Found</h1>");
    }

    send(fd, response.c_str(), response.size(), 0);
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    std::cout << "✅ Task completed on fd=" << fd << std::endl;
}

// =========================== 7. 处理客户端读事件（主线程执行） ===========================
void handleClientRead(int fd) {
    char buffer[BUFFER_SIZE];
    int n = read(fd, buffer, sizeof(buffer));

    if (n <= 0) {
        epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }

    std::string raw_data(buffer, n);
    size_t header_end = raw_data.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        // 数据不完整，直接关闭（简化处理）
        epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }

    HttpRequest req;
    std::string header_part = raw_data.substr(0, header_end + 4);
    if (!parseRequest(header_part, req)) {
        std::string resp = buildResponse(400, "<h1>400 Bad Request</h1>");
        send(fd, resp.c_str(), resp.size(), 0);
        epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }

    // 解析 Content-Length，提取 Body
    size_t contentLen = 0;
    auto it = req.headers.find("Content-Length");
    if (it != req.headers.end()) {
        try {
            contentLen = std::stoul(it->second);
        } catch (...) {
            contentLen = 0;
        }
    }
    if (contentLen > 0 && raw_data.size() > header_end + 4) {
        req.body = raw_data.substr(header_end + 4, contentLen);
    }

    std::cout << "📩 Main thread parsed: " << req.method << " " << req.uri << ", enqueuing task..." << std::endl;

    // 将任务丢入线程池
    g_pool->enqueue([fd, req] {
        handleBusiness(fd, req);
    });
}

// =========================== 8. 主函数 ===========================
int main() {
    signal(SIGPIPE, SIG_IGN);

    // 创建线程池
    ThreadPool pool(4);
    g_pool = &pool;

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);

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

    // 设置非阻塞 & epoll
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    int epoll_fd = epoll_create1(0);
    epoll_fd_global = epoll_fd;

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "🚀 Day 5 Server (epoll + ThreadPool) running on port " << PORT << std::endl;
    std::cout << "💡 Try: curl -X POST -d 'hello' http://127.0.0.1:" << PORT << "/submit" << std::endl;
    std::cout << "⏳ Main thread will NOT block. Processing logs appear after 3 seconds (simulated work)." << std::endl;

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                while (true) {
                    struct sockaddr_in client_addr;
                    socklen_t len = sizeof(client_addr);
                    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    int flags_c = fcntl(client_fd, F_GETFL, 0);
                    fcntl(client_fd, F_SETFL, flags_c | O_NONBLOCK);
                    ev.events = EPOLLIN | EPOLLRDHUP;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    std::cout << "✅ New client connected, fd=" << client_fd << std::endl;
                }
            } else {
                handleClientRead(fd);
            }
        }
    }

    close(server_fd);
    return 0;
}