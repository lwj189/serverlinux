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

// =========================== 1. 数据结构（与 Day 3/4 一致） ===========================
struct HttpRequest {
    std::string method;
    std::string uri;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// 解析器和响应生成器（完全复用 Day 3，一字不改，为了节省篇幅这里省略具体实现，但你的代码里必须有）
// 假设 parseRequest 和 buildResponse 已经复制过来了（和 Day 3 完全一样）
bool parseRequest(const std::string& rawHeader, HttpRequest& req) { /* ... 同 Day3 ... */ return true; }
std::string buildResponse(int statusCode, const std::string& body) { /* ... 同 Day3 ... */ return ""; }

// =========================== 2. Day 5 核心：线程池类 ===========================
class ThreadPool {
public:
    // 构造函数：启动指定数量的线程
    ThreadPool(size_t numThreads) : stop(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        // 1. 加锁取任务
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        // 2. 如果队列为空且没停止，就阻塞在这里等待唤醒
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });
                        // 3. 如果停止且队列为空，退出线程
                        if (this->stop && this->tasks.empty()) return;
                        // 4. 取出任务
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    // 5. 执行任务（解锁后执行，不占用锁，提高并发）
                    task();
                }
            });
        }
    }

    // 添加任务（万能模板，可以接受任何可调用对象）
    template<class F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one(); // 唤醒一个等待的线程
    }

    // 析构函数：停止所有线程
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all(); // 唤醒所有线程，让他们退出
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

// =========================== 3. 全局变量（让 handleClientRead 能访问线程池） ===========================
int epoll_fd_global;
ThreadPool* g_pool = nullptr; // 为了简洁，用全局指针指向线程池

// =========================== 4. 处理业务逻辑（在子线程中执行） ===========================
void handleBusiness(int fd, const HttpRequest& req) {
    // 模拟耗时业务（比如查数据库、计算密集）：睡眠 3 秒
    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    // 生成响应（和 Day 3 路由一样）
    std::string response;
    if (req.method == "GET" && req.uri == "/") {
        response = buildResponse(200, "<h1>🏠 Day5 Home</h1><p>Processed by thread: " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "</p>");
    } else if (req.method == "POST" && req.uri == "/submit") {
        response = buildResponse(200, "<h1>POST OK</h1><p>Received: " + req.body + " (handled by thread)</p>");
    } else {
        response = buildResponse(404, "<h1>404 Not Found</h1>");
    }

    // 发送响应（在子线程中直接发送）
    send(fd, response.c_str(), response.size(), 0);
    
    // 清理资源（从 epoll 移除并关闭 fd）
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    
    std::cout << "✅ Task completed on fd=" << fd << std::endl;
}

// =========================== 5. 处理客户端读事件（主线程执行，极速解析，不阻塞） ===========================
void handleClientRead(int fd) {
    char buffer[BUFFER_SIZE];
    int n = read(fd, buffer, sizeof(buffer));
    
    if (n <= 0) {
        epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        return;
    }

    // 为了简化，假设这次读到的数据就是完整请求（实际生产需要像 Day4 那样拼缓冲区）
    // 但在 Day5 演示中，我们直接用简单逻辑，重点展示线程池。
    // 注意：真正的工程代码需要像 Day4 那样维护 per-client 缓冲区，这里为了聚焦线程池，暂时简化。
    std::string raw_data(buffer, n);
    size_t header_end = raw_data.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        // 数据不完整，直接断开（演示中省略复杂拼接，避免喧宾夺主）
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

    // 解析 Content-Length 获取 Body（演示简化）
    size_t contentLen = 0;
    auto it = req.headers.find("Content-Length");
    if (it != req.headers.end()) contentLen = std::stoul(it->second);
    if (contentLen > 0 && raw_data.size() > header_end + 4) {
        req.body = raw_data.substr(header_end + 4, contentLen);
    }

    std::cout << "📩 Main thread parsed: " << req.method << " " << req.uri << ", enqueuing task..." << std::endl;

    // *** Day 5 核心变化 ***：主线程不处理业务，不发送响应，直接丢给线程池！
    g_pool->enqueue([fd, req] {
        // 这个 lambda 会在某个工作线程中执行
        handleBusiness(fd, req);
    });
    
    // 主线程立刻返回，继续 epoll_wait，不阻塞！
}

// =========================== 6. 主函数 ===========================
int main() {
    signal(SIGPIPE, SIG_IGN);

    // ---------- 1. 创建线程池（启动 4 个后台厨师） ----------
    ThreadPool pool(4);
    g_pool = &pool;

    // ---------- 2. 创建 socket、bind、listen（和 Day 2 一样） ----------
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

    // ---------- 3. 设置非阻塞 & epoll ----------
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    int epoll_fd = epoll_create1(0);
    epoll_fd_global = epoll_fd; // 赋值给全局变量供子线程清理使用

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "🚀 Day 5 Server (epoll + ThreadPool) running on port " << PORT << std::endl;
    std::cout << "💡 Try: curl -X POST -d 'hello' http://127.0.0.1:" << PORT << "/submit" << std::endl;
    std::cout << "⏳ The main thread will NOT block. Processing logs will appear after 3 seconds (simulated work)." << std::endl;

    // ---------- 4. 主事件循环（只负责 accept 和 读/解析，不阻塞！） ----------
    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                // 接受新连接（和 Day 4 一样）
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
                // 已有客户端发来数据 -> 主线程快速解析，丢进线程池
                handleClientRead(fd);
            }
        }
    }

    close(server_fd);
    return 0;
}