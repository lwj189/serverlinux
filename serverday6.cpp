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
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <vector>
#include <mysql/mysql.h>          // MySQL C API

// ---------- 配置 ----------
#define PORT 8888
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

// 数据库连接配置
const char* DB_HOST = "127.0.0.1";
const char* DB_USER = "root";
const char* DB_PASS = "yourpassword";  // 改成你的密码
const char* DB_NAME = "testdb";
unsigned int DB_PORT = 3306;

// ---------- HTTP 请求结构 ----------
struct HttpRequest {
    std::string method;
    std::string uri;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// ---------- HTTP 解析与响应生成（复用 Day3） ----------
bool parseRequest(const std::string& rawHeader, HttpRequest& req) {
    std::istringstream stream(rawHeader);
    std::string line;
    if (!std::getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    std::istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) return false;
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

// ---------- MySQL 连接池 ----------
class ConnectionPool {
public:
    ConnectionPool(size_t poolSize) {
        for (size_t i = 0; i < poolSize; ++i) {
            MYSQL* conn = mysql_init(nullptr);
            if (!conn) {
                std::cerr << "mysql_init failed" << std::endl;
                continue;
            }
            if (!mysql_real_connect(conn, DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT, nullptr, 0)) {
                std::cerr << "Connect failed: " << mysql_error(conn) << std::endl;
                mysql_close(conn);
                continue;
            }
            pool.push(conn);
        }
        if (pool.empty()) {
            std::cerr << "No database connections available. Exiting." << std::endl;
            exit(EXIT_FAILURE);
        }
        std::cout << "✅ Connection pool initialized with " << pool.size() << " connections." << std::endl;
    }

    MYSQL* getConnection() {
        std::unique_lock<std::mutex> lock(mtx);
        cond.wait(lock, [this] { return !pool.empty(); });
        MYSQL* conn = pool.front();
        pool.pop();
        return conn;
    }

    void returnConnection(MYSQL* conn) {
        if (!conn) return;
        std::unique_lock<std::mutex> lock(mtx);
        pool.push(conn);
        cond.notify_one();
    }

    ~ConnectionPool() {
        while (!pool.empty()) {
            mysql_close(pool.front());
            pool.pop();
        }
    }

private:
    std::queue<MYSQL*> pool;
    std::mutex mtx;
    std::condition_variable cond;
};

// ---------- 线程池（复用 Day5） ----------
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

// ---------- 全局变量 ----------
int epoll_fd_global;
ThreadPool* g_thread_pool = nullptr;
ConnectionPool* g_conn_pool = nullptr;

// ---------- 预处理语句示例：查询用户 ----------
bool queryUserByName(MYSQL* conn, const std::string& name, std::string& result) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        result = "mysql_stmt_init failed";
        return false;
    }

    const char* sql = "SELECT id, name, age FROM users WHERE name = ?";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        result = "Prepare failed: " + std::string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    // 绑定参数
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    char name_buf[256];
    strncpy(name_buf, name.c_str(), sizeof(name_buf) - 1);
    unsigned long name_len = name.length();
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = name_buf;
    bind[0].buffer_length = sizeof(name_buf);
    bind[0].length = &name_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        result = "Bind param failed: " + std::string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        result = "Execute failed: " + std::string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    // 绑定结果
    MYSQL_BIND result_bind[3];
    memset(result_bind, 0, sizeof(result_bind));
    int id;
    char name_out[256];
    int age;
    unsigned long id_len, name_len_out, age_len;
    my_bool is_null[3];

    result_bind[0].buffer_type = MYSQL_TYPE_LONG;
    result_bind[0].buffer = &id;
    result_bind[0].length = &id_len;
    result_bind[0].is_null = &is_null[0];

    result_bind[1].buffer_type = MYSQL_TYPE_STRING;
    result_bind[1].buffer = name_out;
    result_bind[1].buffer_length = sizeof(name_out);
    result_bind[1].length = &name_len_out;
    result_bind[1].is_null = &is_null[1];

    result_bind[2].buffer_type = MYSQL_TYPE_LONG;
    result_bind[2].buffer = &age;
    result_bind[2].length = &age_len;
    result_bind[2].is_null = &is_null[2];

    if (mysql_stmt_bind_result(stmt, result_bind) != 0) {
        result = "Bind result failed: " + std::string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    bool found = false;
    if (mysql_stmt_fetch(stmt) == 0) {
        found = true;
        std::ostringstream oss;
        oss << "ID: " << id << ", Name: " << name_out << ", Age: " << age;
        result = oss.str();
    }

    mysql_stmt_close(stmt);
    if (!found) {
        result = "User not found";
        return false;
    }
    return true;
}

// ---------- 预处理语句示例：插入用户 ----------
bool insertUser(MYSQL* conn, const std::string& name, int age) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    const char* sql = "INSERT INTO users (name, age) VALUES (?, ?)";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        std::cerr << "Prepare insert failed: " << mysql_stmt_error(stmt) << std::endl;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    char name_buf[256];
    strncpy(name_buf, name.c_str(), sizeof(name_buf) - 1);
    unsigned long name_len = name.length();
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = name_buf;
    bind[0].buffer_length = sizeof(name_buf);
    bind[0].length = &name_len;

    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &age;
    bind[1].is_null = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::cerr << "Bind insert failed: " << mysql_stmt_error(stmt) << std::endl;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::cerr << "Execute insert failed: " << mysql_stmt_error(stmt) << std::endl;
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    return true;
}

// ---------- 业务处理函数（工作线程执行） ----------
void handleBusiness(int fd, const HttpRequest& req) {
    // 1. 从连接池借一个连接
    MYSQL* conn = g_conn_pool->getConnection();

    std::string response_body;
    int status_code = 200;

    // 2. 根据路由处理
    if (req.method == "GET" && req.uri == "/") {
        response_body = "<h1>🏠 Day6 Home</h1><p>Use GET /users?name=xxx or POST /users (body: name=xxx&age=xx)</p>";
    }
    else if (req.method == "GET" && req.uri.find("/users") == 0) {
        // 解析查询参数 name=xxx
        std::string name;
        size_t q = req.uri.find('?');
        if (q != std::string::npos) {
            std::string query = req.uri.substr(q + 1);
            size_t eq = query.find('=');
            if (eq != std::string::npos && query.substr(0, eq) == "name") {
                name = query.substr(eq + 1);
            }
        }
        if (name.empty()) {
            status_code = 400;
            response_body = "<h1>400 Bad Request</h1><p>Missing name parameter</p>";
        } else {
            std::string result;
            bool ok = queryUserByName(conn, name, result);
            if (ok) {
                response_body = "<h1>User found</h1><p>" + result + "</p>";
            } else {
                status_code = 404;
                response_body = "<h1>404 Not Found</h1><p>" + result + "</p>";
            }
        }
    }
    else if (req.method == "POST" && req.uri == "/users") {
        // 解析 body 中的 name=xxx&age=xx
        std::string name;
        int age = 0;
        std::istringstream bodyStream(req.body);
        std::string pair;
        while (std::getline(bodyStream, pair, '&')) {
            size_t eq = pair.find('=');
            if (eq != std::string::npos) {
                std::string key = pair.substr(0, eq);
                std::string value = pair.substr(eq + 1);
                if (key == "name") name = value;
                else if (key == "age") age = std::stoi(value);
            }
        }
        if (name.empty() || age <= 0) {
            status_code = 400;
            response_body = "<h1>400 Bad Request</h1><p>Missing name or invalid age</p>";
        } else {
            if (insertUser(conn, name, age)) {
                response_body = "<h1>User inserted successfully</h1>";
            } else {
                status_code = 500;
                response_body = "<h1>500 Internal Server Error</h1><p>Database insert failed</p>";
            }
        }
    }
    else {
        status_code = 404;
        response_body = "<h1>404 Not Found</h1>";
    }

    // 3. 归还连接（一定要归还！）
    g_conn_pool->returnConnection(conn);

    // 4. 生成响应并发送
    std::string response = buildResponse(status_code, response_body);
    send(fd, response.c_str(), response.size(), 0);

    // 5. 清理 socket
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    std::cout << "✅ Task completed on fd=" << fd << std::endl;
}

// ---------- 主线程读事件处理（快速解析，入队） ----------
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

    // 解析 Content-Length 获取 body
    size_t contentLen = 0;
    auto it = req.headers.find("Content-Length");
    if (it != req.headers.end()) {
        try { contentLen = std::stoul(it->second); } catch (...) { contentLen = 0; }
    }
    if (contentLen > 0 && raw_data.size() > header_end + 4) {
        req.body = raw_data.substr(header_end + 4, contentLen);
    }

    std::cout << "📩 Main thread parsed: " << req.method << " " << req.uri << ", enqueuing task..." << std::endl;

    // 将任务丢入线程池
    g_thread_pool->enqueue([fd, req] {
        handleBusiness(fd, req);
    });
}

// ---------- 主函数 ----------
int main() {
    signal(SIGPIPE, SIG_IGN);

    // 1. 创建连接池（10 个连接）
    ConnectionPool connPool(10);
    g_conn_pool = &connPool;

    // 2. 创建线程池（4 个工作线程）
    ThreadPool threadPool(4);
    g_thread_pool = &threadPool;

    // 3. 创建 socket、bind、listen（同之前）
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

    // 4. 设置非阻塞 & epoll
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    int epoll_fd = epoll_create1(0);
    epoll_fd_global = epoll_fd;

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "🚀 Day 6 Server (epoll + ThreadPool + MySQL Pool) running on port " << PORT << std::endl;
    std::cout << "📌 Test GET: curl 'http://127.0.0.1:8888/users?name=Alice'" << std::endl;
    std::cout << "📌 Test POST: curl -X POST -d 'name=Bob&age=25' http://127.0.0.1:8888/users" << std::endl;

    // 5. 主事件循环
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