#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <vector>
#include <cctype>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unordered_map>
#include <chrono>



struct RedisValue {
    std::string value;
    long long expires_at;
    bool has_expiry;
  };


// you need to declare functions outside the other function
long long current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();
  }


void handle_command(const std::vector<std::string>& parsed_elements, 
                    std::unordered_map<std::string, RedisValue>& map, 
                    int reply_fd,
                    const std::string& replid,
                    std::vector<int>& replica_fds,
                    const std::string& role) {
  if (parsed_elements.empty()) return;
  std::string command = parsed_elements[0];
  for (char &c : command) c = std::toupper(c);

  if (command == "SET" && parsed_elements.size() > 2) {
    if (parsed_elements.size() > 4) {
      std::string time_command = parsed_elements[3];
      for (char &c : time_command) c = std::toupper(static_cast<unsigned char>(c));
      long long time = stoi(parsed_elements[4]);
      if (time_command == "EX") time *= 1000;
      time = current_time_ms() + time;
      map[parsed_elements[1]] = RedisValue{parsed_elements[2], time, true};
    } else {
      map[parsed_elements[1]] = RedisValue{parsed_elements[2], 0, false};
    }
    
    if (reply_fd != -1) {
      write(reply_fd, "+OK\r\n", 5);
    }
  }
  else if (command == "GET" && parsed_elements.size() > 1) {
    std::string response;
    auto it = map.find(parsed_elements[1]);
    if (it != map.end() && !it->second.has_expiry) {
      std::string arg = it->second.value;
      response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    else if (it != map.end() && it->second.has_expiry) {
      if (it->second.expires_at < current_time_ms()) {
        map.erase(it);
        response = "$-1\r\n";
      }
      else { 
        std::string arg = it->second.value;
        response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
      }
    }
    else {
      response = "$-1\r\n";
    }
    if (reply_fd != -1) {
      write(reply_fd, response.c_str(), response.length());
    }
  }
  else if (command == "INFO" && parsed_elements.size() > 1) {
    std::string arg = parsed_elements[1];
    for (char &c : arg) c = std::toupper(c);
    if (arg == "REPLICATION") {
      std::string payload = "role:" + role + "\r\n";
      payload += "master_replid:" + replid + "\r\n";
      payload += "master_repl_offset:0\r\n";
      std::string response = "$" + std::to_string(payload.length()) + "\r\n" + payload + "\r\n";
      if (reply_fd != -1) write(reply_fd, response.c_str(), response.length());
    }
  }
  else if (command == "REPLCONF") {
    if (reply_fd != -1) write(reply_fd, "+OK\r\n", 5);
  }
  else if (command == "PSYNC") {
    std::string response = "+FULLRESYNC " + replid + " 0\r\n";
    if (reply_fd != -1) write(reply_fd, response.c_str(), response.length());

    const unsigned char empty_rdb_bytes[] = {
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 
        0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 
        0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 
        0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 
        0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 
        0xf1, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
    std::string empty_rdb(reinterpret_cast<const char*>(empty_rdb_bytes), sizeof(empty_rdb_bytes));
    std::string rdb_header = "$" + std::to_string(empty_rdb.length()) + "\r\n";

    if (reply_fd != -1) {
      write(reply_fd, rdb_header.c_str(), rdb_header.length());
      write(reply_fd, empty_rdb.c_str(), empty_rdb.length());
      replica_fds.push_back(reply_fd);
    }
  }
  else {
    if (reply_fd != -1) write(reply_fd, "+PONG\r\n", 7);
  }

  
}

// the two parameters argc and argv mena argument count and argument
// vector, which is an array of C style strings containing the commands
int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  // create a socket
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }

  // create a socket for the master
  int master_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (master_fd < 0) {
    std::cerr << "Failed to create master socket\n";
    return 1;
  }
  int reuse = 1;
  if (setsockopt(master_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  

  // default port address will be 6379
  int port_address = 6379;
  // default role will be master, else slave
  std::string role = "master";
  // replication id is 40 characters random int
  std::string replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  // default offset = 0
  int offset = 0;

  std::string master_host;
  std::string master_port;


  // go through the arguments that are passed in
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--port" && i + 1 < argc) {
      port_address = std::stoi(argv[i + 1]);
    }
    // a replica must simultaneously act like a server to the rest
    // of the world and a client to the master
    else if (std::string(argv[i]) == "--replicaof" && i + 1 < argc) {
      role = "slave";
      std::string replicaof_arg = argv[i + 1];
      size_t space_pos = replicaof_arg.find(' ');
      
      if (space_pos != std::string::npos) {
        master_host = replicaof_arg.substr(0, space_pos);
        master_port = replicaof_arg.substr(space_pos + 1);
      }
    }
  }

  // basic server bind to port
  struct sockaddr_in server_addr;
  // AF_INET chooses the language, which is IPv4 addresses
  server_addr.sin_family = AF_INET;
  // INADDR_ANY means "listen for incoming traffic on every IP address this computer owns"
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port_address);

  // bind assigns a local port and address to your own socket so others can find you
  // so that incoming traffic knows exactly where to go
  if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port\n";
    return 1;
  }
  
  // listen for connections
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  // this creates an array of 1024 pollfd structures, all initialized to 0. keeps track
  // of which client is currently active and waiting for attention.
  std::array<pollfd, 1024> polls{}; 
  // the master socket is the very first entry. assigning server_fd to .fd means keep an eye
  // on this socket. .events = POLLIN (POLLIN means pull input) means only wake up if there is
  // an input coming into the poll.
  polls[0] = pollfd{.fd = server_fd, .events = POLLIN};

  // creating a hashmap to store the stuff for get and set, the keys are strings
  // and the vlaeus are RedisValues
  std::unordered_map<std::string, RedisValue> map;

  // gotta use this data structure to keep track of the replicas for
  // propogation purposes
  std::vector<int> replica_fds;

  // keeps track of how many active entries there are in the array
  int pollsCount = 1;

  if (role == "slave") {
    // gotta connect to master
    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(std::stoi(master_port));

    if (master_host == "localhost") {
      master_host = "127.0.0.1";
    }
    // the replica socket is making a direct, outgoing call to the master, so you need the exact IP
    // so the inet_pton translates master_host into network binary and
    // inserts it into the sin_addr slot
    inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);

    // connect reaches out to a remote port to establish a link with someone else
    // this is for the replica to connect to the master port for updates
    connect(master_fd, reinterpret_cast<sockaddr*>(&master_addr), sizeof(master_addr));
    
    // the handshake between the replica and the master
    std::string response = "*1\r\n$4\r\nPING\r\n";
    // the fd stands for file descriptor. master_fd is an operating number that the
    // operating system assigns to keep track of the open connection
    // the c_str function translates std::str into a c style string
    write(master_fd, response.c_str(), response.length());
    std::array<char, 4096> buffer;
    int bytesRead = read(master_fd, buffer.data(), buffer.size());

    std::string port_str = std::to_string(port_address);
    // 3 distinct words, so you need the *3
    response = "*3\r\n";
    // first word, 8 long
    response += "$8\r\nREPLCONF\r\n";
    // second, 14 long
    response += "$14\r\nlistening-port\r\n";
    // lastly for you port
    response += "$" + std::to_string(port_str.length()) + "\r\n" + port_str + "\r\n";
    write(master_fd, response.c_str(), response.length());
    bytesRead = read(master_fd, buffer.data(), buffer.size());

    response = "*3\r\n";
    response += "$8\r\nREPLCONF\r\n";
    response += "$4\r\ncapa\r\n";
    response += "$6\r\npsync2\r\n";
    write(master_fd, response.c_str(), response.length());
    bytesRead = read(master_fd, buffer.data(), buffer.size());

    response = "*3\r\n";
    response += "$5\r\nPSYNC\r\n";
    response += "$1\r\n?\r\n";
    response += "$2\r\n-1\r\n";
    write(master_fd, response.c_str(), response.length());
    bytesRead = read(master_fd, buffer.data(), buffer.size());

    // 2. Read and discard the RDB file sent by the master
    std::string rdb_response;
    std::array<char, 4096> rdb_buffer;
    while (rdb_response.find("\r\n") == std::string::npos) {
      int n = read(master_fd, rdb_buffer.data(), rdb_buffer.size());
      if (n <= 0) break;
      rdb_response.append(rdb_buffer.data(), n);
    }

    size_t crlf_pos = rdb_response.find("\r\n");
    if (crlf_pos != std::string::npos && rdb_response[0] == '$') {
      int rdb_len = std::stoi(rdb_response.substr(1, crlf_pos - 1));
      int total_read = rdb_response.length() - (crlf_pos + 2);
      while (total_read < rdb_len) {
        int to_read = std::min(static_cast<size_t>(rdb_len - total_read), rdb_buffer.size());
        int n = read(master_fd, rdb_buffer.data(), to_read);
        if (n <= 0) break;
        total_read += n;
      }
    }

    // 3. NOW register master_fd for polling so it only receives clean RESP commands
    polls[pollsCount] = pollfd{.fd = master_fd, .events = POLLIN};
    ++pollsCount;
  }
  

  

  
  while (true) {
    // the -1 is a timeout value that means "wait indefinitely", the kernel pauses everything
    // here until something actually happens on one of the sockets that I registered
    if (poll(polls.data(), pollsCount, -1) > 0) {
      std::cout << "poll received\n";

      // goes through all the active polls
      for (int i = 0; i < pollsCount; ++i) {
        // checks the socket's revents (returned events) and if it isn't ready for reading 
        // (POLLIN) then it's skipped
        if (!(polls[i].revents & POLLIN)) {
          continue;
        }

        if (polls[i].fd == master_fd) {

        }

        // since the first slot is the server socket, this check determines that the activity
        // is on the server socket: it's a client trying to connect
        if (polls[i].fd == server_fd) {
          std::cout << "new client!\n";
          sockaddr_in client_addr{};
          int client_addr_len = sizeof(client_addr);

          if (const int client_fd = accept(
              server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), 
              reinterpret_cast<socklen_t *>(&client_addr_len)); client_fd >= 0) {
            polls[pollsCount] = pollfd{.fd = client_fd, .events = POLLIN};
            ++pollsCount;
          }
        } 
        else {
          std::cout << "new data!\n";

          std::array<char, 4096> buffer{};
          const auto bytesRead = read(polls[i].fd, buffer.data(), buffer.size());

          if (bytesRead <= 0) {
            std::cout << "client disconnected\n";
            close(polls[i].fd);
            std::swap(polls[i], polls[pollsCount - 1]);
            --pollsCount;
            --i;
            continue;
          }
          
          std::string request(buffer.data(), bytesRead);
          size_t cursor = 0;
          std::vector<std::string> parsed_elements;

          if (!request.empty() && request[cursor] == '*') {
            size_t first_crlf = request.find("\r\n", cursor);
            if (first_crlf != std::string::npos) {
              cursor = first_crlf + 2;
            }
          }

          while (cursor < request.length()){
            size_t len_crlf = request.find("\r\n", cursor);
            if (len_crlf == std::string::npos) break;

            std::string length_str = request.substr(cursor + 1, len_crlf - cursor - 1);

            try {
              int length = std::stoi(length_str);
              cursor = len_crlf + 2;
              std::string element = request.substr(cursor, length);
              parsed_elements.push_back(element);
              cursor = cursor + length + 2;
            } catch (const std::invalid_argument& e) {
              std::cout << "Invalid RESP format encountered.\n";
              break; 
            }
          }
          
          if (!parsed_elements.empty()) {
            std::string command = parsed_elements[0];
            for (char &c : command) c = std::toupper(c);

            int target_fd = (polls[i].fd == master_fd) ? -1 : polls[i].fd;
            handle_command(parsed_elements, map, target_fd, replid, replica_fds, role);

            if (command == "SET" && polls[i].fd != master_fd) {
              for (size_t j = 0; j < replica_fds.size(); ++j) {
                write(replica_fds[j], request.c_str(), request.length());
              }
            }
          }
        }
      }
    }
  }
  close(server_fd);
  return 0;
}
