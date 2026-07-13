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

// you need to declare functions outside the other function
long long current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();
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
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct RedisValue {
    std::string value;
    long long expires_at;
    bool has_expiry;
  };

  // default port address will be 6379
  int port_address = 6379;
  // go through the arguments that are passed in
  for (int i = 1; i < argc - 1; ++i) {
    // you need this std::string() because otherwise it will just read
    // the memory address
    if (std::string(argv[i]) == "--port") {
      port_address = std::stoi(argv[i + 1]);
    }
  }

  // bind to port
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port_address);
  
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

  // keeps track of how many active entries there are in the array
  int pollsCount = 1;
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

        // since the first slot is the server socket, this check determines that the activity
        // is on the server socket: it's a client trying to connect
        if (i == 0) {
          std::cout << "new client!\n";
          sockaddr_in client_addr{};
          int client_addr_len = sizeof(client_addr);

          if (const int client_fd = accept(
              server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), 
              reinterpret_cast<socklen_t *>(&client_addr_len)); client_fd >= 0) {
            // this creates a new active socket in the polls array. it's a pollfd... it's a 
            // client_fd, and the event is a poll input
            polls[pollsCount] = pollfd{.fd = client_fd, .events = POLLIN};
            // because now there's one more active socket, so next time poll() is called it
            // knows to monitor this new client socket as well.
            ++pollsCount;
          }
        } else {
          std::cout << "new data!\n";

          // this is going to be standard, basically the buffer is temporary storage that exists
          // for you to dump your data into once you read it. 4096 is going to be the standard
          // buffer size for whatever new data is received.
          std::array<char, 4096> buffer{};
          const auto bytesRead = 
              // looks at the incoming data, copies that data into the buffer, and returns the 
              // number of bytes that it actually wrote
              read(polls[i].fd, buffer.data(), buffer.size());

          if (bytesRead <= 0) {
            std::cout << "client disconnected\n";
            close(polls[i].fd);
            std::swap(polls[i], polls[pollsCount - 1]);
            --pollsCount;
            --i;
            continue;
          }
          
          // this gets the data that's come in, initializes how far the cursor is
          std::cout << "received data from a client\n";
          std::string request(buffer.data(), bytesRead);
          size_t cursor = 0;
          std::vector<std::string> parsed_elements;

          // Skip the array header line (e.g., "*2\r\n") before entering the loop
          if (!request.empty() && request[cursor] == '*') {
            size_t first_crlf = request.find("\r\n", cursor);
            if (first_crlf != std::string::npos) {
              cursor = first_crlf + 2;
            }
          }

          //
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

            if (command == "ECHO" && parsed_elements.size() > 1) {
              std::string arg = parsed_elements[1];
              std::string response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";

              write(polls[i].fd, response.c_str(), response.length());
            }
            else if (command == "INFO" && parsed_elements.size() > 1) {
              std::string arg = parsed_elements[1];
              for (char &c : arg) c = std::toupper(c);
              if (arg == "REPLICATION") {
                std::string response = "$11\r\nrole:master\r\n";
                write(polls[i].fd, response.c_str(), response.length());
              }
              

            }
            else if (command == "SET" && parsed_elements.size() > 2) {
              // block for the expiry set read
              if (parsed_elements.size() > 4) {
                // need to convert to upper, do that with a loop
                std::string time_command = parsed_elements[3];
                for (char &c : time_command) {
                  c = std::toupper(static_cast<unsigned char>(c));
                }
                if (time_command == "EX") {
                  size_t time = stoi(parsed_elements[4]) * 1000;
                  time = current_time_ms() + time;
                  // because I'm initializing a struct here, I need curly braces not parenthesis
                  map[parsed_elements[1]] = RedisValue{parsed_elements[2], time, true};
                  
                }
                else if (time_command == "PX") {
                  size_t time = stoi(parsed_elements[4]);
                  time = current_time_ms() + time;
                  map[parsed_elements[1]] = RedisValue{parsed_elements[2], time, true};
                }
              }
              else {
                // the time variable doesn't exist in this block, so I hard code it to 0
                map[parsed_elements[1]] = RedisValue{parsed_elements[2], 0, false};
              }
              write(polls[i].fd, "+OK\r\n", 5);
            }
            else if (command == "GET" && parsed_elements.size() > 1) {
              // need to handle the case that the key doesn't exist
              std::string response;
              auto it = map.find(parsed_elements[1]);
              if (it != map.end() && !it->second.has_expiry) {
                std::string arg = it->second.value;
                response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
              }
              // "it" points to the map pair, so I need the it->second
              else if (it != map.end() && it->second.has_expiry) {
                if (it->second.expires_at < current_time_ms()) {
                  // erase "it" here in order to prevent memory leaks
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
              write(polls[i].fd, response.c_str(), response.length());
            }
            else {
              write(polls[i].fd, "+PONG\r\n", 7);
            }
          }
        }
      }
    }
  }


  
  // // first step to accepting a connection
  // struct sockaddr_in client_addr;
  // int client_addr_len = sizeof(client_addr);
  // std::cout << "Waiting for a client to connect...\n";

  // // You can use print statements as follows for debugging, they'll be visible when running tests.
  // std::cout << "Logs from your program will appear here!\n";

  // // accept a connection
  // int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&client_addr_len);
  // std::cout << "Client connected\n";


  // char buffer[1024];
  // // need to handle multiple commands. putting recv() and send() in a loop
  // // breaking out of the loop when client disconnects (when recv's return <= 0)
  // while (true) {
  //   // read the data
  //   int bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
  //   if (bytes_received <= 0) {
  //     break;
  //   }
  //   const char *response = "+PONG\r\n";
  //   // write the data
  //   send(client_fd, response, strlen(response), 0);
  // }

  // // now I need to figure out how to handle multiple concurrent clients
  // // using an event loop
  


  // // need to remember to close the client_fd
  // close(client_fd);
  close(server_fd);

  return 0;
}
