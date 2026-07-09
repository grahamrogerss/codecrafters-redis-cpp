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
  
  // bind to port 6379
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
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
