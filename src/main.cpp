#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
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
  
  if (bind(server_fd, reinterperet_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  // listen for connections
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  std::array<pollfd, 1024> polls{};
  polls[0] = pollfd{.fd = server_fd, .events = POLLIN}:

  int pollsCount = 1;
  while (true) {
    if (poll(polls.data, pollsCount, -1) > 0) {
      std::cout << "poll received\n";

      for (int i = 0; i < pollsCount; ++i) {
        if (!(polls[i].revents & POLLIN)) {
          continue;
        }

        if (i == 0) {
          std::cout << "new client!\n";
          sockaddr_in client_addr{};
          int client_addr_len = sizeof(client_addr);

          if (const int client_fd = accept(
              server_fd, reinterperet_cast<struct sockaddr *>(&client_addr), 
              reinterperet_cast<socklen_t*>(&client_addr_len)); client_fd >= 0) {
            polls[pollsCount] = pollfd{.fd = client_fd, .events = POLLIN};
            ++pollsCount;
          }
        } else {
          std::cout << "new data!\n";

          std::array<char, 4096> buffer{};
          const auto bytesRead = 
              read(polls[i].fd, buffer.data(), buffer.size());
          
          if (bytesRead >= 0) {
            std::cout << "received data from a client\n";
            write(polls[i].fd, "+PONG\r\n", 7);
          } else {
            std::cout << "client disconnected\n";
            std::swap(polls[i], polls[pollsCount - 1]);
            --pollsCount;
            --i;
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
