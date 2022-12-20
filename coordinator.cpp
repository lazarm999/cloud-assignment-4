#include "AzureBlobClient.h"
#include "CurlEasyPtr.h"
#include "utils.h"
#include <chrono>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <poll.h>
#include <unordered_map>
#include <algorithm>

using namespace std::literals;

const int LISTENQ = 128;
const int TIMEOUT = 60000; // in ms

struct client_descriptor {
   Task task;
   std::chrono::steady_clock::time_point last_seen;
   client_descriptor(Task& t) {
      task = t;
      last_seen = std::chrono::steady_clock::now();
   }
   client_descriptor() {
      task.task_id = 0;
      last_seen = std::chrono::steady_clock::now();
   }
};

int OpenListenSd(const char* port) {
   if (!port) return -1;
   int listensd, optval = 1, rv;
   addrinfo hints, *listp, *p;
   memset(&hints, 0, sizeof(hints));
   hints.ai_family = AF_UNSPEC;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_flags = AI_PASSIVE; //   | AI_ADDRCONFIG | AI_NUMERICSERV;
   if ((rv = getaddrinfo(NULL, port, &hints, &listp)) != 0) {
      std::cerr << "getaddrinfo error: " << gai_strerror(rv) << std::endl;
      exit(1);
   }

   for (p = listp; p != nullptr; p = p->ai_next) {
      if ((listensd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) continue;
      /* Eliminates "Address already in use" error from bind */
      setsockopt(listensd, SOL_SOCKET, SO_REUSEADDR, &optval , sizeof(int));
      if (bind(listensd, p->ai_addr, p->ai_addrlen) == 0) break; // success
      close(listensd);
   }
   freeaddrinfo(listp);
   if (!p) {
      return -1;
   }
   if (listen(listensd, LISTENQ) < 0) {
      close(listensd);
      return -1;
   }
   return listensd;
}

long SendTask(int connsd, std::vector<Task>& unassignedTasks, std::unordered_map<int, client_descriptor>& clients) {
   auto task = unassignedTasks.back();
   clients[connsd].task = task;

   ssize_t sentlen;
   if ((sentlen = send(connsd, &task, sizeof(task), 0)) <= 0) {
      //std::cout << "Here " << sentlen << std::endl;
      clients.erase(connsd);
      close(connsd);
      return -1l;
   }

   unassignedTasks.pop_back();
   clients[connsd].last_seen = std::chrono::steady_clock::now();
   return sentlen;
}

int RecvResult(int connsd, std::vector<Task>& unassignedTasks, std::unordered_map<int, client_descriptor>& clients) {
   int result;
   if (recv(connsd, &result, sizeof(result), 0) < 0) {
      auto task = clients[connsd].task;
      clients.erase(connsd);
      unassignedTasks.push_back(task);
      close(connsd);
      return -1;
   }
   clients[connsd] = {}; // resets the info structure

   return result;
}

int AddToPoll(pollfd* psds, int newsd, int& sd_count, int sd_size)
{
    if (sd_count == sd_size) {
        return 0;
    }

    memset(&psds[sd_count], 0, sizeof(pollfd));
    psds[sd_count].fd = newsd;
    psds[sd_count].events = POLLIN | POLLOUT; // Check ready-to-read and ready-to-write

    sd_count++;
    return 1;
}

// Remove an index from the set
void DelFromPollsds(pollfd* psds, int i, int& sd_count)
{
    // Copy the one from the end over this one
    psds[i] = psds[sd_count-1];
    sd_count--;
}

void AggregateLocal(std::vector<DCPair>& top25, unsigned bucketCnt) {
   // for each subtotal file
   std::ifstream in;
   for (auto i=0u; i<bucketCnt; ++i) {
      std::string filename = "./data/aggr/bucket" + std::to_string(i) + ".top25.csv";
      in.open(filename);
      if (!in) std::cout << "open() failed!\n";
      for (std::string row; std::getline(in, row, '\n');) {
         std::string domain;
         unsigned count;
         auto rowStream = std::stringstream(row);
         std::getline(rowStream, domain, '\t');
         rowStream >> count;
         top25.push_back({domain, 0ul, count});
      }
      in.close();
   }
   std::sort(top25.begin(), top25.end(), [](DCPair a, DCPair b) {
      return a.count > b.count;
   });
   if (top25.size() > 25) top25.resize(25);
   // for each domain, count
   // add pair to vector of strings
   // sort by count
   // resize to top25
}

void Aggregate(std::vector<DCPair>& top25, unsigned bucketCount) {
   // for each subtotal file
   for (auto i=0u; i<bucketCount; ++i) {
      std::string blobname = "aggr/bucket" + std::to_string(i) + "/top25.csv";
      auto in = DownloadStreamFromAzure(blobname);
      for (std::string row; std::getline(in, row, '\n');) {
         std::string domain;
         unsigned count;
         auto rowStream = std::stringstream(row);
         std::getline(rowStream, domain, '\t');
         rowStream >> count;
         top25.push_back({domain, 0ul, count});
      }
   }
   std::sort(top25.begin(), top25.end(), [](DCPair a, DCPair b) {
      return a.count > b.count;
   });
   if (top25.size() > 25) top25.resize(25);
   // for each domain, count
   // add pair to vector of strings
   // sort by count
   // resize to top25
}

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <listen port> <bucketNo>" << std::endl;
      return 1;
   }

   auto bucketCountL = std::strtoul(argv[2], nullptr, 10);
   if (bucketCountL == 0 || bucketCountL > UINT_MAX) {
      std::cerr << "Usage: " << argv[0] << " <listen port> <bucketNo>" << std::endl;
      return 1;
   }
   auto bucketCount = static_cast<unsigned>(bucketCountL);
   // std::cout << bucketCount << std::endl;
   
   // std::string blobname = "aggr/bucket";
   // auto& client = *AzureBlobClient::Instance();
   // auto blobs = client.listBlobs(blobname);
   // std::cout << blobs.size() << std::endl;
   // for (auto& blob : blobs) {
   //    client.deleteBlob(blob);
   // }
   // exit(0);
   
   // x % 30u != static_cast<unsigned long>(hash)
   // auto ss = DownloadStreamFromAzure(blobname);
   // std::cout << ss.rdbuf();

   CurlGlobalSetup curlSetup;

   auto listUrl = std::string("https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv");//std::string(argv[1]);

   // Download the file list
   //std::cout << fileList.str();
   // return 0;

   pollfd* psds = new pollfd[LISTENQ + 1];
   int listensd = OpenListenSd(argv[1]);

   psds[0].fd = listensd; psds[0].events = POLLIN;
   auto psdlen = 1;

   std::unordered_map<int, client_descriptor> clients;

   for (int taskId=1; taskId<=2; ++taskId) {
      auto remainingTasks = 0u;
      std::vector<Task> unassignedTasks;
      // Iterate over all files and mark them as unassigned
      if (taskId == 1) {
         auto fileList = DownloadStreamFromAzure("data/filelist.csv"); // DownloadStreamWithCUrl(listUrl);// 
         for (std::string url; std::getline(fileList, url, '\n');) {
            Task task;
            task.task_id = 1;
            task.bucket_cnt = bucketCount;
            strncpy(task.task_info.blobname, url.data(), TASK_INFO_SIZE);
            unassignedTasks.push_back(task);
            remainingTasks++;
            //if (!--n) break;
         }
         // std::cout << unassignedTasks.front().task_info.blobname << "\t" << unassignedTasks.back().task_info.blobname << "\t" << remainingTasks << std::endl;
      }
      else {
         for (auto i=0u; i<bucketCount; ++i) {
            Task task;
            task.task_id = 2;
            task.bucket_cnt = bucketCount;
            task.task_info.bucketId = i;
            unassignedTasks.push_back(task);
         }
         remainingTasks = bucketCount;
      }
      while (remainingTasks > 0) {
         int poll_count = poll(psds, psdlen, 1000);
         if (poll_count == -1) {
            perror("poll");
            exit(1);
         }
         else if (poll_count == 0) continue;
         for (int i = 0; i < psdlen; i++) { // for each polled socket
            if (psds[i].fd == listensd) {
               if ((psds[i].revents & POLLIN) && psdlen <= LISTENQ+1) {
                  sockaddr_storage clientaddr;
                  socklen_t clientlen;    
                  // accept connection with worker
                  //std::cout << "Here!";
                  int connsd = accept(listensd, (sockaddr*)&clientaddr, &clientlen);
                  // add socket descriptor to polling list
                  AddToPoll(psds, connsd, psdlen, LISTENQ+1);
                  clients[connsd] = {};
               }
            }
            else {
               auto connsd = psds[i].fd;
               auto now = std::chrono::steady_clock::now();
               if (psds[i].revents & POLLIN) {
                  auto result = RecvResult(connsd, unassignedTasks, clients);
                  if (result != -1) {
                     remainingTasks--;
                     //std::cout << "TaskId: " << taskId << "\tRemaining: " << remainingTasks << std::endl;
                     if (!remainingTasks) break;
                  }
               }
               else if ((psds[i].revents & POLLHUP) || std::chrono::duration_cast<std::chrono::milliseconds>(now - clients[connsd].last_seen).count() > TIMEOUT) {
                  //std::cout << "Here" << (psds[i].revents) << std::endl;
                  auto task = clients[connsd].task;
                  clients.erase(connsd);
                  if (!task.task_id) unassignedTasks.push_back(task);
                  DelFromPollsds(psds, i, psdlen);
                  close(connsd);
                  i--;
                  continue;
               }
               else if ((psds[i].revents & POLLOUT) && !clients[connsd].task.task_id && !unassignedTasks.empty()) {
                  // std::cout << "File: " << remainingTasks << ", unassigned len: " << unassignedTasks.size() << std::endl;
                  if (SendTask(connsd, unassignedTasks, clients) == -1) {
                     DelFromPollsds(psds, i, psdlen);
                  }
               }
            }
         }
      }
   }

   for (int i=0; i<psdlen; i++) close(psds[i].fd);

   std::vector<DCPair> top25;
   Aggregate(top25, bucketCount);

   //std::cout << "Top 25 domains:\n";
   for (auto& pair : top25) {
      std::cout << pair.domain << "\t" << pair.count << "\n";
   }
   
   //close(listensd);
   delete[] psds;

   return 0;
}
