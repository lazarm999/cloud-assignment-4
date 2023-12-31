#include "utils.h"
#include "CurlEasyPtr.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <cstring>
#include <string>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <regex>
#include <unordered_map>
#include <vector>
#include <functional>

int ConnectToServer(const char* host, const char* port) {
   // returns socket descriptor connected with process at (host, port) or -1 in case of failure
   int rc;

   addrinfo hints, *listp, *p;  
   std::memset(&hints, 0, sizeof(addrinfo));
   hints.ai_family = AF_UNSPEC;
   hints.ai_socktype = SOCK_STREAM;
   if ((rc = getaddrinfo(host, port, &hints, &listp)) != 0) {
      std::cerr << "getaddrinfo error: " << gai_strerror(rc) << std::endl;
      exit(1); 
   }

   int clientsd;

   for (p = listp; p != nullptr; p = p->ai_next) {
      if ((clientsd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) continue;
      if (connect(clientsd, p->ai_addr, p->ai_addrlen) != -1) break; // success
      close(clientsd);
   }

   freeaddrinfo(listp);
   if (!p) return -1; // p is nullptr, all connections failed

   return clientsd;
}

std::string ExtractDomain(std::string& url) {
   auto urlPtr = url.data();
   auto limit = urlPtr + url.size();
   urlPtr = std::strstr(urlPtr, "://");
   if (!urlPtr) return url;
   urlPtr += 3;
   auto end = urlPtr;
   for (; end < limit && (*end) != '/'; ++end);
   return std::string(urlPtr, static_cast<size_t>(end-urlPtr));
}

std::unordered_map<std::string, unsigned> CountDomains(std::stringstream& csvData) {

   std::unordered_map<std::string, unsigned> domainCount;

   for (std::string row; std::getline(csvData, row, '\n');) {
      auto rowStream = std::stringstream(std::move(row));
      // Check the URL in the second column
      unsigned columnIndex = 0;
      for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
         // column 0 is id, 1 is URL
         if (columnIndex == 1) {
            auto domain = ExtractDomain(column);
            if (domainCount.find(domain) == domainCount.end()) {
               domainCount[domain] = 1;
            }
            else domainCount[domain]++;
            break;
         }
      }
   }
   return domainCount;
}

void WriteCountsLocal(std::string& fileno, std::vector<DCPair>& domainCount, unsigned bucketCnt) {
   auto outstreams = new std::ofstream[bucketCnt];
   std::hash<std::string> hashFunc {};
   for (auto& entry : domainCount) {
      auto hash = hashFunc(entry.domain) % bucketCnt;
      auto& outstream = outstreams[hash];
      if (!outstream.is_open()) {
         auto filename = "./data/aggr/bucket" + std::to_string(hash) + "." + fileno + ".csv";
         outstream.open(filename);
         if (!outstream) {std::cout << "File failed!";}
      }
      outstream << entry.domain << "\t" << entry.count << "\n";
   }
   for (auto i=0u; i<bucketCnt; ++i) {
      outstreams[i].close();
   }
}

std::string ExtractNumberFromBlobname(std::string& filename) {
   std::regex rgx(".*\\.([0-9]+)\\.csv");
   std::smatch match;
   std::regex_match(filename, match, rgx);
   if (match.size() < 2) return "";
   return match[1].str();
}

void WriteCounts(std::string& fileno, std::unordered_map<std::string, unsigned>& domainCount, unsigned bucketCnt) {
   auto uploadStreams = new std::stringstream[bucketCnt];
   std::hash<std::string> hashFunc {};
   for (auto& entry : domainCount) {
      auto bucketId = hashFunc(entry.first) % bucketCnt;
      auto& uploadStream = uploadStreams[bucketId];
      uploadStream << entry.first << "\t" << entry.second << "\n";
   }
   for (auto i = 0u; i < bucketCnt; ++i) {
      if (uploadStreams[i].rdbuf()->in_avail() == 0) continue;
      auto blobname = "aggr/bucket" + std::to_string(i) + "/" + fileno + ".csv";
      UploadStreamToAzure(blobname, uploadStreams[i]);
   }
   delete[] uploadStreams;
}

void UpdateCounts(std::unordered_map<std::string, unsigned>& counts, std::stringstream& csvData){
   for (std::string row; std::getline(csvData, row, '\n');) {
      std::stringstream rowAsStream(row);
      std::string domain;
      unsigned count=0;
      std::getline(rowAsStream, domain, '\t');
      rowAsStream >> count;
      if (counts.find(domain) == counts.end()) {
         counts[domain] = count;
      }
      else counts[domain] += count;
   }
}

void AggregateCounts(int bucketId) {
   // list all files with given prefix
   std::unordered_map<std::string, unsigned> counts;
   auto blobnamePrefix = "aggr/bucket" + std::to_string(bucketId) + "/";
   auto blobs = ListAzureBlobs(blobnamePrefix);
   for (auto& blob : blobs) {
      if (blob.find("top25") != blob.npos) continue;
      auto stream = DownloadStreamFromAzure(blob);
      UpdateCounts(counts, stream);
   }
   std::vector<DCPair> countAsVector;
   for (auto& pair : counts) {
      countAsVector.push_back({pair.first, pair.second});
   }
   std::sort(countAsVector.begin(), countAsVector.end(), [](const DCPair& a, const DCPair& b) {
      return a.count > b.count;
   });
   auto limit = countAsVector.size() < 25 ? countAsVector.size() : 25;
   std::stringstream outstream;
   for (auto i=0u; i<limit; ++i) {
      outstream << countAsVector[i].domain << "\t" << countAsVector[i].count << "\n";
   }
   auto outBlob = blobnamePrefix + "top25.csv";
   UploadStreamToAzure(outBlob, outstream);
   // for each file update counts
   // write final result to aggr/bucket{bucketId}/top25.csv
}

void AggregateCountsLocal(int bucketId) {
   // list all files with given prefix
   static const int PARTITION_NUM = 100;

   std::unordered_map<std::string, unsigned> counts;
   auto blobnamePrefix = "./data/aggr/bucket" + std::to_string(bucketId);
   std::ifstream in;
   for (int i=0; i<PARTITION_NUM; ++i) {
      std::string filename = blobnamePrefix + (i < 10 ? ".0" : ".") + std::to_string(i) + ".csv";
      in.open(filename);
      if (!in) {
         std::cerr << "open() failed!";
         in.close();
         continue;
      }
      std::stringstream ss;
      ss << in.rdbuf();
      UpdateCounts(counts, ss);
      in.close();
   }
   std::vector<DCPair> countAsVector;
   for (auto& pair : counts) {
      countAsVector.push_back({pair.first, pair.second});
   }
   std::sort(countAsVector.begin(), countAsVector.end(), [](const DCPair& a, const DCPair& b) {
      return a.count > b.count;
   });
   auto limit = countAsVector.size() < 25 ? countAsVector.size() : 25;
   std::ofstream outstream(blobnamePrefix + ".top25.csv");
   if (!outstream) return;
   for (auto i=0u; i<limit; ++i) {
      outstream << countAsVector[i].domain << "\t" << countAsVector[i].count << "\n";
   }
   // for each file update counts
   // write final result to data/totals/bucket{bucketId}.csv
}

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work

int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }
   // TODO:
   //    1. connect to coordinator specified by host and port
   //       getaddrinfo(), connect(), see: https://beej.us/guide/bgnet/html/#system-calls-or-bust
   //    2. receive work from coordinator
   //       recv(), matching the coordinator's send() work
   //    3. process work
   //       see coordinator.cpp
   //    4. report result
   //       send(), matching the coordinator's recv()
   //    5. repeat

   Task task;
   int load1 = 0, load2 = 0;
   sleep(1);
   auto start = std::chrono::steady_clock::now();

   int clientsd = ConnectToServer(argv[1], argv[2]);
   if (clientsd < 0) {
      exit(1); // unsuccessfull
   }

   auto pid = getpid();
   
   CurlGlobalSetup curlSetup;

   long msglen;
   while ((msglen = recv(clientsd, &task, sizeof(task), 0)) > 0) { // while not EOF(0) or error(-1)
      if (task.task_id == 1) {
         auto taskInfo = std::string(task.task_info.blobname);
         auto ss = DownloadStreamFromAzure(taskInfo);
         auto domainCount = CountDomains(ss);
         auto fileNo = ExtractNumberFromBlobname(taskInfo);
         if (fileNo.empty()) break;
         WriteCounts(fileNo, domainCount, task.bucket_cnt);
      }
      else if (task.task_id == 2) {
         // second task
         AggregateCounts(task.task_info.bucketId);
      }
      else break;
      
      // Send result
      int result = 1; // success
      send(clientsd, &result, sizeof(result), 0);
      if (task.task_id == 1) load1++;
      if (task.task_id == 2) load2++;
   }

   if (msglen < 0) { // error occurred
      std::cerr << pid << " recv() error: " << std::strerror(errno) << std::endl;
      exit(1);
   }
   
   auto end = std::chrono::steady_clock::now();
   
   std::cout << "Time: " << std::chrono::duration_cast<std::chrono::seconds>(end-start).count() << "s\n";
   std::cout << "Task 1 load: " << load1 << "\nTask 2 load: " << load2 << std::endl;

   return 0;
}