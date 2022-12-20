#include <sstream>
#include <string>
#include <vector>

// const unsigned MAX_PARTITIONS = 4;
const size_t TASK_INFO_SIZE = 128;

union TaskInfo {
   char blobname[TASK_INFO_SIZE];
   int bucketId;
};

struct Task {
   char task_id;
   unsigned bucket_cnt;
   TaskInfo task_info;
};

struct DCPair {
   std::string domain;
   std::size_t hash;
   unsigned count;
};

std::stringstream DownloadStreamWithCUrl(const std::string& url);

std::stringstream DownloadStreamFromAzure(const std::string& blobname);

void UploadStreamToAzure(const std::string blobname, std::stringstream& stream);

std::vector<std::string> ListAzureBlobs(std::string& prefix);