#include <sstream>
#include <string>

std::stringstream DownloadStreamWithCUrl(const std::string& url);

std::stringstream DownloadStreamFromAzure(const std::string& blobname);

void uploadDataToAzure();