#include "AzureBlobClient.h"
#include "CurlEasyPtr.h"
#include "utils.h"

std::stringstream DownloadStreamWithCUrl(const std::string& url) {
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(url);
   return curl.performToStringStream();
}
std::stringstream DownloadStreamFromAzure(const std::string& blobname) {
   AzureBlobClient& blobClient = *AzureBlobClient::Instance();
   blobClient.setContainer("cbdp-assignment4");

   return blobClient.downloadStringStream(blobname);
}
void UploadStreamToAzure(const std::string blobname, std::stringstream& stream) {
   AzureBlobClient& blobClient = *AzureBlobClient::Instance();
   
   blobClient.setContainer("cbdp-assignment4");
   
   blobClient.uploadStringStream(blobname, stream);
}

std::vector<std::string> ListAzureBlobs(std::string& prefix) {
   AzureBlobClient& blobClient = *AzureBlobClient::Instance();
   blobClient.setContainer("cbdp-assignment4");
   return blobClient.listBlobs(prefix);
}