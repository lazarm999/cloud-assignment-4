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

void uploadDataToAzure() {
   // TODO: add your azure credentials, get them via:
   // az storage account list
   // az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
   AzureBlobClient& blobClient = *AzureBlobClient::Instance();

   //std::cerr << "Creating Azure blob container" << std::endl;
   blobClient.setContainer("cbdp-assignment4");

   // auto curlSetup = CurlGlobalSetup();
   // auto filelistUrl = std::string(argv[1]);

   // // Download the file list
   // auto curl = CurlEasyPtr::easyInit();

   // curl.setUrl(filelistUrl);
   // auto fileList = curl.performToStringStream();

   // std::cerr << "Uploading files" << std::endl;
   // std::stringstream transformed;
   // for (std::string file; std::getline(fileList, file, '\n');) {
   //    curl.setUrl(file);
   //    auto urlList = curl.performToStringStream();
   //    auto offset = file.find_last_of('/')+1;
   //    auto blobname = "data/" + file.substr(offset);
   //    transformed << blobname << "\n";
   //    blobClient.uploadStringStream(blobname, urlList);
   // }

   // blobClient.uploadStringStream("data/filelist.csv", transformed);

   // std::cerr << "Uploading a blob" << std::endl;
   // {
   //    std::stringstream upload;
   //    upload << "Hello World!" << std::endl;
   //    blobClient.uploadStringStream("hello", upload);
   // }

   // std::cerr << "Downloading the file list again" << std::endl;
   // auto downloaded = blobClient.downloadStringStream("data/filelist.csv");

   auto files = blobClient.listBlobs("data/");

   std::cerr << "Files:" << std::endl;
   for (auto& file : files) {
      std::cerr << file << std::endl;
   }

   //std::cerr << "Deleting the container" << std::endl;
   //blobClient.deleteContainer();
}