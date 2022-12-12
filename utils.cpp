#include "AzureBlobClient.h"
#include "CurlEasyPtr.h"
#include "utils.h"

static const std::string accountName = "cbdp3526stoyk";
static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYy8iLCJpYXQiOjE2NzA4NjExNDUsIm5iZiI6MTY3MDg2MTE0NSwiZXhwIjoxNjcwODY1MzAwLCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBMWNiSzlKdTAwQlp6MUpWTUkxenUwT0JOYWkyTEVGeEJqbkE2ZEhNY2ZKaUhVRUZ5c25XKzNNMkdjdjRNWW01bUJReFRiWTJrN1pzR3NPcDV6ekkwcEFIdlJoVkZQc05ubXErcTdRWTFDblk9IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiU3RvamtvdmljIiwiZ2l2ZW5fbmFtZSI6IlVyb3MiLCJncm91cHMiOlsiMDhlZmQ5YjItN2ExMS00NjZmLWI5NTktMThiNmMzYzQzZjNiIiwiZDAxMmU3YzctYjBiYi00NGE0LTgzMjQtZTYwZDU3ZTg1OWEyIiwiNWYxNTdhZjAtNjIzOS00OTE1LTllYmEtNDQyMGFiMTI4OTBjIl0sImlwYWRkciI6IjE3Ni40LjY4LjY4IiwibmFtZSI6IlN0b2prb3ZpYywgVXJvcyIsIm9pZCI6Ijc5Y2RhZWE4LTc3YTgtNDU3Ny05YjIxLWRmNzRkMGFkMWJmMCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0xNDk5MjYxNzI3LTU1MTc2MTAyLTM1Mjk1MDk5MjktMTAzOTg2MSIsInB1aWQiOiIxMDAzMjAwMjU2NkI4MTdCIiwicmgiOiIwLkFYUUE2VWw3WGRKUTNFQzZzUlNpMlFOVUxJR21CdVRVODZoQ2tMYkNzQ2xKZXZGMEFOOC4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiI3djF0MFhvZExsOFlnVmFzQmIzQzJieEJSejNfZ1hrMmV5ZC1DUFFvVXYwIiwidGlkIjoiNWQ3YjQ5ZTktNTBkMi00MGRjLWJhYjEtMTRhMmQ5MDM1NDJjIiwidW5pcXVlX25hbWUiOiJ1cm9zLnN0b2prb3ZpY0B0dW0uZGUiLCJ1cG4iOiJ1cm9zLnN0b2prb3ZpY0B0dW0uZGUiLCJ1dGkiOiJGamRhR0dEY3VFNnREUXVVQmVVNUFRIiwidmVyIjoiMS4wIn0.LvEQ2Pr-k1F1SEKSS8KD-F0HhkRbAL_onj9ihosZ9SDLd28t9qVPa6Xdx5DwpbAqp8TRc4xPRWVwsSL-v9WDAAW8BAGYmUUjlizCiY7-KA0_7DYnayX7xDzffc0LCRGUyH_FEhum2iaIaklow6-7470p08roiBYWCwFv6ZBBKG_w9jhhTu_eOh87NjWcvobcPrXWUFyjDfSh7PcB8jWVJqaaAQtPF7uRRrxEgdjq80VWJz7ZbAPN0T5Qi6nVhdSaEySKhi7XFUmZUeyvESb9wAnxED03XV7oFjcKuuDrQF9Fmr2fytE61baoqDn0uzHoSoqpDOWlkvt0pvSdOx0ZMA";

std::stringstream DownloadStreamWithCUrl(const std::string& url) {
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(url);
   return curl.performToStringStream();
}
std::stringstream DownloadStreamFromAzure(const std::string& blobname) {
   auto blobClient = AzureBlobClient(accountName, accountToken);

   blobClient.setContainer("cbdp-assignment4");

   return blobClient.downloadStringStream(blobname);
}
void uploadDataToAzure() {
   // TODO: add your azure credentials, get them via:
   // az storage account list
   // az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
   auto blobClient = AzureBlobClient(accountName, accountToken);

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