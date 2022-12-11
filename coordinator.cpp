#include "AzureBlobClient.h"
#include "CurlEasyPtr.h"
#include <iostream>
#include <string>

void uploadDataToAzure() {
   // TODO: add your azure credentials, get them via:
   // az storage account list
   // az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
   static const std::string accountName = "cbdp3526stoyk";
   static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYy8iLCJpYXQiOjE2NzA3NjMyMDgsIm5iZiI6MTY3MDc2MzIwOCwiZXhwIjoxNjcwNzY4MTc2LCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBT2d2bDc2cUdHSk5kUkUxT2U5SEpBTmVyMXRWZHNuZld3VHVDTmZpY1VBYm0zdjZZU3BlRjVVZEZ5M01SQWtTT3M3OVNGdFBKckc2cTA5ZzE5ZGRoeUl4Zk44Vmx0Nk5VTU9uQ2dGYTFqZVE9IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiU3RvamtvdmljIiwiZ2l2ZW5fbmFtZSI6IlVyb3MiLCJncm91cHMiOlsiMDhlZmQ5YjItN2ExMS00NjZmLWI5NTktMThiNmMzYzQzZjNiIiwiZDAxMmU3YzctYjBiYi00NGE0LTgzMjQtZTYwZDU3ZTg1OWEyIiwiNWYxNTdhZjAtNjIzOS00OTE1LTllYmEtNDQyMGFiMTI4OTBjIl0sImlwYWRkciI6IjE3Ni40LjI3LjE2MCIsIm5hbWUiOiJTdG9qa292aWMsIFVyb3MiLCJvaWQiOiI3OWNkYWVhOC03N2E4LTQ1NzctOWIyMS1kZjc0ZDBhZDFiZjAiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTQ5OTI2MTcyNy01NTE3NjEwMi0zNTI5NTA5OTI5LTEwMzk4NjEiLCJwdWlkIjoiMTAwMzIwMDI1NjZCODE3QiIsInJoIjoiMC5BWFFBNlVsN1hkSlEzRUM2c1JTaTJRTlVMSUdtQnVUVTg2aENrTGJDc0NsSmV2RjBBTjguIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiN3YxdDBYb2RMbDhZZ1Zhc0JiM0MyYnhCUnozX2dYazJleWQtQ1BRb1V2MCIsInRpZCI6IjVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYyIsInVuaXF1ZV9uYW1lIjoidXJvcy5zdG9qa292aWNAdHVtLmRlIiwidXBuIjoidXJvcy5zdG9qa292aWNAdHVtLmRlIiwidXRpIjoieEt5S2lSamJkVUtuV3FfQU1uVUZBQSIsInZlciI6IjEuMCJ9.HV-VbsXxfqt74KAPCcCj_1JRAEE-Xq-qugllTcyj1smFjykg9H5fvxl_N-iCf3H5b_rkncCRFqr4_1fQ4tzdnoqSmTjSl4qOs-1fU3wvlwUbytS7iFTqnlUxN1hWBbzGED3fnDXnhMhgkW36SXCjfTNJDhfZDP8jJoCyzDBdAGEJh2xpr_67ctYhJaU7FfGpSgvcj2Bxh9tZA9As7Exg34Piltm_BxdLFF7wQvhmCFamVaYtnhAvietqURY_Ccgr6DFGysUz3VDF0g9qqpUvn3YeY608ieKmti1m1j_iWNeuvCTyhwfr15iXudeH4tG_Lur_dFzClVcbTlMyt_GuzA";
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

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   uploadDataToAzure();

   return 0;
}
