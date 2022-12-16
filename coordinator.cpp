#include "AzureBlobClient.h"
#include <iostream>
#include <string>

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   // TODO: add your azure credentials, get them via:
   // az storage account list
   // az account get-access-token --resource https://storage.azure.com/ -o tsv --query accessToken
   static const std::string accountName = "cbdp3526stoyk";
   static const std::string accountToken = "GA1qkfnIPcFcipDUrlnhgHozYHqVal3ig860uWt92EPMJQjcT83lvny7H+1U9TwzYeGhl2c9c/DH+ASt9dZ/Yw==";
   auto blobClient = AzureBlobClient(accountName, accountToken);

   std::cerr << "Creating Azure blob container" << std::endl;
   blobClient.createContainer("test");

   std::cerr << "Uploading a blob" << std::endl;
   {
      std::stringstream upload;
      upload << "Hello World!" << std::endl;
      blobClient.uploadStringStream("hello", upload);
   }

   std::cerr << "Downloading the blob again" << std::endl;
   auto downloaded = blobClient.downloadStringStream("hello");

   std::cerr << "Recieved: " << downloaded.str() << std::endl;

   std::cerr << "Deleting the container" << std::endl;
   blobClient.deleteContainer();

   return 0;
}
