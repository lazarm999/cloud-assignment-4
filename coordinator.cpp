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
   static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYy8iLCJpYXQiOjE2NzA2ODc4NzQsIm5iZiI6MTY3MDY4Nzg3NCwiZXhwIjoxNjcwNjkyMzMzLCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBeTcrR3FFLy9aUzRqemFtb1Jtc0VOcnhGNGZrbkUrOGQvSUJjVEw3bnFmZkZEaVdvSHhPanBsb2dYbUErQ21PT1FkM2RjWThIZlFRU2dLTUlJK0FzekdKVW1wNVFHYituM2daYUFKbE9OZE09IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiU3RvamtvdmljIiwiZ2l2ZW5fbmFtZSI6IlVyb3MiLCJncm91cHMiOlsiMDhlZmQ5YjItN2ExMS00NjZmLWI5NTktMThiNmMzYzQzZjNiIiwiZDAxMmU3YzctYjBiYi00NGE0LTgzMjQtZTYwZDU3ZTg1OWEyIiwiNWYxNTdhZjAtNjIzOS00OTE1LTllYmEtNDQyMGFiMTI4OTBjIl0sImlwYWRkciI6IjE3Ni40LjI3LjE2MCIsIm5hbWUiOiJTdG9qa292aWMsIFVyb3MiLCJvaWQiOiI3OWNkYWVhOC03N2E4LTQ1NzctOWIyMS1kZjc0ZDBhZDFiZjAiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTQ5OTI2MTcyNy01NTE3NjEwMi0zNTI5NTA5OTI5LTEwMzk4NjEiLCJwdWlkIjoiMTAwMzIwMDI1NjZCODE3QiIsInJoIjoiMC5BWFFBNlVsN1hkSlEzRUM2c1JTaTJRTlVMSUdtQnVUVTg2aENrTGJDc0NsSmV2RjBBTjguIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiN3YxdDBYb2RMbDhZZ1Zhc0JiM0MyYnhCUnozX2dYazJleWQtQ1BRb1V2MCIsInRpZCI6IjVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYyIsInVuaXF1ZV9uYW1lIjoidXJvcy5zdG9qa292aWNAdHVtLmRlIiwidXBuIjoidXJvcy5zdG9qa292aWNAdHVtLmRlIiwidXRpIjoiNlNqd2laMFdRay1VdmI5T0FmVWZBUSIsInZlciI6IjEuMCJ9.LSPlYA9RZOeNseJQRu0hf0HNK36HQXdTewCuQXEHcBqv7PL6Mb6kp6ZKHWtvAddFGQ9x3Rfz0WUqixxPefiQqY0w3BeoYFZtt-_n4-BMGRIgFcddAew9JAUEF1hfQmQfF6SxBJ9PPmf5Q0AG5Qu-e7f7e71LG2m56A2Jo4zBl315ixwgTCFR0rUjxQZXO3aV3HyfJd3cRszyPc19AIqQgyS7SXkLQsW7loVu_jBJOX2J7PLbPoEcGT18S73sB9wrhOTdxpzpKa-zTLIMVJTgW5mJeerLFub8_q4RqNIdYy9V-OrRaAjVAI5HbcFkjcLEaqRmG4Y3c7nge1kXYagDAw";
   auto blobClient = AzureBlobClient(accountName, accountToken);

   std::cerr << "Creating Azure blob container" << std::endl;
   blobClient.createContainer("cbdp-assignment4");

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
