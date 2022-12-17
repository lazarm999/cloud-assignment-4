#include "AzureBlobClient.h"

static const std::string accountName = "cbdp3526stoyk";
static const std::string accountToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzVkN2I0OWU5LTUwZDItNDBkYy1iYWIxLTE0YTJkOTAzNTQyYy8iLCJpYXQiOjE2NzA4NjExNDUsIm5iZiI6MTY3MDg2MTE0NSwiZXhwIjoxNjcwODY1MzAwLCJhY3IiOiIxIiwiYWlvIjoiQVZRQXEvOFRBQUFBMWNiSzlKdTAwQlp6MUpWTUkxenUwT0JOYWkyTEVGeEJqbkE2ZEhNY2ZKaUhVRUZ5c25XKzNNMkdjdjRNWW01bUJReFRiWTJrN1pzR3NPcDV6ekkwcEFIdlJoVkZQc05ubXErcTdRWTFDblk9IiwiYW1yIjpbInB3ZCIsIm1mYSJdLCJhcHBpZCI6IjA0YjA3Nzk1LThkZGItNDYxYS1iYmVlLTAyZjllMWJmN2I0NiIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiU3RvamtvdmljIiwiZ2l2ZW5fbmFtZSI6IlVyb3MiLCJncm91cHMiOlsiMDhlZmQ5YjItN2ExMS00NjZmLWI5NTktMThiNmMzYzQzZjNiIiwiZDAxMmU3YzctYjBiYi00NGE0LTgzMjQtZTYwZDU3ZTg1OWEyIiwiNWYxNTdhZjAtNjIzOS00OTE1LTllYmEtNDQyMGFiMTI4OTBjIl0sImlwYWRkciI6IjE3Ni40LjY4LjY4IiwibmFtZSI6IlN0b2prb3ZpYywgVXJvcyIsIm9pZCI6Ijc5Y2RhZWE4LTc3YTgtNDU3Ny05YjIxLWRmNzRkMGFkMWJmMCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0xNDk5MjYxNzI3LTU1MTc2MTAyLTM1Mjk1MDk5MjktMTAzOTg2MSIsInB1aWQiOiIxMDAzMjAwMjU2NkI4MTdCIiwicmgiOiIwLkFYUUE2VWw3WGRKUTNFQzZzUlNpMlFOVUxJR21CdVRVODZoQ2tMYkNzQ2xKZXZGMEFOOC4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiI3djF0MFhvZExsOFlnVmFzQmIzQzJieEJSejNfZ1hrMmV5ZC1DUFFvVXYwIiwidGlkIjoiNWQ3YjQ5ZTktNTBkMi00MGRjLWJhYjEtMTRhMmQ5MDM1NDJjIiwidW5pcXVlX25hbWUiOiJ1cm9zLnN0b2prb3ZpY0B0dW0uZGUiLCJ1cG4iOiJ1cm9zLnN0b2prb3ZpY0B0dW0uZGUiLCJ1dGkiOiJGamRhR0dEY3VFNnREUXVVQmVVNUFRIiwidmVyIjoiMS4wIn0.LvEQ2Pr-k1F1SEKSS8KD-F0HhkRbAL_onj9ihosZ9SDLd28t9qVPa6Xdx5DwpbAqp8TRc4xPRWVwsSL-v9WDAAW8BAGYmUUjlizCiY7-KA0_7DYnayX7xDzffc0LCRGUyH_FEhum2iaIaklow6-7470p08roiBYWCwFv6ZBBKG_w9jhhTu_eOh87NjWcvobcPrXWUFyjDfSh7PcB8jWVJqaaAQtPF7uRRrxEgdjq80VWJz7ZbAPN0T5Qi6nVhdSaEySKhi7XFUmZUeyvESb9wAnxED03XV7oFjcKuuDrQF9Fmr2fytE61baoqDn0uzHoSoqpDOWlkvt0pvSdOx0ZMA";
static const std::string accountKey = "GA1qkfnIPcFcipDUrlnhgHozYHqVal3ig860uWt92EPMJQjcT83lvny7H+1U9TwzYeGhl2c9c/DH+ASt9dZ/Yw==";

AzureBlobClient* AzureBlobClient::instance = nullptr;

static std::string formatError(const azure::storage_lite::storage_error& error)
// Format an Azure storage error
{
   return "Error " + error.code + ": " + error.code_name + (error.message.empty() ? "" : ": ") + error.message;
}

azure::storage_lite::blob_client AzureBlobClient::createClient(const std::string& accountName, const std::string& accountKey)
// Create a container that stores all blobs
{
   using namespace azure::storage_lite;
   std::shared_ptr<storage_credential> cred = std::make_shared<shared_key_credential>(accountName, accountKey);
   std::shared_ptr<storage_account> account = std::make_shared<storage_account>(accountName, std::move(cred), /* use_https */ true);

   return {std::move(account), 16};
}

AzureBlobClient::AzureBlobClient(const std::string& accountName, const std::string& accountKey)
   : client(createClient(accountName, accountKey))
// Constructor
{
}

AzureBlobClient* AzureBlobClient::Instance()  {
      if (!AzureBlobClient::instance) {
         instance = new AzureBlobClient(accountName, accountKey);
         instance->setContainer("cbdp-assignment4");
      }
      return instance;
   }

void AzureBlobClient::createContainer(std::string containerName)
// Create a container that stores all blobs
{
   auto containerRequest = client.create_container(containerName).get();
   if (!containerRequest.success())
      throw std::runtime_error("Azure create container failed: " + formatError(containerRequest.error()));
   this->containerName = std::move(containerName);
}

void AzureBlobClient::deleteContainer()
// Delete the container that stored all blobs
{
   auto deleteRequest = client.delete_container(containerName).get();
   if (!deleteRequest.success())
      throw std::runtime_error("Azure delete container failed: " + formatError(deleteRequest.error()));
   this->containerName = {};
}

void AzureBlobClient::uploadStringStream(const std::string& blobName, std::stringstream& stream)
// Write a string stream to a blob
{
   auto uploadRequest = client.upload_block_blob_from_stream(containerName, blobName, stream, {}).get();
   if (!uploadRequest.success())
      throw std::runtime_error("Azure upload blob failed: " + formatError(uploadRequest.error()));
}

std::stringstream AzureBlobClient::downloadStringStream(const std::string& blobName)
// Read a string stream from a blob
{
   std::stringstream result;
   auto downloadRequest = client.download_blob_to_stream(containerName, blobName, 0, 0, result).get();
   if (!downloadRequest.success())
      throw std::runtime_error("Azure download blob failed: " + formatError(downloadRequest.error()));
   return result;
}

void AzureBlobClient::deleteBlob(const std::string& blobName) {
   auto deleteRequest = client.delete_blob(containerName, blobName).get();
   if (!deleteRequest.success())
      throw std::runtime_error("Azure delete blob failed: " + formatError(deleteRequest.error()));
}

std::vector<std::string> AzureBlobClient::listBlobs(const std::string& prefix = "")
// List all blobs in the container
{
   std::vector<std::string> results;
   std::string continuationToken;

   do {
      auto blobs = client.list_blobs_segmented(containerName, "", continuationToken, prefix).get();
      if (!blobs.success())
         throw std::runtime_error("Azure list blobs: " + formatError(blobs.error()));
      for (auto& blob : blobs.response().blobs)
         results.push_back(std::move(blob.name));
      continuationToken = std::move(blobs.response().next_marker);
   } while (!continuationToken.empty());

   return results;
}
