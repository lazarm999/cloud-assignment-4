# Managing Shared State for Distributed Query Execution

In this assignment, we implement a distributed query execution with shared state.
Feel free to reuse anything of the last assignment.

We start again with the same partitioned data on external storage and similar elasticity goals.
However, we now process a query that needs to share state between workers.
As a query, we want to calculate how often each domain appeared (not only a specific domain as in the last
assignment), and report the result for the top 25 domains.

For each input partition, we now build multiple (partial) aggregates, one for each domain, which we then need to merge.
Merging on the coordinator, as we did in the last assignment, does not scale well for this query.
To scale the merge phase, we partition the aggregates and store them in shared state.
After the initial aggregation and partitioning, we distribute the work of merging the partial aggregate partitions, and
send the merged results to the coordinator.

As an example, consider the initial `filelist.csv` with three partitions:

```
test.csv.1
test.csv.2
test.csv.3
```

Each worker takes a partition, partitions and aggregates it, and stores the aggregate in shared state files.
E.g., we partition the aggregates each into three partitions:

```
aggregated.1.test.csv.1
aggregated.2.test.csv.1
aggregated.3.test.csv.1
aggregated.1.test.csv.2
aggregated.2.test.csv.2
aggregated.3.test.csv.2
aggregated.1.test.csv.3
aggregated.2.test.csv.3
aggregated.3.test.csv.3
```

Now each of the workers takes one aggregate partition, e.g.:

```
aggregated.1.test.csv.1
aggregated.1.test.csv.2
aggregated.1.test.csv.3
```

For this aggregate partition, we can now calculate a total result and get the top 25 `top25.1.csv`, which we again store
in shared state.
Afterwards, the coordinator can collect the small results, calculate, and print the overall top 25.

You can see a detailed diagram of the query execution stages [here](screenshots/diagram.png)

## Shared State

To test data stored in shared state locally, you can use files on your file system
([`std::fstream`](https://en.cppreference.com/w/cpp/io/basic_fstream)).
To share state in Azure, you can use the [Azure Storage Library](https://github.com/Azure/azure-storage-cpplite).
Our scaffold includes a small example how to use the Azure blob storage (similar to S3).

For Azure storage, you also need
to [create a storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-cli):

```
az storage account create --name "cbdp$RANDOM" --resource-group cbdp-resourcegroup --location westeurope
```

You also might need to add the `Storage Blob Data Contributor` role assignment to that storage account through the Azure
web interface.

## Report Questions


### Part 1: Azure Deployment and Testing

1. After you have finished with the Azure tutorial, measure the time it takes for the Assignment 3 query to run on Azure. What do you notice?

2. Go to the Azure monitoring panel for your containers: explain what is the bottleneck that increases query execution time. Include screenshots if needed.

3. Let’s get faster: Pre-upload the data partitions and fileList inside Azure blob storage. Adapt your solution to read from there. What is the speedup you observe? How is it explained? 

NOTE: You can use the Assignment 3 repository for this section, or a different branch in this repository.

### Part 2: Managing Shared State - Design Questions

1. Give a brief description of your solution.

2. What was the query execution time in Azure? Include screenshots, logs if needed.

3. Which partitioning method did you choose for the calculation of the partial aggregates? Why?

4. What number of subpartitions did you choose, and why? If you choose to create 100 subpartitions for each partition, is it a good choice? Is there a scalability limit?

5. How does the coordinator know which partitions to send to the workers for the merging phase?

6. How do workers differentiate from the two tasks they have to perform (partial aggregation, merging subpartitions)?

7. Give a brief sequence diagram to show how the coordinator and workers communicate. Add some details about the communication format.

### Part 3: Notes

If we need some prerequisites to run your solution, please include them here. 

Also include a brief description of your unit tests.

## Execution

Install dependencies. Slightly more due than last time due to the Azure library.

```
sudo apt install cmake g++ libcurl4-openssl-dev libssl-dev uuid-dev
```

## Submission:
You can submit everything via GitLab.
First fork this repository, and add all members of your group to a single repository.
Then, in this group repository, add:
* Names of all members of your group in groupMembers.txt
* Code that implements the assignment
* Test scripts that demonstrate the capabilities of your solution (correctness, elasticity, resilience)
* A written report giving a brief description of your implementation, and answering the design questions.


## Deploy to Azure:

In this assignment, please run your experiments on Microsoft Azure.
We have a [tutorial](AZURE_TUTORIAL.md) with detailed instructions on how to deploy and run your solution in Azure.