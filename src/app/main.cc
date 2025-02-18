#include <iostream>
#include <string>
#include <csignal>

#include "../../include/method.h"
#include "../../include/messageQueue.h"
#include "../../include/datawrite.h"
using namespace std;

int main(int argc, char **argv)
{
    signal(SIGINT, tool::signalHandler);

    uint32_t chunkingType;
    uint32_t compressionMethod;
    uint32_t processNum;
    uint64_t ExchunkSize = -1;
    bool isDisk = false;
    string dirName;
    string myName = "Ftcg";

    vector<string> readfileList;

    const char optString[] = "i:m:c:n:s:d:";
    if (argc < sizeof(optString) - 4)
    {
        cout << "Usage: " << argv[0] << " -i <input file> -m <chunking method> -c <compression method> -n <process number> -d <isDisk>" << endl;
        cout << "Chunking Methods: " << "0 for fixed size chunking, 1 for FastCDC chunking" << endl;
        cout << "Compression Methods: " << "0 for lz4, 1 lz4-cluster-basline" << endl;
        return 0;
    }

    int option = 0;
    while ((option = getopt(argc, argv, optString)) != -1)
    {
        switch (option)
        {
        case 'i':
            dirName.assign(optarg);
            break;
        case 'c':
            chunkingType = atoi(optarg);
            break;
        case 'm':
            compressionMethod = atoi(optarg);
            break;
        case 'n':
            processNum = atoi(optarg);
            break;
        case 's':
            ExchunkSize = atoi(optarg);
            break;
        case 'd':
            isDisk = atoi(optarg);
            break;
        default:
            break;
        }
    }

    absMethod *absMethodObj;

    Chunker *chunkerObj = new Chunker(chunkingType);
    // MessageQueue<Chunk_t> *chunker2lz41 = new MessageQueue<Chunk_t>(CHUNK_QUEUE_SIZE);
    ProduceConsumerQueue<Chunk_t> *chunker2lz4 = new ProduceConsumerQueue<Chunk_t>(CHUNK_QUEUE_SIZE);
    chunkerObj->SetOutputMQ(chunker2lz4);

    switch (compressionMethod)
    {
    case 0:
        // lz4 comapre
        absMethodObj = new lz4Compare(); // perChunk compression
        break;
        // case 1:
        //     absMethodObj = new lz4Baseline(); // SFonly non FP
        //     break;
        // case 2:
        //     absMethodObj = new FPLz4Baseline(); // FP + SF
        //     break;
    case 1:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_SF(); // Dedup + HSF
        else
            absMethodObj = new Dedup_SF(ExchunkSize);
        break;
    case 2:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_HSF(); // Dedup + HSF
        else
            absMethodObj = new Dedup_HSF(ExchunkSize);
        break;
    case 3:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_HSFRank(); // Dedup + HSF
        else
            absMethodObj = new Dedup_HSFRank(ExchunkSize);
        break;
    case 4:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_HSFRank_BIB(); // Dedup + HSF
        else
            absMethodObj = new Dedup_HSFRank_BIB(ExchunkSize);
        break;
    // case 4:
    //     if (ExchunkSize == -1)
    //         absMethodObj = new FPLz4BaselineImplement(); // Dedup + tmpTest
    //     else
    //         absMethodObj = new FPLz4BaselineImplement(ExchunkSize);
    //     break;
    case 5:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_SF_BIB(); // Dedup + SF + BIB
        else
            absMethodObj = new Dedup_SF_BIB(ExchunkSize);
        break;
    // case 6:
    //     absMethodObj = new FPOnly(); // FPonly
    //     break;
    // case 7:
    //     absMethodObj = new bruteforce(); // BF
    //     break;
    case 6:
        if (ExchunkSize == -1)
            absMethodObj = new Dedup_HSH_BIB(); // Dedup + HSF + BIB
        else
            absMethodObj = new Dedup_HSH_BIB(ExchunkSize);
        break;
    case 7:
        if (ExchunkSize == -1)
            absMethodObj = new bruteforce();
        else
            absMethodObj = new bruteforce(ExchunkSize);
    default:
        break;
    }
    /*      absMethodObj init     */
    absMethodObj->dataWrite_ = new dataWrite();
    if (isDisk)
        absMethodObj->SetIsDisk(isDisk);

    tool::traverse_dir(dirName, readfileList, nofilter);
    sort(readfileList.begin(), readfileList.end(), tool::compareNat);

    boost::thread *thTmp;
    boost::thread::attributes attrs;
    attrs.set_stack_size(THREAD_STACK_SIZE);

    absMethodObj->SetInputMQ(chunker2lz4);

    tool::Logging(myName.c_str(), "Start to process %d files\n", processNum);
    auto startsum = std::chrono::high_resolution_clock::now();
    for (auto i = 0; i < processNum; i++)
    {
        chunkerObj->LoadChunkFile(readfileList[i]);
        thTmp = new boost::thread(attrs, boost::bind(&Chunker::Chunking, chunkerObj));
        if (i == processNum - 1)
        {
            absMethodObj->isLastFile = true;
        }
        absMethodObj->setFileName(readfileList[i]);
        absMethodObj->ProcessOneTrace();

        thTmp->join();
        delete thTmp;
    }
    auto endsum = std::chrono::high_resolution_clock::now();
    auto sumTime = (endsum - startsum);
    auto sumTimeInSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(endsum - startsum).count();
    std::cout << "Time taken by for loop: " << sumTimeInSeconds << " s " << std::endl;
    std::cout << "loop throughput is " << (double)absMethodObj->totalLogicalSize / (double)sumTimeInSeconds / (1 << 30) << "GiB/s" << std::endl;

    tool::Logging(myName.c_str(), "processNum %d \n", processNum);
    tool::Logging(myName.c_str(), "Group Num is %d\n", absMethodObj->groupNum);
    tool::Logging(myName.c_str(), "Total logical size is %lu\n", absMethodObj->totalLogicalSize);
    tool::Logging(myName.c_str(), "Total unique size is %lu\n", absMethodObj->totaluniqueSize);
    tool::Logging(myName.c_str(), "Total compressed size is %lu\n", absMethodObj->totalCompressedSize);
    tool::Logging(myName.c_str(), "Overall Compression ratio is %.4f\n", (double)absMethodObj->totalLogicalSize / (double)absMethodObj->totalCompressedSize);
    tool::Logging(myName.c_str(), "Unique Compression ratio is %.4f\n", (double)absMethodObj->totaluniqueSize / (double)absMethodObj->totalCompressedSize);
    absMethodObj->PrintChunkInfo(dirName, chunkingType, compressionMethod, processNum, sumTimeInSeconds);
    // absMethodObj->DeCompressionAll();
    // if (true)
    //     for (auto i = 0; i < processNum; i++)
    //     {
    //         absMethodObj->setFileName(readfileList[i]);
    //         absMethodObj->restoreFile(readfileList[i]);
    //     }
    delete chunkerObj;
    delete absMethodObj;
    return 0;
}