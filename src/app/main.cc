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

    string dirName;
    string myName = "Ftcg";

    vector<string> readfileList;

    const char optString[] = "i:m:c:n:s:";
    if (argc < sizeof(optString) - 2)
    {
        cout << "Usage: " << argv[0] << " -i <input file> -m <chunking method> -c <compression method> -n <process number>" << endl;
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
        absMethodObj = new lz4Compare();
        break;
    case 1:
        absMethodObj = new lz4Baseline();
        break;
    case 2:
        absMethodObj = new FPLz4Baseline();
        break;
    case 3:
        if (ExchunkSize == -1)
            absMethodObj = new FPHierarchicalSF();
        else
            absMethodObj = new FPHierarchicalSF(ExchunkSize);
        break;
    case 4:
        if (ExchunkSize == -1)
            absMethodObj = new FPLz4BaselineImplement();
        else
            absMethodObj = new FPLz4BaselineImplement(ExchunkSize);
        break;
    case 5:
        if (ExchunkSize == -1)
            absMethodObj = new FPLz4SFBlockinBlock();
        else
            absMethodObj = new FPLz4SFBlockinBlock(ExchunkSize);
        break;
    case 6:
        absMethodObj = new FPOnly();
        break;
    case 7:
        absMethodObj = new bruteforce();
        break;
    case 8:
        absMethodObj = new characterFrequency();
        break;
    default:
        break;
    }
    absMethodObj->dataWrite_ = new dataWrite();
    tool::traverse_dir(dirName, readfileList, nofilter);
    sort(readfileList.begin(), readfileList.end(), tool::compareNat);

    boost::thread *thTmp;
    boost::thread::attributes attrs;
    attrs.set_stack_size(THREAD_STACK_SIZE);

    absMethodObj->SetInputMQ(chunker2lz4);

    tool::Logging(myName.c_str(), "Start to process %d files\n", processNum);
    for (auto i = 0; i < processNum; i++)
    {
        chunkerObj->LoadChunkFile(readfileList[i]);
        thTmp = new boost::thread(attrs, boost::bind(&Chunker::Chunking, chunkerObj));
        if (i == processNum - 1)
        {
            absMethodObj->isLastFile = true;
        }
        absMethodObj->ProcessOneTrace();

        thTmp->join();
        delete thTmp;
    }

    tool::Logging(myName.c_str(), " processNum %d \n", processNum);
    tool::Logging(myName.c_str(), "Group Num is %d\n", absMethodObj->groupNum);
    tool::Logging(myName.c_str(), "Total logical size is %lu\n", absMethodObj->totalLogicalSize);
    tool::Logging(myName.c_str(), "Total compressed size is %lu\n", absMethodObj->totalCompressedSize);
    tool::Logging(myName.c_str(), "Compression ratio is %.4f\n", (double)absMethodObj->totalLogicalSize / (double)absMethodObj->totalCompressedSize);

    delete chunkerObj;
    delete absMethodObj;
    return 0;
}