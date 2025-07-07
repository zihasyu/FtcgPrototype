#include "../../include/method/FP-lz4Baseline-implement.h"

FPLz4BaselineImplement::FPLz4BaselineImplement()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}
FPLz4BaselineImplement::FPLz4BaselineImplement(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

FPLz4BaselineImplement::~FPLz4BaselineImplement()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void FPLz4BaselineImplement::ProcessOneTrace()
{
    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        string hashStr;
        if (recieveQueue->Pop(tmpChunk))
        {
            // calculate feature
            if (!IsDedup(tmpChunk))
            {
                table.Put(std::to_string(tmpChunk.chunkID),
                          std::string(reinterpret_cast<char *>(tmpChunk.chunkContent), tmpChunk.chunkSize));
                tmpChunk.isGrouped = false;
                unfinishedChunkNum++;
                Chunk_Insert(tmpChunk);
                totalChunkNum++;
                // todo:add to decipe
            }
            else
            {
                // todo:add to decipe
                totalChunkNum++;
            }
        }
    }
    if (!isLastFile)
    {
        recieveQueue->done_ = false;
        return;
    }
    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    totalFeature += table.original_feature_key_table.size();
    set<uint64_t> finishedChunks;

    ofstream out("../frequencyTable.txt", ios::app);

    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());
    tool::Logging(myName_.c_str(), "FP finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "FP unfinished chunk num is %d\n", unfinishedChunkNum);
    for (auto feature : table.feature_key_table_)
    {
        tmpGroup.clear();
        for (auto id : feature.second)
        {
            if (finishedChunks.find(stoull(id)) != finishedChunks.end())
            {
                continue;
            }
            tmpGroup.insert(stoull(id));
            chunkSet[stoull(id)].isGrouped = true;
            unfinishedChunkNum--;
            finishedChunks.insert(stoull(id));
            // if (tmpGroup.size() == MAX_GROUP_SIZE)
            // {
            //     finishedGroups.push_back(tmpGroup);
            //     tmpGroup.clear();
            // }
        }
        if (tmpGroup.size() > 1)
        {
            finishedGroups.push_back(tmpGroup);
            tmpGroup.clear();
        }
        else
        {
            for (auto id : tmpGroup)
            {
                finishedChunks.erase(id);
                chunkSet[id].isGrouped = false;
                unfinishedChunkNum++;
            }
            tmpGroup.clear();
        }
    }
    tool::Logging(myName_.c_str(), "a Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "a Unfinished chunk num is %d\n", unfinishedChunkNum);

    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "first group size, frequency" << endl;
    out << 1 << "," << unfinishedChunkNum << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    FinalMerge();

    groupNum += finishedGroups.size();

    std::vector<uint64_t> maxGroup;
    tmpGroup.clear();
    for (const auto &it : finishedGroups)
    {
        compressedChunkNum += it.size();
        if (it.size() > tmpGroup.size())
        {
            tmpGroup = it;
        }
    }
    for (auto id : tmpGroup)
    {
        maxGroup.push_back(id);
    }
    vector<vector<double>> deltaRatioTable(maxGroup.size(), vector<double>(maxGroup.size(), 0));
    uint8_t *deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    for (auto id = maxGroup.begin(); id != maxGroup.end(); id++)
    {
        Chunk_t basechunk = Get_Chunk_Info(*id);
        for (auto id2 = id + 1; id2 != maxGroup.end(); id2++)
        {
            uint8_t *deltachunk;
            Chunk_t tmpChunk = Get_Chunk_Info(*id2);
            uint64_t savesize = 0;
            deltachunk = xd3_encode(tmpChunk.chunkContent, tmpChunk.chunkSize, basechunk.chunkContent, basechunk.chunkSize, &savesize, deltaMaxChunkBuffer);
            if (savesize > tmpChunk.chunkSize)
            {
                std::cerr << "delta error" << std::endl;
                return;
            }
            if (savesize == 0)
            {
                cout << "delta error and can't to restore" << endl;
                return;
            }
            double ratio = (double)tmpChunk.chunkSize / (double)savesize;
            deltaRatioTable[distance(maxGroup.begin(), id)][distance(maxGroup.begin(), id2)] = ratio;
            free(deltachunk);
        }
    }
    // print ratio
    for (auto it : deltaRatioTable)
    {
        for (auto it2 : it)
        {
            cout << it2 << " ";
        }
        cout << endl;
    }
    free(deltaMaxChunkBuffer);

    tool::Logging(myName_.c_str(), "compressed chunk num is %d\n", compressedChunkNum);

    // CompressionToFinishedGroup();
    // MigratoryCompression();
    out.close();
    // calculate the throughput
    // double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
    // double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
    // tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    recieveQueue->done_ = false;
    return;
}
