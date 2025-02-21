#include "../../include/method/Dedup-HSFRank.h"

Dedup_HSFRank::Dedup_HSFRank()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSFRank::Dedup_HSFRank(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSFRank::~Dedup_HSFRank()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void Dedup_HSFRank::ProcessOneTrace()
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
                // table.PutHSFRank(std::to_string(tmpChunk.chunkID), (char *)tmpChunk.chunkContent);
                table.PutHSFRank(std::to_string(tmpChunk.chunkID),
                                 std::string(reinterpret_cast<char *>(tmpChunk.chunkContent), tmpChunk.chunkSize));
                hierarchicalSFC_unfinished_chunk[table.key_hierarchicalSF_table[to_string(tmpChunk.chunkID)][2]].push_back(tmpChunk.chunkID);
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

    for (auto &it : table.hierarchicalSFA_C_table)
    {
        vector<string> SFCs(it.second.begin(), it.second.end());
        sort(SFCs.begin(), SFCs.end(), [&](const string &a, const string &b)
             { return a > b; });
        tmpGroup.clear();
        for (auto sf_c : SFCs)
        {
            for (auto groups : hierarchicalSFC_unfinished_chunk[sf_c])
            {
                tmpGroup.insert(groups);
                if (tmpGroup.size() >= MAX_GROUP_SIZE)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        finishedChunks.insert(id);
                        chunkSet[id].isGrouped = true;
                        unfinishedChunkNum--;
                    }
                    tmpGroup.clear();
                }
            }
        }
        if (tmpGroup.size() > 1)
        {
            finishedGroups.push_back(tmpGroup);
            for (auto id : tmpGroup)
            {
                finishedChunks.insert(id);
                chunkSet[id].isGrouped = true;
                unfinishedChunkNum--;
            }
            tmpGroup.clear();
        }
        else
        {
            for (auto id : tmpGroup)
            {
                // chunkSet[id].isGrouped = false;
                // unfinishedChunkNum++;
            }
            tmpGroup.clear();
        }
    }

    tool::Logging(myName_.c_str(), "HSF Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "HSF Unfinished chunk num is %d\n", unfinishedChunkNum);

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

    for (const auto &it : finishedGroups)
    {
        compressedChunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "compressed chunk num is %d\n", compressedChunkNum);
    // tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());

    // CompressionToFinishedGroup();
    MigratoryCompression();

    out << "group size, logical size, compressed size, ratio" << endl;
    for (auto it : groupLogicalSize)
    {
        out << it.first << ", " << it.second << ", " << groupCompressedSize[it.first] << ", " << (double)it.second / (double)groupCompressedSize[it.first] << endl;
    }
    out.close();
    // calculate the throughput
    double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
    double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
    tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    recieveQueue->done_ = false;
    return;
}