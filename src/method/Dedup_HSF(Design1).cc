#include "../../include/method/Dedup-HSF.h"

Dedup_HSF::Dedup_HSF()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSF::Dedup_HSF(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSF::~Dedup_HSF()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void Dedup_HSF::ProcessOneTrace()
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
                table.PutHierarchicalSF(std::to_string(tmpChunk.chunkID),
                                        std::string(reinterpret_cast<char *>(tmpChunk.chunkContent), tmpChunk.chunkSize));
                hierarchicalSFC_unfinished_group[table.key_hierarchicalSF_table[to_string(tmpChunk.chunkID)][2]].push_back({tmpChunk.chunkID});
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

    for (auto it : table.hierarchicalSFB_C_table)
    {
        for (auto sf_c : it.second)
        {
            hierarchicalSFB_unfinished_group[it.first].insert(hierarchicalSFB_unfinished_group[it.first].end(), hierarchicalSFC_unfinished_group[sf_c].begin(), hierarchicalSFC_unfinished_group[sf_c].end());
        }
    }
    for (auto it : table.hierarchicalSFA_B_table)
    {
        for (auto sf_b : it.second)
        {
            hierarchicalSFA_unfinished_group[it.first].insert(hierarchicalSFA_unfinished_group[it.first].end(), hierarchicalSFB_unfinished_group[sf_b].begin(), hierarchicalSFB_unfinished_group[sf_b].end());
        }
    }
    // SFa分组
    for (auto it = hierarchicalSFA_unfinished_group.begin(); it != hierarchicalSFA_unfinished_group.end(); it++)
    {
        auto &groups = it->second;
        // if (it->first == "0" || groups.size() > 0)
        if (groups.size() > 512)
        {
            tmpGroup.clear();
            for (auto group : it->second)
            {
                if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE)
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
                tmpGroup.insert(group.begin(), group.end());
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
            }
            tmpGroup.clear();
        }
        else
        {
            groupmerge(groups, MAX_GROUP_SIZE);
            for (auto group : groups)
            {
                if (group.size() >= 2)

                {
                    for (auto id : group)
                    {
                        finishedChunks.insert(id);
                        chunkSet[id].isGrouped = true;
                        unfinishedChunkNum--;
                    }
                    finishedGroups.push_back(group);
                }
            }
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

    for (const auto &it : finishedGroups)
    {
        compressedChunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "compressed chunk num is %d\n", compressedChunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
    tool::Logging(myName_.c_str(), "Compression start\n");
    CompressionToFinishedGroup();
    tool::Logging(myName_.c_str(), "Compression finished\n");
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