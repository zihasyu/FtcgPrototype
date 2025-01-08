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
                table.PutHierarchicalSF(std::to_string(tmpChunk.chunkID), (char *)tmpChunk.chunkContent);
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
    // vector<set<string>> unfinishedGroups;
    set<uint64_t> finishedChunks;
    set<uint64_t> unfinishedChunks;
    set<uint64_t> tmpGroup; // 16一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);

    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFA_unfinished_group;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFB_unfinished_group;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFC_unfinished_group;

    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());

    // 根据FP进行初次分组
    frequency_table.clear();
    for (auto it : this->FPindex)
    {
        tmpGroup.clear();
        frequency_table[it.second.size()]++;
        for (auto id : it.second)
        {
            if (id == *it.second.begin())
            {
                tmpGroup.insert(id);
                // hierarchicalSFA_unfinished_group[table.key_hierarchicalSF_table[to_string(id)][0]].push_back(group);
                // hierarchicalSFB_unfinished_group[table.key_hierarchicalSF_table[to_string(id)][1]].push_back(group);
                hierarchicalSFC_unfinished_group[table.key_hierarchicalSF_table[to_string(id)][2]].push_back(tmpGroup);
                unfinishedChunks.insert(id);
                tmpGroup.clear();
                continue;
            }
            tmpGroup.insert(id);
            finishedChunks.insert(id);
        }
        if (tmpGroup.size() > 0)
        {
            // FPunfinishedGroups[to_string(*it.second.begin())] = tmpGroup;
            compressedChunkNum += tmpGroup.size();
        }
    }

    out << "FP group size, frequency" << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }
    tool::Logging(myName_.c_str(), "FP finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "FP unfinished chunk num is %d\n", unfinishedChunks.size());

    for (auto it : table.hierarchicalSFB_C_table)
    {
        for (auto sf_c : it.second)
        {
            hierarchicalSFB_unfinished_group[it.first].insert(hierarchicalSFB_unfinished_group[it.first].end(), hierarchicalSFC_unfinished_group[sf_c].begin(), hierarchicalSFC_unfinished_group[sf_c].end());
        }
    }

    // SFb分组
    for (auto it : hierarchicalSFB_unfinished_group)
    {
        tmpGroup.clear();
        auto &groups = it.second;
        if (it.first == "0" || groups.size() > 1000)
        {
            for (auto group : it.second)
            {
                if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        finishedChunks.insert(id);
                        unfinishedChunks.erase(id);
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
                    unfinishedChunks.erase(id);
                }
                tmpGroup.clear();
            }
            else
            {
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
            continue;
        }

        groupmerge(groups, MAX_GROUP_SIZE);
        for (auto group : groups)
        {
            if (group.size() < 2)
            {
                continue;
            }
            for (auto id : group)
            {
                finishedChunks.insert(id);
                unfinishedChunks.erase(id);
            }
            finishedGroups.push_back(group);
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
        if (it->first == "0" || groups.size() > 1000)
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
                        unfinishedChunks.erase(id);
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
                    unfinishedChunks.erase(id);
                }
                tmpGroup.clear();
            }
            else
            {
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
            continue;
        }

        groupmerge(groups, MAX_GROUP_SIZE);
        for (auto group : groups)
        {
            if (group.size() < 2)
            {
                continue;
            }
            for (auto id : group)
            {
                finishedChunks.insert(id);
                unfinishedChunks.erase(id);
            }
            finishedGroups.push_back(group);
        }
    }
    tool::Logging(myName_.c_str(), "a Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "a Unfinished chunk num is %d\n", unfinishedChunks.size());

    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "first group size, frequency" << endl;
    out << 1 << "," << unfinishedChunks.size() << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    vector<set<uint64_t>> leftGroups;
    for (auto id : unfinishedChunks)
    {
        tmpGroup.insert(id);
        if (tmpGroup.size() == MAX_GROUP_SIZE)
        {
            leftGroups.push_back(tmpGroup);
            finishedGroups.push_back(tmpGroup);
            tmpGroup.clear();
        }
    }
    if (tmpGroup.size() > 0)
    {
        leftGroups.push_back(tmpGroup);
        finishedGroups.push_back(tmpGroup);
        tmpGroup.clear();
    }
    // second_group(unFullGroups, finishedGroups);
    // tool::Logging(myName_.c_str(), "third Finished group num is %d\n", finishedGroups.size());
    groupNum += finishedGroups.size();
    // 对Group进行排序
    // sort(finishedGroups.begin(), finishedGroups.end(), [](const set<string> &a, const set<string> &b)
    //      { return a.size() > b.size(); });

    // 对group中的chunk以16为一组进行压缩

    // // 验证group中的chunk是否有重复
    // set<string> allChunks;
    // for (auto group : finishedGroups)
    // {
    //     for (auto id : group)
    //     {
    //         if (allChunks.find(id) != allChunks.end())
    //         {
    //             tool::Logging(myName_.c_str(), "chunk %s is in two group\n", id.c_str());
    //         }
    //         allChunks.insert(id);
    //     }
    // }
    for (auto it : finishedGroups)
    {
        compressedChunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "compressed chunk num is %d\n", compressedChunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
    totalLogicalSize = compressedChunkNum * 8 * 1024;
    totalCompressedSize = 0;
    map<int, uint64_t> groupLogicalSize;
    map<int, uint64_t> groupCompressedSize;
    for (auto group : finishedGroups)
    {
        if (!groupLogicalSize[group.size()])
        {
            groupLogicalSize[group.size()] = 0;
        }
        if (!groupCompressedSize[group.size()])
        {
            groupCompressedSize[group.size()] = 0;
        }
        for (auto id : group)
        {

            auto start = std::chrono::high_resolution_clock::now();
            Chunk_t GroupTmpChunk = Get_Chunk_Info(id);
            memcpy(clusterBuffer + clusterSize, GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize);
            if (GroupTmpChunk.loadFromDisk)
            {
                free(GroupTmpChunk.chunkContent);
            }
            auto end = std::chrono::high_resolution_clock::now();
            clustringTime += end - start;

            clusterCnt++;
            ChunkNum++;
            // clusterSize += GroupTmpChunk.chunkSize;
            clusterSize += chunkSet[id].chunkSize;
        }
        groupLogicalSize[group.size()] += clusterSize;
        // do lz4 compression
        int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
        if (compressedSize <= 0)
        {
            compressedSize = clusterSize;
        }
        totalCompressedSize += compressedSize;
        // totalLogicalSize += clusterSize;
        clusterNum++;
        clusterCnt = 0;
        clusterSize = 0;

        groupCompressedSize[group.size()] += compressedSize;
    }

    out << "group size, logical size, compressed size, ratio" << endl;
    for (auto it : groupLogicalSize)
    {
        out << it.first << ", " << it.second << ", " << groupCompressedSize[it.first] << ", " << (double)it.second / (double)groupCompressedSize[it.first] << endl;
    }
    out.close();

    // process last cluster
    if (clusterCnt > 0)
    {
        int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);

        if (compressedSize <= 0)
        {
            compressedSize = clusterSize;
        }
        totalCompressedSize += compressedSize;
        // totalLogicalSize += (clusterCnt * 8 * 1024);
        clusterCnt = 0;
    }

    // tool::Logging(myName_.c_str(), "%d feature with only one chunk and total feature num is %d\n", singeFeature, table.original_feature_key_table.size());

    // calculate the throughput
    double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
    double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
    tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    recieveQueue->done_ = false;
    return;
}
