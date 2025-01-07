#include "../../include/method/Dedup-HSF-BIB.h"

Dedup_HSH_BIB::Dedup_HSH_BIB()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSH_BIB::Dedup_HSH_BIB(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSH_BIB::~Dedup_HSH_BIB()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void Dedup_HSH_BIB::ProcessOneTrace()
{
    auto start = std::chrono::high_resolution_clock::now();
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
            stringstream ss;
            ss << tmpChunk.chunkID;

            // table.PutOrignals(ss.str(), (char *)tmpChunk.chunkContent);
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            //  nTransTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // finesseTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            table.PutHierarchicalSF(ss.str(), (char *)tmpChunk.chunkContent);

            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            GenerateHash(mdCtx, tmpChunk.chunkContent, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
            FP_Insert(hashStr, tmpChunk.chunkID);

            Chunk_Insert(tmpChunk);
            totalChunkNum++;
        }
    }

    if (!isLastFile)
    {
        recieveQueue->done_ = false;
        return;
    }
    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);

    // auto start = std::chrono::high_resolution_clock::now();
    // for (auto it : table.original_feature_key_table)
    // {
    //     for (auto id : it.second)
    //     {
    //     }
    // }
    // auto end = std::chrono::high_resolution_clock::now();
    // clustringTime += end - start;

    // vector<feature_t> sorted_original_features = table.sortFeatureBySetSize();

    totalFeature += table.original_feature_key_table.size();
    vector<set<uint64_t>> finishedGroups;
    unordered_map<string, set<uint64_t>> FPunfinishedGroups;
    set<uint64_t> finishedChunks;
    set<uint64_t> unfinishedChunks;
    vector<set<uint64_t>> adjGroups;
    set<uint64_t> tmpGroup; // 16一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);
    map<feature_t, set<string>> feature_FP_Table;
    vector<vector<int>> indices; // 标识特征值下面那些group有相同的FP

    unordered_map<string, vector<set<uint64_t>>>
        hierarchicalSFA_unfinished_group;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFB_unfinished_group;
    map<string, vector<uint64_t>> hierarchicalSFC_unfinished_chunk;
    // set<string> usedChunks;

    // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());
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
                hierarchicalSFC_unfinished_chunk[table.key_hierarchicalSF_table[to_string(id)][2]].push_back(id);
                feature_FP_Table[table.original_key_feature_table_[to_string(id)]].insert(it.first);
                unfinishedChunks.insert(id);
                tmpGroup.clear();
                continue;
            }
            tmpGroup.insert(id);
            finishedChunks.insert(id);
        }
        if (tmpGroup.size() > 0)
        {
            FPunfinishedGroups[to_string(*it.second.begin())] = tmpGroup;
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
                if (tmpGroup.size() == MAX_GROUP_SIZE)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        finishedChunks.insert(id);
                        unfinishedChunks.erase(id);
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
    }

    // for (auto it : table.hierarchicalSFB_C_table)
    // {
    //     for (auto sf_c : it.second)
    //     {
    //         hierarchicalSFB_unfinished_group[it.first].insert(hierarchicalSFB_unfinished_group[it.first].end(), hierarchicalSFC_unfinished_group[sf_c].begin(), hierarchicalSFC_unfinished_group[sf_c].end());
    //     }
    // }

    // for (auto it : table.hierarchicalSFA_B_table)
    // {
    //     for (auto sf_b : it.second)
    //     {
    //         hierarchicalSFA_unfinished_group[it.first].insert(hierarchicalSFA_unfinished_group[it.first].end(), hierarchicalSFB_unfinished_group[sf_b].begin(), hierarchicalSFB_unfinished_group[sf_b].end());
    //     }
    // }
    // // SFa分组
    // for (auto it = hierarchicalSFA_unfinished_group.begin(); it != hierarchicalSFA_unfinished_group.end(); it++)
    // {
    //     auto &groups = it->second;
    //     if (it->first == "0" || groups.size() > 0)
    //     {
    //         set<string> tmpGroup;
    //         for (auto group : it->second)
    //         {
    //             if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE)
    //             {
    //                 finishedGroups.push_back(tmpGroup);
    //                 for (auto id : tmpGroup)
    //                 {
    //                     finishedChunks.insert(id);
    //                     unfinishedChunks.erase(id);
    //                 }
    //                 tmpGroup.clear();
    //             }
    //             tmpGroup.insert(group.begin(), group.end());
    //         }
    //         if (tmpGroup.size() > 1)
    //         {
    //             finishedGroups.push_back(tmpGroup);
    //             for (auto id : tmpGroup)
    //             {
    //                 finishedChunks.insert(id);
    //                 unfinishedChunks.erase(id);
    //             }
    //             tmpGroup.clear();
    //         }
    //         else
    //         {
    //             for (auto id : tmpGroup)
    //             {
    //                 unfinishedChunks.insert(id);
    //             }
    //             tmpGroup.clear();
    //         }
    //         continue;
    //     }

    //     groupmerge(groups, MAX_GROUP_SIZE);
    //     for (auto group : groups)
    //     {
    //         if (group.size() < 2)
    //         {
    //             continue;
    //         }
    //         for (auto id : group)
    //         {
    //             finishedChunks.insert(id);
    //             unfinishedChunks.erase(id);
    //         }
    //         finishedGroups.push_back(group);
    //     }
    // }
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

    // 三次分组
    // cout << "----------------------------BIB------------------------------" << endl;
    // 对每一个1块进行FastCDC,找到一个代表块,代表块的chunkid为原来块的id
    vector<representChunk_t> representChunkSet;
    FeatureIndexTable representTable;
    map<super_feature_t, vector<set<uint64_t>>> Rfeature_unfinishedGroup;

    for (auto id : unfinishedChunks)
    {
        uint64_t start = 0;
        uint64_t end = 0;

        // 用于内存读写
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        Chunk_t GroupTmpChunk = Get_Chunk_Info(id);
        ThirdCutPointHashMin(GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize, start, end);
        // ThirdCutPointHashMin_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        representChunk_t representChunk;
        representChunk.chunkID = id;
        representChunk.offset_start = start;
        representChunk.offset_end = end;
        representChunkSet.push_back(representChunk);
        string representChunkContent((char *)GroupTmpChunk.chunkContent + start, end - start);
        representTable.Put(to_string(id), (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[to_string(id)][0]].push_back({id});
        if (GroupTmpChunk.loadFromDisk)
        {
            free(GroupTmpChunk.chunkContent);
        }
    }
    for (auto group = finishedGroups.begin(); group != finishedGroups.end();)
    {
        if (group->size() > MAX_GROUP_SIZE - 1)
        {
            group++;
            continue;
        }
        auto id = *group->begin();
        uint64_t start = 0;
        uint64_t end = 0;

        // 用于内存读写
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        Chunk_t GroupTmpChunk = Get_Chunk_Info(id);
        ThirdCutPointHashMin(GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize, start, end);
        representChunk_t representChunk;
        representChunk.chunkID = id;
        representChunk.offset_start = start;
        representChunk.offset_end = end;
        representChunkSet.push_back(representChunk);
        string representChunkContent((char *)GroupTmpChunk.chunkContent + start, end - start);
        representTable.Put(to_string(id), (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[to_string(id)][0]].push_back(*group);
        for (auto id : *group)
        {
            unfinishedChunks.insert(id);
            finishedChunks.erase(id);
        }
        group = finishedGroups.erase(group);
    }
    for (auto it : representTable.feature_key_table_)
    {
        tmpGroup.clear();
        if (it.first == 0 || Rfeature_unfinishedGroup[it.first].size() > 0)
        {
            for (auto group : Rfeature_unfinishedGroup[it.first])
            {
                if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE && tmpGroup.size() > 0)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.erase(id);
                        finishedChunks.insert(id);
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
                    unfinishedChunks.erase(id);
                    finishedChunks.insert(id);
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

        auto groups = Rfeature_unfinishedGroup[it.first];
        // 记录哪些group被合并
        // set<string> firstID;
        // for (auto group = groups.begin(); group != groups.end(); group++)
        // {
        //     firstID.insert(*group->begin());
        // }
        groupmerge(groups, MAX_GROUP_SIZE);
        for (auto group : groups)
        {
            if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE && tmpGroup.size() > 0)
            {
                finishedGroups.push_back(tmpGroup);
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.erase(id);
                    finishedChunks.insert(id);
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
                unfinishedChunks.erase(id);
                finishedChunks.insert(id);
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
    }
    tool::Logging(myName_.c_str(), "third Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "third Unfinished chunk num is %d\n", unfinishedChunks.size());
    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "third group size, frequency" << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    for (auto id : unfinishedChunks)
    {
        tmpGroup.insert(id);
        if (tmpGroup.size() == MAX_GROUP_SIZE)
        {
            finishedGroups.push_back(tmpGroup);
            tmpGroup.clear();
        }
    }
    if (tmpGroup.size() > 0)
    {
        finishedGroups.push_back(tmpGroup);
    }

    unfinishedChunks.clear();
    for (auto groups = finishedGroups.begin(); groups != finishedGroups.end();)
    {
        if (groups->size() > 1)
        {
            groups++;
            continue;
        }
        for (auto id : *groups)
        {
            unfinishedChunks.insert(id);
        }
        groups = finishedGroups.erase(groups);
    }
    // 将剩余的1块进行分组

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

    auto end = std::chrono::high_resolution_clock::now();
    featureExtractTime += end - start;

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
