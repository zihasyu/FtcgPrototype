#include "../../include/lz4Baseline.h"

lz4Baseline::lz4Baseline()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

lz4Baseline::~lz4Baseline()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

// 找到一个group中频率最高的feature，排除掉当前feature
feature_t lz4Baseline::find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeatures)
{
    map<feature_t, int> freq;
    for (auto chunk : chunks)
    {
        for (auto it = table.original_key_features_table_[chunk].begin(); it != table.original_key_features_table_[chunk].end(); it++)
        {
            freq[*it]++;
        }
    }
    for (auto it : usedFeatures)
    {
        freq.erase(it);
    }
    if (freq.size() == 0)
    {
        return 0;
    }
    int max_freq = 0;
    feature_t most_freq_feature = 0;
    for (auto it : freq)
    {
        if (it.second > max_freq)
        {
            max_freq = it.second;
            most_freq_feature = it.first;
        }
    }
    return most_freq_feature;
}

// 找到一个group中最相关的feature，排除掉当前feature
feature_t lz4Baseline::find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature)
{
    int max_correlation = 0;
    feature_t most_correlated_feature = 1;
    for (auto it : table.original_features_corelation_matrix[feature])
    {
        if (it.second > max_correlation && usedFeature.find(it.first) == usedFeature.end())
        {
            max_correlation = it.second;
            most_correlated_feature = it.first;
        }
    }
    return most_correlated_feature;
}

// // 二次分组
// void lz4Baseline::second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group)
// {
//     // int size = unFullGroups.size();
//     int count = 0;
//     set<feature_t> usedFeature;
//     for (auto it = unFullGroups.begin(); it != unFullGroups.end(); it++)
//     {
//         // count++;
//         if (usedFeature.find(it->first) != usedFeature.end())
//         {
//             continue;
//         }
//         usedFeature.insert(it->first);
//         feature_t tempFeature = 0;
//         int max_correlation = 0;
//         auto it1_map = table.original_features_corelation_matrix.find(it->first);
//         for (auto it2 = next(it); it2 != unFullGroups.end(); it2++)
//         {
//             if (usedFeature.find(it2->first) != usedFeature.end())
//             {
//                 continue;
//             }
//             if (it1_map == table.original_features_corelation_matrix.end() || it1_map->second.find(it2->first) == it1_map->second.end())
//             {
//                 continue;
//             }
//             if (it1_map->second[it2->first] > max_correlation)
//             {
//                 max_correlation = it1_map->second[it2->first];
//                 tempFeature = it2->first;
//             }
//         }
//         if (tempFeature == 0)
//         {
//             set<string> tempGroup = it->second;
//             group.push_back(tempGroup);
//             continue;
//         }
//         usedFeature.insert(tempFeature);
//         set<string> tempGroup = it->second;

//         tempGroup.insert(unFullGroups[tempFeature].begin(), unFullGroups[tempFeature].end());

//         group.push_back(tempGroup);
//     }
// }

void lz4Baseline::ProcessOneTrace()
{
    int count = 0;
    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        if (recieveQueue->Pop(tmpChunk))
        {
            // if (count >= EXPERIMENT_CHUNK_NUM)
            // {
            //     continue;
            // }
            // count++;
            // calculate feature
            stringstream ss;
            ss << tmpChunk.chunkID;
            auto start = std::chrono::high_resolution_clock::now();
            // table.PutOrignals(ss.str(), (char *)tmpChunk.chunkContent);
            table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            //  nTransTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // finesseTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            auto end = std::chrono::high_resolution_clock::now();
            featureExtractTime += end - start;
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);

            // chunkSet.push_back(tmpChunk);
            dataWrite_->Chunk_Insert(tmpChunk);
            totalChunkNum++;
        }
    }
    if (!isLastFile)
    {
        recieveQueue->done_ = false;
        return;
    }
    totalFeature += table.original_feature_key_table.size();
    vector<set<string>> finishedGroups;
    // vector<set<string>> unfinishedGroups;
    set<string> finishedChunks;
    set<string> unfinishedChunks;
    vector<set<string>> adjGroups;
    set<string> group; // 16一组chunkid
    // set<string> usedChunks;
    // 对于每一个feature进行分组
    // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());
    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());
    for (auto it : table.feature_key_table_)
    {
        for (auto id : it.second)
        {

            // 如果这个chunk已经被分组过了
            if (finishedChunks.find(id) != finishedChunks.end() || unfinishedChunks.find(id) != unfinishedChunks.end())
            {
                continue;
            }
            group.insert(id);
            if (group.size() == MAX_GROUP_SIZE)
            {
                for (auto id : group)
                {
                    finishedChunks.insert(id);
                }
                finishedGroups.push_back(group);
                group.clear();
            }
        }
        if (group.size() > 1)
        {
            for (auto id : group)
            {
                finishedChunks.insert(id);
            }
            finishedGroups.push_back(group);
            group.clear();
        }
        else if (group.size() > 0)
        {
            for (auto id : group)
            {
                unfinishedChunks.insert(id);
            }
            group.clear();
        }
    }
    frequency_table.clear();
    ofstream out("../frequencyTable.txt", ios::app);
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

    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    tool::Logging(myName_.c_str(), "first Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "first Unfinished chunk num is %d\n", unfinishedChunks.size());

    group.clear();
    for (auto id : unfinishedChunks)
    {
        group.insert(id);
        if (group.size() == MAX_GROUP_SIZE)
        {
            finishedGroups.push_back(group);
            group.clear();
        }
    }
    if (group.size() > 0)
    {
        finishedGroups.push_back(group);
        group.clear();
    }

    // second_group(unFullGroups, finishedGroups);
    // tool::Logging(myName_.c_str(), "third Finished group num is %d\n", finishedGroups.size());
    groupNum += finishedGroups.size();
    int tmpchunkNum = 0;
    for (auto it : finishedGroups)
    {
        tmpchunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "tmp chunk num is %d\n", tmpchunkNum);
    // 对Group进行排序
    // sort(finishedGroups.begin(), finishedGroups.end(), [](const set<string> &a, const set<string> &b)
    //      { return a.size() > b.size(); });

    // 对group中的chunk以16为一组进行压缩

    totalLogicalSize = 0;
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
            // memcpy(clusterBuffer + clusterSize, chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize);

            Chunk_t GroupTmpChunk = dataWrite_->Get_Chunk_Info(stoull(id));
            memcpy(clusterBuffer + clusterSize, GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize);

            if (GroupTmpChunk.loadFromDisk)
            {
                free(GroupTmpChunk.chunkContent);
            }
            auto end = std::chrono::high_resolution_clock::now();
            clustringTime += end - start;

            clusterCnt++;
            ChunkNum++;
            clusterSize += GroupTmpChunk.chunkSize;
            // clusterSize += chunkSet[stoull(id)].chunkSize;
        }
        groupLogicalSize[group.size()] += clusterSize;
        // do lz4 compression
        int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
        if (compressedSize <= 0)
        {
            compressedSize = clusterSize;
        }
        totalCompressedSize += compressedSize;
        totalLogicalSize += clusterSize;
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
        totalLogicalSize += (clusterCnt * 8 * 1024);
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

// 最初的方法，用于对比
void lz4Baseline::ProcessOneTraceOrigin()
{

    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        if (recieveQueue->Pop(tmpChunk))
        {
            // calculate feature
            stringstream ss;
            ss << tmpChunk.chunkID;
            auto start = std::chrono::high_resolution_clock::now();
            table.PutOrignal(ss.str(), (char *)tmpChunk.chunkContent);
            auto end = std::chrono::high_resolution_clock::now();
            featureExtractTime += end - start;
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);

            chunkSet.push_back(tmpChunk);
        }
    }

    // auto start = std::chrono::high_resolution_clock::now();
    // for (auto it : table.original_feature_key_table)
    // {
    //     for (auto id : it.second)
    //     {
    //     }
    // }
    // auto end = std::chrono::high_resolution_clock::now();
    // clustringTime += end - start;
    totalFeature += table.original_feature_key_table.size();
    for (auto it : table.original_feature_key_table)
    {
        if (it.second.size() < MAX_GROUP_SIZE)
        {
            feature_less_than_16++;
        }

        for (auto id : it.second)
        {
            // 记录只有一个chunk的feature数量
            if (it.second.size() == 1)
            {
                singeFeature++;
            }

            if (clusterCnt < MAX_GROUP_SIZE)
            {
                auto start = std::chrono::high_resolution_clock::now();
                memcpy(clusterBuffer + clusterSize, chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize);
                auto end = std::chrono::high_resolution_clock::now();
                clustringTime += end - start;
            }
            else
            {
                // do lz4 compression
                int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
                if (compressedSize <= 0)
                {
                    compressedSize = clusterSize;
                }
                totalCompressedSize += compressedSize;
                totalLogicalSize += clusterSize;
                clusterNum++;
                clusterCnt = 0;
                clusterSize = 0;

                // copy new chunk
                memcpy(clusterBuffer + clusterSize, chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize);
            }
            clusterCnt++;
            ChunkNum++;
            clusterSize += chunkSet[stoull(id)].chunkSize;
        }
    }

    // process last cluster
    if (clusterCnt > 0)
    {

        int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
        if (compressedSize <= 0)
        {
            compressedSize = clusterSize;
        }
        totalCompressedSize += compressedSize;
        totalLogicalSize += (clusterCnt * 8 * 1024);
        clusterCnt = 0;
    }
    tool::Logging(myName_.c_str(), "%d feature with only one chunk and total feature num is %d\n", singeFeature, table.original_feature_key_table.size());

    // calculate the throughput
    double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
    double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
    tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    recieveQueue->done_ = false;
    return;
}

void lz4Baseline::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}

// 通过频率进行二次分组
// totalFeature += table.original_feature_key_table.size();
//     vector<feature_t> sorted_original_features = table.sortFeatureBySetSize();
//     vector<set<string>> groups;
//     set<string> group; // 16一组chunkid
//     set<string> usedChunks;
//     table.initOriginalFeatureCorelationMatrix(); // 初始化子特征之间的相似度矩阵
//     for (auto it : table.original_features_corelation_matrix)
//     {
//         for (auto it2 : it.second)
//         {
//             if (it2.second == 1)
//             {
//                 corelationIsOne++;
//             }
//             totalMatrixSize++;
//         }
//     }

//     for (auto it : table.original_features_key_table)
//     {
//         if (it.second.size() == 1)
//         {
//             singeFeature++;
//         }
//         totalOriginalFeature++;
//     }

//     // 按照feature的chunk数量从大到小排序，逐个取出feature的chunk
//     for (auto it : sorted_original_features)
//     {
//         if (table.original_feature_key_table[it].size() < 16)
//         {
//             feature_less_than_16++;
//         }
//         // if (table.original_feature_key_table[it].size() == 1)
//         // {
//         //     singeFeature++;
//         // }
//         // 遍历这个feature的所有chunk
//         for (auto id : table.original_feature_key_table[it])
//         {
//             // 如果这个chunk已经被用过了
//             if (usedChunks.find(id) != usedChunks.end())
//             {
//                 continue;
//             }

//             group.insert(id);
//             usedChunks.insert(id);

//             if (group.size() == 16)
//             {
//                 groups.push_back(group);
//                 group.clear();
//             }
//         }

//         // 如果这个feature的chunk数量不足16个，那么找到这个group中频率最高的子特征值，根据子特征值继续添加chunk
//         if (group.size() > 0)
//         {
//             notFullGroupNum++;
//             set<feature_t> usedFeatures; // 记录已经使用过的feature
//             usedFeatures.insert(it);
//             while (group.size() < 16 && group.size() > 0)
//             {
//                 feature_t most_freq_feature = find_most_freq_feature(group, usedFeatures);
//                 usedFeatures.insert(most_freq_feature);
//                 if (most_freq_feature == 0)
//                 {
//                     break;
//                 }
//                 for (auto id : table.original_features_key_table[most_freq_feature])
//                 {
//                     if (usedChunks.find(id) != usedChunks.end())
//                     {
//                         continue;
//                     }
//                     group.insert(id);
//                     usedChunks.insert(id);
//                     if (group.size() == 16)
//                     {
//                         groups.push_back(group);
//                         group.clear();
//                         break;
//                     }
//                 }
//             }
//         }
//         // 如果这个feature的chunk数量依然不足16个，那么直接将这个group添加到groups中
//         if (group.size() > 0)
//         {
//             stillNotFullGroupNum++;
//             groups.push_back(group);
//             group.clear();
//         }
//     }