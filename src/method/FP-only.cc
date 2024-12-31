#include "../../include/FP-only.h"

FPOnly::FPOnly()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

FPOnly::~FPOnly()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

// 找到一个group中频率最高的feature，排除掉当前feature
feature_t FPOnly::find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeatures)
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
feature_t FPOnly::find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature)
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

void FPOnly::ProcessOneTrace()
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
            stringstream ss;
            ss << tmpChunk.chunkID;
            // auto start = std::chrono::high_resolution_clock::now();
            // table.PutOrignals(ss.str(), (char *)tmpChunk.chunkContent);
            // // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // //  nTransTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // // finesseTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // auto end = std::chrono::high_resolution_clock::now();
            // featureExtractTime += end - start;
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            GenerateHash(mdCtx, tmpChunk.chunkContent, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
            FP_Insert(hashStr, tmpChunk.chunkID);

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

    vector<set<string>> finishedGroups;
    unordered_map<string, set<string>> FPunfinishedGroups;
    set<string> finishedChunks;
    set<string> unfinishedChunks;
    vector<set<string>> adjGroups;
    set<string> tmpGroup; // 16一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);
    map<feature_t, set<string>> feature_FP_Table;

    // set<string> usedChunks;

    // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());

    // 根据FP进行初次分组
    frequency_table.clear();
    for (auto it : this->FPindex)
    {
        frequency_table[it.second.size()]++;
        tmpGroup.clear();
        for (auto id : it.second)
        {
            if (id == *it.second.begin())
            {
                unfinishedChunks.insert(to_string(id));
                continue;
            }
            tmpGroup.insert(to_string(id));
            finishedChunks.insert(to_string(id));
        }
        if (tmpGroup.size() > 0)
        {
            FPdeduplicationTable[to_string(*it.second.begin())] = tmpGroup;
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

    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "first group size, frequency" << endl;
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
        tmpGroup.clear();
    }
    // second_group(unFullGroups, finishedGroups);
    // tool::Logging(myName_.c_str(), "third Finished group num is %d\n", finishedGroups.size());
    groupNum += finishedGroups.size();

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

void FPOnly::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}
