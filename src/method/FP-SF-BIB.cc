#include "../../include/FP-SF-BIB.h"

FPLz4SFBlockinBlock::FPLz4SFBlockinBlock()
{
    lz4ChunkBuffer = (uint8_t *)malloc(MAX_GROUP_SIZE * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // MAX_GROUP_SIZE * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(MAX_GROUP_SIZE * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}
FPLz4SFBlockinBlock::FPLz4SFBlockinBlock(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(MAX_GROUP_SIZE * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // MAX_GROUP_SIZE * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(MAX_GROUP_SIZE * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

FPLz4SFBlockinBlock::~FPLz4SFBlockinBlock()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

// 找到一个group中频率最高的feature，排除掉当前feature
feature_t FPLz4SFBlockinBlock::find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeatures)
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
feature_t FPLz4SFBlockinBlock::find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature)
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

void FPLz4SFBlockinBlock::groupmerge(vector<set<string>> &sets, int t, int k)
{
    while (true)
    {
        int n = sets.size();
        int min_diff = INT_MAX;
        pair<int, int> best_pair(-1, -1);
        bool found_k = false;

        for (int i = 0; i < n; ++i)
        {
            for (int j = i + 1; j < n; ++j)
            {
                set<string> temp(sets[i].begin(), sets[i].end());
                temp.insert(sets[j].begin(), sets[j].end());
                int new_size = temp.size();

                if (new_size <= t)
                {
                    int diff = t - new_size;
                    if (diff < min_diff)
                    {
                        min_diff = diff;
                        best_pair = make_pair(i, j);
                    }
                }
            }
        }
        if (best_pair.first == -1 || best_pair.second == -1)
            break; // No more pairs can be merged.
        // Merge the two sets.
        sets[best_pair.first].insert(sets[best_pair.second].begin(), sets[best_pair.second].end());

        // Remove the second set from the list as it has been merged into the first one.
        sets.erase(sets.begin() + best_pair.second);
    }
}

void FPLz4SFBlockinBlock::ProcessOneTrace()
{
    int count = 0;
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
            // if (count >= max)
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
            GenerateHash(mdCtx, tmpChunk.chunkContent, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
            FP_Insert(hashStr, tmpChunk.chunkID);

            chunkSet.push_back(tmpChunk);
            // dataWrite_->Chunk_Insert(tmpChunk);

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

    totalFeature += table.original_feature_key_table.size();
    vector<set<string>> finishedGroups;
    unordered_map<string, set<string>> FPunfinishedGroups;
    set<string> finishedChunks;
    set<string> unfinishedChunks;
    vector<set<string>> adjGroups;
    set<string> tmpGroup; // MAX_GROUP_SIZE一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);
    if (!out.is_open())
    {
        tool::Logging(myName_.c_str(), "open file failed\n");
        return;
    }
    map<feature_t, set<string>> feature_FP_Table;
    // set<string> usedChunks;

    // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());
    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());

    // 根据FP进行初次分组
    frequency_table.clear();
    for (auto it : this->FPindex)
    {
        frequency_table[it.second.size()]++;
        auto element = it.second.begin();
        auto id = *element;
        feature_FP_Table[table.original_key_feature_table_[to_string(id)]].insert(it.first);
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
    // 对于每一个feature进行分组
    for (auto feature : table.feature_key_table_)
    {
        tmpGroup.clear();
        for (auto id : feature.second)
        {
            if (finishedChunks.find(id) != finishedChunks.end())
            {
                continue;
            }
            tmpGroup.insert(id);
            unfinishedChunks.erase(id);
            finishedChunks.insert(id);
            if (tmpGroup.size() == MAX_GROUP_SIZE)
            {
                finishedGroups.push_back(tmpGroup);
                tmpGroup.clear();
            }
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
                unfinishedChunks.insert(id);
            }
            tmpGroup.clear();
        }
    }

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

    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    tool::Logging(myName_.c_str(), "second Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "second Unfinished chunk num is %d\n", unfinishedChunks.size());
    // cout << "----------------------------BIB------------------------------" << endl;
    // 对每一个1块进行FastCDC,找到一个代表块,代表块的chunkid为原来块的id
    vector<representChunk_t> representChunkSet;
    FeatureIndexTable representTable;
    map<super_feature_t, vector<set<string>>> Rfeature_unfinishedGroup;

    for (auto id : unfinishedChunks)
    {
        uint64_t start = 0;
        uint64_t end = 0;

        // 用于dataWrite
        // tmpChunk = dataWrite_->Get_Chunk_Info(stoull(id));
        // ThirdCutPointHashMin(tmpChunk.chunkContent, tmpChunk.chunkSize, start, end);
        // representChunk_t representChunk;
        // representChunk.chunkID = stoull(id);
        // representChunk.offset_start = start;
        // representChunk.offset_end = end;
        // representChunkSet.push_back(representChunk);
        // string representChunkContent((char *)tmpChunk.chunkContent + start, end - start);
        // representTable.Put(id, (char *)representChunkContent.c_str());
        // Rfeature_unfinishedGroup[representTable.original_key_feature_table_[id][0]].push_back({id});

        // 用于内存读写
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        ThirdCutPointHashMin(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointHashMin_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        representChunk_t representChunk;
        representChunk.chunkID = stoull(id);
        representChunk.offset_start = start;
        representChunk.offset_end = end;
        representChunkSet.push_back(representChunk);
        string representChunkContent((char *)chunkSet[stoull(id)].chunkContent + start, end - start);
        representTable.Put(id, (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[id][0]].push_back({id});
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

        // 用于dataWrite
        // tmpChunk = dataWrite_->Get_Chunk_Info(stoull(id));
        // ThirdCutPointHashMin(tmpChunk.chunkContent, tmpChunk.chunkSize, start, end);
        // representChunk_t representChunk;
        // representChunk.chunkID = stoull(id);
        // representChunk.offset_start = start;
        // representChunk.offset_end = end;
        // representChunkSet.push_back(representChunk);
        // string representChunkContent((char *)tmpChunk.chunkContent + start, end - start);
        // representTable.Put(id, (char *)representChunkContent.c_str());
        // Rfeature_unfinishedGroup[representTable.key_feature_table_[id][0]].push_back(*group);
        // group = finishedGroups.erase(group);

        // 用于内存读写
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        ThirdCutPointHashMin(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointHashMin_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        representChunk_t representChunk;
        representChunk.chunkID = stoull(id);
        representChunk.offset_start = start;
        representChunk.offset_end = end;
        representChunkSet.push_back(representChunk);
        string representChunkContent((char *)chunkSet[stoull(id)].chunkContent + start, end - start);
        representTable.Put(id, (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[id][0]].push_back(*group);
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
        if (it.first == 0 || Rfeature_unfinishedGroup[it.first].size() > 1000)
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
            if (tmpGroup.size() > 0)
            {
                finishedGroups.push_back(tmpGroup);
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.erase(id);
                    finishedChunks.insert(id);
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
        groupmerge(groups, MAX_GROUP_SIZE, 1);
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
        if (tmpGroup.size() > 0)
        {
            finishedGroups.push_back(tmpGroup);
            for (auto id : tmpGroup)
            {
                unfinishedChunks.erase(id);
                finishedChunks.insert(id);
            }
            tmpGroup.clear();
        }
    }
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

    tool::Logging(myName_.c_str(), "third Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "third Unfinished chunk num is %d\n", unfinishedChunks.size());

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
    vector<set<string>> leftGroups;
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
    // cout << "---------------------------------1 chunk---------------------------------" << endl;
    groupNum += finishedGroups.size();

    for (auto it : finishedGroups)
    {
        compressedChunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "tmp chunk num is %d\n", compressedChunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
    tool::Logging(myName_.c_str(), "%d chunk with bibfeature is zero\n", representTable.original_feature_key_table[0].size());
    clusterSize = 0;
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
            memcpy(clusterBuffer + clusterSize, chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize);

            // Chunk_t GroupTmpChunk = dataWrite_->Get_Chunk_Info(stoull(id));
            // memcpy(clusterBuffer + clusterSize, GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize);

            // if (GroupTmpChunk.loadFromDisk)
            // {
            //     free(GroupTmpChunk.chunkContent);
            // }
            auto end = std::chrono::high_resolution_clock::now();
            clustringTime += end - start;

            clusterCnt++;
            ChunkNum++;
            // clusterSize += GroupTmpChunk.chunkSize;
            clusterSize += chunkSet[stoull(id)].chunkSize;
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
