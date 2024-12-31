#include "../../include/bruteforce.h"

bruteforce::bruteforce()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}
bruteforce::bruteforce(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

bruteforce::~bruteforce()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

// 找到一个group中频率最高的feature，排除掉当前feature
feature_t bruteforce::find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeatures)
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
feature_t bruteforce::find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature)
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

void bruteforce::groupmerge(vector<set<string>> &sets, int t, int k)
{
    while (true)
    {
        int n = sets.size();
        int min_diff = INT_MAX;
        int min_total_size = INT_MAX;
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
                        // Check if either set has size k
                        if (sets[i].size() == k || sets[j].size() == k)
                        {
                            min_diff = diff;
                            best_pair = make_pair(i, j);
                            found_k = true;
                        }
                        else if (!found_k)
                        {
                            min_diff = diff;
                            best_pair = make_pair(i, j);
                        }
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

void bruteforce::ProcessOneTrace()
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
            auto start = std::chrono::high_resolution_clock::now();
            // table.PutOrignals(ss.str(), (char *)tmpChunk.chunkContent);
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
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
    if (isLastFile)
    {
        vector<set<string>> finishedGroups;
        set<string> finishedChunks;
        set<string> unfinishedChunks;
        set<string> tmpGroup; // 16一组chunkid
        ofstream out("../frequencyTable.txt", ios::app);
        if (!out.is_open())
        {
            tool::Logging(myName_.c_str(), "open file failed\n");
            return;
        }
        vector<bool> isMerged(experimentChunkCount, false);
        vector<vector<int64_t>> mergedReduceSizeTable(experimentChunkCount, vector<int64_t>(experimentChunkCount, 0));
        int64_t bestMergeReduceSize = 0;
        pair<int, int> bestMergePair;
        map<int, int64_t> mergedGroupSize;
        map<int, set<int>> mergedGroup;
        for (int i = 0; i < experimentChunkCount; i++)
        {
            memcpy(clusterBuffer, chunkSet[i].chunkContent, chunkSet[i].chunkSize);
            int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, chunkSet[i].chunkSize, chunkSet[i].chunkSize, 3);
            if (compressedSize <= 0)
            {
                compressedSize = chunkSet[i].chunkSize;
            }
            mergedGroupSize[i] = compressedSize;
            mergedGroup[i].insert(i);
        }
        for (int i = 0; i < experimentChunkCount; i++)
        {
            if (isMerged[i])
            {
                continue;
            }
            for (int j = i + 1; j < experimentChunkCount; j++)
            {
                if (isMerged[j])
                {
                    continue;
                }
                memcpy(clusterBuffer, chunkSet[i].chunkContent, chunkSet[i].chunkSize);
                memcpy(clusterBuffer + chunkSet[i].chunkSize, chunkSet[j].chunkContent, chunkSet[j].chunkSize);
                int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, chunkSet[i].chunkSize + chunkSet[j].chunkSize, chunkSet[i].chunkSize + chunkSet[j].chunkSize, 3);
                if (compressedSize <= 0)
                {
                    compressedSize = chunkSet[i].chunkSize + chunkSet[j].chunkSize;
                }
                int reduceSize = (mergedGroupSize[i] + mergedGroupSize[j]) - compressedSize;
                mergedReduceSizeTable[i][j] = reduceSize;
                if (reduceSize > bestMergeReduceSize)
                {
                    bestMergeReduceSize = reduceSize;
                    bestMergePair = make_pair(i, j);
                }
            }
        }
        tool::Logging(myName_.c_str(), "best pair is %d, %d\n", bestMergePair.first, bestMergePair.second);
        isMerged[bestMergePair.second] = true;
        mergedGroup[bestMergePair.first].insert(bestMergePair.second);
        mergedGroup[bestMergePair.second].clear();
        mergedGroupSize[bestMergePair.first] = (mergedGroupSize[bestMergePair.first] + mergedGroupSize[bestMergePair.second]) - bestMergeReduceSize;
        tool::Logging(myName_.c_str(), "reduce size: %d\n", bestMergeReduceSize);

        while (true)
        {
            bestMergeReduceSize = 0;
            // 根据bestpair更新矩阵
            clusterSize = 0;
            for (auto id : mergedGroup[bestMergePair.first])
            {
                memcpy(clusterBuffer + clusterSize, chunkSet[id].chunkContent, chunkSet[id].chunkSize);
                clusterSize += chunkSet[id].chunkSize;
            }
            for (int i = 0; i < experimentChunkCount; i++)
            {
                if (isMerged[i] || i == bestMergePair.first)
                {
                    continue;
                }
                if (mergedGroup[bestMergePair.first].find(i) != mergedGroup[bestMergePair.first].end())
                {
                    continue;
                }
                for (auto id : mergedGroup[i])
                {
                    memcpy(clusterBuffer + clusterSize, chunkSet[id].chunkContent, chunkSet[id].chunkSize);
                    clusterSize += chunkSet[id].chunkSize;
                }
                int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
                if (compressedSize <= 0)
                {
                    compressedSize = clusterSize;
                }
                int64_t mergeSize = mergedGroupSize[bestMergePair.first] + mergedGroupSize[i] - compressedSize;
                mergedReduceSizeTable[bestMergePair.first][i] = mergeSize;
            }
            // 寻找新的bestpair
            bestMergePair = make_pair(-1, -1);
            for (int i = 0; i < experimentChunkCount; i++)
            {
                if (isMerged[i])
                {
                    continue;
                }
                for (int j = i + 1; j < experimentChunkCount; j++)
                {
                    if (isMerged[j] || mergedGroup[i].size() + mergedGroup[j].size() > MAX_GROUP_SIZE)
                    {
                        continue;
                    }
                    if (mergedReduceSizeTable[i][j] > bestMergeReduceSize)
                    {
                        bestMergeReduceSize = mergedReduceSizeTable[i][j];
                        bestMergePair = make_pair(i, j);
                    }
                }
            }
            if (bestMergePair.first == -1 || bestMergePair.second == -1)
            {
                tool::Logging(myName_.c_str(), "no best pair found\n");
                break;
            }
            tool::Logging(myName_.c_str(), "best pair is %d, %d\n", bestMergePair.first, bestMergePair.second);
            isMerged[bestMergePair.second] = true;
            mergedGroup[bestMergePair.first].insert(mergedGroup[bestMergePair.second].begin(), mergedGroup[bestMergePair.second].end());
            mergedGroup[bestMergePair.second].clear();
            mergedGroupSize[bestMergePair.first] = mergedGroupSize[bestMergePair.first] + mergedGroupSize[bestMergePair.second] - bestMergeReduceSize;
            tool::Logging(myName_.c_str(), "reduce size: %d\n", bestMergeReduceSize);
        }
        for (auto group : mergedGroup)
        {
            tmpGroup.clear();
            if (group.second.size() == 0)
                continue;
            for (auto id : group.second)
            {
                tmpGroup.insert(to_string(id));
                cout << id << " ";
            }
            finishedGroups.push_back(tmpGroup);
            cout << endl;
        }

        frequency_table.clear();
        for (auto it : finishedGroups)
        {
            frequency_table[it.size()]++;
        }
        out << "group size, frequency" << endl;
        for (auto it : frequency_table)
        {
            out << it.first << ", " << it.second << endl;
        }
        clusterSize = 0;
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
    }
    recieveQueue->done_ = false;
    return;
}

void bruteforce::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}