#include "../../include/characterFrequency.h"

characterFrequency::characterFrequency()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

characterFrequency::characterFrequency(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

characterFrequency::~characterFrequency()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void characterFrequency::groupmerge(vector<set<string>> &sets, int t)
{
    while (true)
    {
        int n = sets.size();
        int min_diff = INT_MAX;
        int min_total_size = INT_MAX;
        pair<int, int> best_pair(-1, -1);

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

std::array<double, 256> characterFrequency::calculateFrequency(const Chunk_t &chunk)
{
    std::array<double, 256> frequency{0.0};
    for (int i = 0; i < chunk.chunkSize; i++)
    {
        frequency[chunk.chunkContent[i]] += 1.0;
    }
    for (int i = 0; i < 256; i++)
    {
        frequency[i] /= chunk.chunkSize;
    }
    return frequency;
}

double characterFrequency::calculateEntropy(const Chunk_t &chunk)
{
    std::array<double, 256> frequency = calculateFrequency(chunk);
    double entropy = 0;
    for (int i = 0; i < 256; i++)
    {
        entropy += frequency[i] * log2(frequency[i]);
    }
    entropy = -entropy;
    return entropy;
}
std::vector<set<string>> characterFrequency::clusterChunks(const std::vector<Chunk_t> &chunkSet, int clusterNum)
{
    cv::Mat data(chunkSet.size(), 256, CV_32F);
    for (int i = 0; i < chunkSet.size(); i++)
    {
        auto frequency = calculateFrequency(chunkSet[i]);
        for (int j = 0; j < 256; j++)
        {
            data.at<float>(i, j) = frequency[j];
        }
    }
    cv::Mat labels, centers;
    cv::kmeans(data, clusterNum, labels,
               cv::TermCriteria(cv::TermCriteria::EPS + cv::TermCriteria::COUNT, 10000, 0.0001), 3, cv::KMEANS_PP_CENTERS, centers);
    std::vector<set<string>> groups(clusterNum);
    for (int i = 0; i < chunkSet.size(); i++)
    {
        groups[labels.at<int>(i)].insert(to_string(i));
    }
    return groups;
}
// 特征提取：将数据块转换为浮点向量
std::vector<float> characterFrequency::extractFeature(const Chunk_t &chunk)
{

    std::vector<float> feature;
    for (int i = 0; i < chunk.chunkSize; i++)
    {
        feature.push_back((float)chunk.chunkContent[i]);
    }
    return feature;
}

void characterFrequency::ProcessOneTrace()
{
    int count = 0;
    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        vector<float> tmpfeature;
        string hashStr;
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

            table.PutOrignal(ss.str(), (char *)tmpChunk.chunkContent);
            GenerateHash(mdCtx, tmpChunk.chunkContent, tmpChunk.chunkSize, hashBuf);
            auto end = std::chrono::high_resolution_clock::now();
            featureExtractTime += end - start;
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
    // vector<feature_t> sorted_original_features = table.sortFeatureBySetSize();
    vector<set<string>> finishedGroups;
    vector<set<string>> unfinishedGroups;
    unordered_map<string, set<string>> FPunfinishedGroups;
    set<string> finishedChunks;
    set<string> unfinishedChunks;
    set<string> tmpGroup; // 16一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);
    map<feature_t, set<string>> feature_FP_Table;
    vector<set<string>> featureGroups;
    vector<Chunk_t> unfinishedChunkSet;

    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    // 根据FP进行初次分组
    frequency_table.clear();
    for (auto it : this->FPindex)
    {
        frequency_table[it.second.size()]++;
        auto element = it.second.begin();
        auto id = *element;
        feature_FP_Table[table.original_key_feature_table_[to_string(id)]].insert(it.first);
        tmpGroup.clear();
        if (it.second.size() >= 16)
        {
            for (auto id : it.second)
            {
                tmpGroup.insert(to_string(id));
                finishedChunks.insert(to_string(id));
                if (tmpGroup.size() == 16)
                {
                    finishedGroups.push_back(tmpGroup);
                    tmpGroup.clear();
                }
            }
            if (tmpGroup.size() > 0)
            {
                for (auto id : tmpGroup)
                {
                    finishedChunks.erase(id);
                    unfinishedChunks.insert(id);
                }
                FPunfinishedGroups[it.first].insert(tmpGroup.begin(), tmpGroup.end());
                tmpGroup.clear();
            }
        }
        else
        {
            for (auto id : it.second)
            {
                tmpGroup.insert(to_string(id));
                unfinishedChunks.insert(to_string(id));
            }
            FPunfinishedGroups[it.first].insert(tmpGroup.begin(), tmpGroup.end());
            tmpGroup.clear();
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
    for (auto feature : table.original_feature_key_table)
    {
        tmpGroup.clear();
        for (auto id : feature.second)
        {
            if (finishedChunks.find(id) != finishedChunks.end())
            {
                continue;
            }
            tmpGroup.insert(id);
        }
        if (tmpGroup.size() > 15)
        {
            featureGroups.push_back(tmpGroup);
        }
    }
    tool::Logging(myName_.c_str(), "sf Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "sf Unfinished chunk num is %d\n", unfinishedChunks.size());
    tmpGroup.clear();
    for (auto group : featureGroups)
    {
        map<int, int> idIndex;
        unfinishedChunkSet.clear();
        for (auto id : group)
        {
            unfinishedChunkSet.push_back(chunkSet[stoull(id)]);
            idIndex[unfinishedChunkSet.size() - 1] = stoull(id);
        }
        unfinishedGroups.clear();
        unfinishedGroups = clusterChunks(unfinishedChunkSet, unfinishedChunkSet.size() / MAX_GROUP_SIZE);
        // 把大于MAX_GROUP_SIZE的group拆开
        vector<set<string>> tmpUnfinishedGroups;
        for (auto group = unfinishedGroups.begin(); group != unfinishedGroups.end(); group++)
        {
            tmpGroup.clear();
            if (group->size() > MAX_GROUP_SIZE)
            {
                for (auto id : *group)
                {
                    tmpGroup.insert(id);
                    if (tmpGroup.size() == MAX_GROUP_SIZE)
                    {
                        tmpUnfinishedGroups.push_back(tmpGroup);
                        tmpGroup.clear();
                    }
                }
                if (tmpGroup.size() > 0)
                {
                    tmpUnfinishedGroups.push_back(tmpGroup);
                    tmpGroup.clear();
                }
            }
            else
            {
                tmpUnfinishedGroups.push_back(*group);
            }
        }
        unfinishedGroups = tmpUnfinishedGroups;
        groupmerge(unfinishedGroups, MAX_GROUP_SIZE);
        for (auto group : unfinishedGroups)
        {
            for (auto id : group)
            {
                string index = to_string(idIndex[stoull(id)]);
                if (finishedChunks.find(index) != finishedChunks.end())
                {
                    cout << "chunk " << index << " is in two group" << endl;
                }
                finishedChunks.insert(index);
                unfinishedChunks.erase(index);
            }
            finishedGroups.push_back(group);
        }
    }
    tool::Logging(myName_.c_str(), "cluster Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "cluster Unfinished chunk num is %d\n", unfinishedChunks.size());

    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        // for (auto id : it)
        // {
        //     cout << id << " ";
        // }
        // cout << endl;
        frequency_table[it.size()]++;
    }
    out << "group size, frequency" << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    tmpGroup.clear();
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

    int tmpchunkNum = 0;
    for (auto it : finishedGroups)
    {
        tmpchunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "tmp chunk num is %d\n", tmpchunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
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
    recieveQueue->done_ = false;
    return;
}

void characterFrequency::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}
