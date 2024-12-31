#include "../../include/LSH.h"

LocalitySensitiveHash::LocalitySensitiveHash()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

LocalitySensitiveHash::LocalitySensitiveHash(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

LocalitySensitiveHash::~LocalitySensitiveHash()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void LocalitySensitiveHash::groupmerge(vector<set<string>> &sets, int t, int k)
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
// 特征提取：将数据块转换为浮点向量
std::vector<float> LocalitySensitiveHash::extractFeature(const Chunk_t &chunk)
{

    std::vector<float> feature;
    for (int i = 0; i < chunk.chunkSize; i++)
    {
        feature.push_back((float)chunk.chunkContent[i]);
    }
    return feature;
}
// 生成投影向量
std::vector<std::vector<float>> LocalitySensitiveHash::genernateProjection(int featureDim)
{
    std::random_device rd;                            // 随机设备
    std::mt19937 gen(42);                             // 固定种子（例如：42）
    std::normal_distribution<float> dist(0.0f, 1.0f); // 高斯分布
    std::vector<std::vector<float>> projections(HASH_DIM, std::vector<float>(featureDim));
    for (auto &projection : projections)
    {
        for (auto &value : projection)
        {
            value = dist(gen);
        }
    }
    return projections;
}
// 构建哈希表
HashTable LocalitySensitiveHash::buildHashTable(const std::vector<std::vector<float>> &projections, const std::vector<std::vector<float>> &features)
{
    HashTable hashTable;
    for (size_t i = 0; i < features.size(); ++i)
    {
        HashSignature signature = hashBlock(features[i], projections);
        hashTable[signature].push_back(i);
    }
    return hashTable;
}
// 生成哈希签名
HashSignature LocalitySensitiveHash::hashBlock(const std::vector<float> &feature, const std::vector<std::vector<float>> &projections)
{
    HashSignature signature;
    for (size_t i = 0; i < HASH_DIM; i++)
    {
        float dotProduct = 0.0f;
        for (size_t j = 0; j < feature.size(); j++)
        {
            dotProduct += feature[j] * projections[i][j];
        }
        signature[i] = (dotProduct >= 0);
    }
    return signature;
}
void LocalitySensitiveHash::ProcessOneTrace()
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
    // vector<feature_t> sorted_original_features = table.sortFeatureBySetSize();
    vector<set<string>> finishedGroups;
    // vector<set<string>> unfinishedGroups;
    unordered_map<string, set<string>> FPunfinishedGroups;
    set<string> finishedChunks;
    set<string> unfinishedChunks;
    set<string> tmpGroup; // 16一组chunkid
    ofstream out("../frequencyTable.txt", ios::app);

    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    projections = genernateProjection(CHUNK_SIZE);
    features = vector<vector<float>>(chunkSet.size());
    for (auto &chunk : chunkSet)
    {
        features[chunk.chunkID] = extractFeature(chunk);
    }

    auto hashTable = buildHashTable(projections, features);
    tmpGroup.clear();
    std::cout << "Grouped DataBlock IDs by Hash Signature:" << std::endl;
    for (const auto &entry : hashTable)
    {
        std::cout << "Hash Signature: " << entry.first << " -> DataBlock IDs: ";
        for (int id : entry.second)
        {
            std::cout << id << " ";
            if (entry.second.size() < 2)
            {
                unfinishedChunks.insert(to_string(id));
                continue;
            }
            tmpGroup.insert(to_string(id));
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
        std::cout << std::endl;
    }
    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "LSH group size, frequency" << endl;
    out << 1 << "," << unfinishedChunks.size() << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
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
    int tmpchunkNum = 0;
    for (auto it : finishedGroups)
    {
        tmpchunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "tmp chunk num is %d\n", tmpchunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
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

void LocalitySensitiveHash::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}
