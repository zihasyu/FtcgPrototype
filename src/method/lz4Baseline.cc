#include "../../include/method/lz4Baseline.h"
// non Dedup
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
            tmpChunk.chunkID = ChunkID++;
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

            Chunk_Insert(tmpChunk);
            totalChunkNum++;
        }
    }
    if (!isLastFile)
    {
        recieveQueue->done_ = false;
        return;
    }
    totalFeature += table.original_feature_key_table.size();

    // vector<set<uint64_t>> unfinishedGroups;
    set<uint64_t> finishedChunks;
    set<uint64_t> unfinishedChunks;
    vector<set<uint64_t>> adjGroups;
    set<uint64_t> group; // 16一组chunkid
    // set<string> usedChunks;
    // 对于每一个feature进行分组
    // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());
    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());
    for (auto it : table.feature_key_table_)
    {
        for (auto id : it.second)
        {

            // 如果这个chunk已经被分组过了
            if (finishedChunks.find(stoull(id)) != finishedChunks.end() || unfinishedChunks.find(stoull(id)) != unfinishedChunks.end())
            {
                continue;
            }
            group.insert(stoull(id));
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
