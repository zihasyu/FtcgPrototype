#include "../../include/method/absMethod.h"

absMethod::absMethod() : chunker_(0)
{
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    hashStr.assign(CHUNK_HASH_SIZE, 0);
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
}
absMethod::absMethod(uint64_t ExchunkSize) : chunker_(0, ExchunkSize)
{
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    hashStr.assign(CHUNK_HASH_SIZE, 0);
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
}
absMethod::~absMethod()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void absMethod::GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash)
{
    int expectedHashSize = 0;

    if (!EVP_DigestInit_ex(mdCtx, EVP_sha256(), NULL))
    {
        fprintf(stderr, "CryptoTool: Hash init error.\n");
        exit(EXIT_FAILURE);
    }
    expectedHashSize = 32;

    if (!EVP_DigestUpdate(mdCtx, dataBuffer, dataSize))
    {
        fprintf(stderr, "CryptoTool: Hash error.\n");
        exit(EXIT_FAILURE);
    }
    uint32_t hashSize;
    if (!EVP_DigestFinal_ex(mdCtx, hash, &hashSize))
    {
        fprintf(stderr, "CryptoTool: Hash error.\n");
        exit(EXIT_FAILURE);
    }

    if (hashSize != expectedHashSize)
    {
        fprintf(stderr, "CryptoTool: Hash size error.\n");
        exit(EXIT_FAILURE);
    }

    EVP_MD_CTX_reset(mdCtx);
    return;
}

set<int> absMethod::FP_Find(string fp)
{
    auto it = FPindex.find(fp);
    // cout << FPindex.size() << endl;
    if (it != FPindex.end())
    {
        // cout << "find fp" << endl;
        return it->second;
    }
    else
    {
        // cout << "not find fp" << endl;
        return {};
    }
}

bool absMethod::FP_Insert(string fp, int chunkid)
{
    FPindex[fp].insert(chunkid);
    chunkid_FP_table[chunkid] = fp;
    return true;
}

void absMethod::ThirdCutPointSizeMax(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end)
{
    std::vector<uint64_t> cutPoints;
    uint64_t offset = 0;
    uint64_t maxCutPoint = 0;
    uint64_t maxCutPointStart = 0;

    while (offset < len)
    {
        uint64_t cp = chunker_.CutPointFastCDC(src + offset, len - offset);
        cutPoints.push_back(cp);
        if (cp > maxCutPoint)
        {
            maxCutPoint = cp;
            maxCutPointStart = offset;
            //(src+maxCutPointStart -> src+maxCutPointStart+maxCutPoint) is ChunkInChunk
        }
        offset += cp;
    }
    start = maxCutPointStart;
    end = maxCutPointStart + maxCutPoint;
    if (start == 0 && end == 0)
    {
        cout << "error:start is 0 " << "end :" << end << endl;
    }
}

void absMethod::ThirdCutPointHashMin(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end)
{
    uint64_t offset = 0;
    uint64_t HashMinCutPoint = 0;
    uint64_t HashMinCutPointStart = 0;
    std::string minHashStr;
    std::string currentHashStr(32, 0);

    while (offset < len)
    {
        uint64_t cp = chunker_.CutPointFastCDC(src + offset, len - offset);
        GenerateHash(mdCtx, (uint8_t *)src + offset, cp, hashBuf);
        currentHashStr.assign((char *)hashBuf, 32);
        if (minHashStr.empty() || currentHashStr < minHashStr)
        {
            minHashStr = currentHashStr;
            HashMinCutPoint = cp;
            HashMinCutPointStart = offset;
        }
        offset += cp;
    }
    start = HashMinCutPointStart;
    end = HashMinCutPointStart + HashMinCutPoint;
    if (start == 0 && end == 0)
    {
        cout << "error:start is 0 " << "end :" << end << endl;
    }
}

void absMethod::ThirdCutPointSizeMax_remove(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end)
{
    std::vector<uint64_t> cutPoints;
    uint64_t offset = 0;
    uint64_t maxCutPoint = 0;
    uint64_t maxCutPointStart = 0;
    bool firstCutPoint = true;

    while (offset < len)
    {
        uint64_t cp = chunker_.CutPointFastCDC(src + offset, len - offset);
        cutPoints.push_back(cp);

        // Skip the initial boundary to the first cut point and the last cut point
        if (!firstCutPoint && offset + cp < len)
        {
            if (cp > maxCutPoint)
            {
                maxCutPoint = cp;
                maxCutPointStart = offset;
                //(src+maxCutPointStart -> src+maxCutPointStart+maxCutPoint) is ChunkInChunk
            }
        }

        firstCutPoint = false;
        offset += cp;
    }
    start = maxCutPointStart;
    end = maxCutPointStart + maxCutPoint;
    if (start == 0 && end == 0)
    {
        // cout << "hint: just one cut point" << endl;
        ThirdCutPointSizeMax(src, len, start, end);
    }
    if (start == 0 && end == 0)
    {
        cout << "error:start is 0" << endl;
    }
}

void absMethod::ThirdCutPointHashMin_remove(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end)
{
    uint64_t offset = 0;
    uint64_t HashMinCutPoint = 0;
    uint64_t HashMinCutPointStart = 0;
    std::string hashStr;
    std::string minHashStr;
    std::string currentHashStr(32, 0);
    bool firstCutPoint = true;

    while (offset < len)
    {
        uint64_t cp = chunker_.CutPointFastCDC(src + offset, len - offset);
        GenerateHash(mdCtx, (uint8_t *)src + offset, cp, hashBuf);
        currentHashStr.assign((char *)hashBuf, 32);

        // Skip the initial boundary to the first cut point and the last cut point
        if (!firstCutPoint && offset + cp < len)
        {
            if (minHashStr.empty() || currentHashStr < minHashStr)
            {
                minHashStr = currentHashStr;
                HashMinCutPoint = cp;
                HashMinCutPointStart = offset;
            }
        }

        firstCutPoint = false;
        offset += cp;
    }
    start = HashMinCutPointStart;
    end = HashMinCutPointStart + HashMinCutPoint;
    if (start == 0 && end == 0)
    {
        // cout << "hint: just one cut point" << endl;
        ThirdCutPointSizeMax(src, len, start, end);
    }
    if (start == 0 && end == 0)
    {
        cout << "error:start is 0" << endl;
    }
}

void absMethod::groupmerge(vector<set<uint64_t>> &sets, int t)
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
                set<uint64_t> temp(sets[i].begin(), sets[i].end());
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

void absMethod::Chunk_Insert(const Chunk_t &chunk)
{
    if (isDisk)
        dataWrite_->Chunk_Insert(chunk);
    else
        chunkSet.push_back(chunk);
}

Chunk_t absMethod::Get_Chunk_Info(int id)
{
    if (isDisk)
        return dataWrite_->Get_Chunk_Info(id);
    else
        return chunkSet[id];
};

bool absMethod::IsDedup(Chunk_t &chunk)
{
    GenerateHash(mdCtx, chunk.chunkContent, chunk.chunkSize, hashBuf);
    hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
    auto it = Dedupindex.find(hashStr);
    // dedup chunk
    if (it != Dedupindex.end())
    {
        chunk.chunkID = it->second;
        dataWrite_->RecipeMap[fileName].push_back(chunk.chunkID);
        return true;
    }
    // unique chunk
    else
    {
        chunk.chunkID = ChunkID++;
        Dedupindex[hashStr] = chunk.chunkID;
        dataWrite_->RecipeMap[fileName].push_back(chunk.chunkID);
        return false;
    }
}

void absMethod::CompressionToFinishedGroup()
{
    tool::Logging(myName_.c_str(), "Compression start\n");
    // for (auto group : finishedGroups)
    string outfileName = "./CompressionFiles/" + myName_;
    ofstream outfile(outfileName, std::ios::binary);
    for (uint32_t GroupID = 0; GroupID < finishedGroups.size(); GroupID++)
    {
        groupLogicalSize[finishedGroups[GroupID].size()] = 0;
        groupCompressedSize[finishedGroups[GroupID].size()] = 0;
        for (auto id : finishedGroups[GroupID]) // finishedGroups is index(group->chunkID)
        {

            auto start = std::chrono::high_resolution_clock::now();
            Chunk_t GroupTmpChunk = Get_Chunk_Info(id);
            chunkSet[id].GroupID = GroupID; // final group id for each chunk, it's index(chunkID->groupID)
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

        // do lz4 compression
        int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
        if (compressedSize <= 0)
        {
            compressedSize = clusterSize;
        }
        outfile.write(reinterpret_cast<const char *>(lz4ChunkBuffer), compressedSize);
        totalCompressedSize += compressedSize;
        // totalLogicalSize += clusterSize;
        groupLogicalSize[finishedGroups[GroupID].size()] += clusterSize;
        groupCompressedSize[finishedGroups[GroupID].size()] += compressedSize;
        clusterNum++;
        clusterCnt = 0;
        clusterSize = 0;
    }
    outfile.close();
    tool::Logging(myName_.c_str(), "Compression finished\n");
}
void absMethod::FinalMerge()
{
    for (auto chunk : chunkSet)
    {
        if (chunk.isGrouped == false)
        {
            tmpGroup.insert(chunk.chunkID);
            if (tmpGroup.size() == 1)
            {
                finishedGroups.push_back(tmpGroup);
                tmpGroup.clear();
            }
        }
    }
    // for (auto chunk : chunkSet)
    // {
    //     if (chunk.isGrouped == false)
    //     {
    //         tmpGroup.insert(chunk.chunkID);
    //         if (tmpGroup.size() == MAX_GROUP_SIZE)
    //         {
    //             finishedGroups.push_back(tmpGroup);
    //             tmpGroup.clear();
    //         }
    //     }
    // }
    if (tmpGroup.size() > 0)
    {
        finishedGroups.push_back(tmpGroup);
        tmpGroup.clear();
    }
}
void absMethod::PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time)
{
    string outfileName = "./ChunkInfo.txt";
    ofstream outfile(outfileName, std::ios::app);
    outfile << "-----------------INSTRUCTION----------------------" << endl;
    outfile << "./Ftcg -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << endl;
    outfile << "-----------------COMPRESSION-----------------------" << endl;
    outfile << "Group Num is " << groupNum << endl;
    outfile << "Total logical size is " << totalLogicalSize << endl;
    outfile << "Total compressed size is " << totalCompressedSize << endl;
    outfile << "Compression ratio is " << (double)totalLogicalSize / (double)totalCompressedSize << endl;
    outfile << "-----------------TIME-----------------------" << endl;
    outfile << "Total time is " << time << "s" << endl;
    outfile << "--------------------------------------------------" << endl;
    outfile.close();
}