#include "../../include/absMethod.h"

absMethod::absMethod() : chunker_(0)
{
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}
absMethod::absMethod(uint64_t ExchunkSize) : chunker_(0, ExchunkSize)
{
}
absMethod::~absMethod()
{
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
