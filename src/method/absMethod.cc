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

void absMethod::SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq)
{
    recieveQueue = mq;
    return;
}
