#include "../../include/finesse.h"

Finesse::Finesse()
{
    lz4ChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    deltaMaxChunkBuffer = (uint8_t *)malloc(2 * CONTAINER_MAX_SIZE * sizeof(uint8_t));
    tmpChunkSF = (uint8_t *)malloc(FINESSE_SF_NUM * CHUNK_HASH_SIZE);
    SFindex = new unordered_map<string, vector<int>>[FINESSE_SF_NUM];
}

Finesse::~Finesse()
{
    free(lz4ChunkBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
    free(deltaMaxChunkBuffer);
    free(tmpChunkSF);
    delete[] SFindex;
}

void Finesse::GetSF(unsigned char *ptr, EVP_MD_CTX *mdCtx, uint8_t *SF, int dataSize)
{
    std::mt19937 gen1, gen2; // 优化
    std::uniform_int_distribution<uint32_t> full_uint32_t;
    EVP_MD_CTX *mdCtx_ = mdCtx;
    int BLOCK_SIZE, WINDOW_SIZE;
    int SF_NUM, FEATURE_NUM;
    uint32_t *TRANSPOSE_M;
    uint32_t *TRANSPOSE_A;
    int *subchunkIndex;
    const uint32_t A = 37, MOD = 1000000007;
    uint64_t Apower = 1;
    uint32_t *feature;
    uint64_t *superfeature;
    gen1 = std::mt19937(922);
    gen2 = std::mt19937(314159);
    full_uint32_t = std::uniform_int_distribution<uint32_t>(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());

    BLOCK_SIZE = dataSize;
    WINDOW_SIZE = 48;
    SF_NUM = FINESSE_SF_NUM; // superfeature的个数
    FEATURE_NUM = 12;
    TRANSPOSE_M = new uint32_t[FEATURE_NUM];
    TRANSPOSE_A = new uint32_t[FEATURE_NUM];

    feature = new uint32_t[FEATURE_NUM];
    superfeature = new uint64_t[SF_NUM];
    subchunkIndex = new int[FEATURE_NUM + 1];
    subchunkIndex[0] = 0;
    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        subchunkIndex[i + 1] = (BLOCK_SIZE * (i + 1)) / FEATURE_NUM;
    }
    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        TRANSPOSE_M[i] = ((full_uint32_t(gen1) >> 1) << 1) + 1;
        TRANSPOSE_A[i] = full_uint32_t(gen1);
    }
    for (int i = 0; i < WINDOW_SIZE - 1; ++i)
    {
        Apower *= A;
        Apower %= MOD;
    }
    for (int i = 0; i < FEATURE_NUM; ++i)
        feature[i] = 0;
    for (int i = 0; i < SF_NUM; ++i)
        superfeature[i] = 0; // 初始化

    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        int64_t fp = 0;

        for (int j = subchunkIndex[i]; j < subchunkIndex[i] + WINDOW_SIZE; ++j)
        {
            fp *= A;
            fp += (unsigned char)ptr[j];
            fp %= MOD;
        }

        for (int j = subchunkIndex[i]; j < subchunkIndex[i + 1] - WINDOW_SIZE + 1; ++j)
        {
            if (fp > feature[i])
                feature[i] = fp;

            fp -= (ptr[j] * Apower) % MOD;
            while (fp < 0)
                fp += MOD;
            if (j != subchunkIndex[i + 1] - WINDOW_SIZE)
            {
                fp *= A;
                fp += ptr[j + WINDOW_SIZE];
                fp %= MOD;
            }
        }
    }

    for (int i = 0; i < FEATURE_NUM / SF_NUM; ++i)
    {
        std::sort(feature + i * SF_NUM, feature + (i + 1) * SF_NUM);
    }
    int offset = 0;
    for (int i = 0; i < SF_NUM; ++i)
    {
        uint64_t temp[FEATURE_NUM / SF_NUM];
        for (int j = 0; j < FEATURE_NUM / SF_NUM; ++j)
        {
            temp[j] = feature[j * SF_NUM + i];
        }
        uint8_t *temp3;

        temp3 = (uint8_t *)malloc(FEATURE_NUM / SF_NUM * sizeof(uint64_t));

        memcpy(temp3, temp, FEATURE_NUM / SF_NUM * sizeof(uint64_t));

        uint8_t *temp2;
        temp2 = (uint8_t *)malloc(CHUNK_HASH_SIZE);
        this->GenerateHash(mdCtx_, temp3, sizeof(uint64_t) * FEATURE_NUM / SF_NUM, temp2);
        memcpy(SF + offset, temp2, CHUNK_HASH_SIZE);
        offset = offset + CHUNK_HASH_SIZE;
        free(temp2);
        free(temp3);
    }

    delete[] TRANSPOSE_M;
    delete[] TRANSPOSE_A;
    delete[] feature;
    delete[] superfeature;
    delete[] subchunkIndex;
    return;
}

int Finesse::SF_Find(const char *key, size_t keySize)
{
    string keyStr;
    for (int i = 0; i < FINESSE_SF_NUM; i++)
    {
        keyStr.assign(key + i * CHUNK_HASH_SIZE, CHUNK_HASH_SIZE);
        if (SFindex[i].find(keyStr) != SFindex[i].end())
        {
            // cout<<SFindex[i][keyStr].front()<<endl;
            return SFindex[i][keyStr].back();
        }
    }
    return -1;
}

bool Finesse::SF_Insert(const char *key, size_t keySize, int chunkid)
{
    string keyStr;
    for (int i = 0; i < FINESSE_SF_NUM; i++)
    {
        keyStr.assign(key + i * CHUNK_HASH_SIZE, CHUNK_HASH_SIZE);
        SFindex[i][keyStr].push_back(chunkid);
    }
    return true;
}

void Finesse::GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash)
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

void FinesseIndexTable::Delete(const string &key)
{
    vector<uint64_t> super_features;
    if (GetSuperFeatures(key, &super_features))
    {
        ExecuteDelete(key, super_features);
    }
}

void FinesseIndexTable::ExecuteDelete(const string &key, const vector<uint64_t> &super_features)
{
    for (const uint64_t &sf : super_features)
    {
        feature_key_table_[to_string(sf)].erase(key);
    }
    key_feature_table_.erase(key);
}

void FinesseIndexTable::Put(const string &key, const string &value)
{
    vector<uint64_t> super_features;
    Delete(key);
    finesse.GetSF((unsigned char *)value.c_str(), finesse.mdCtx, finesse.tmpChunkSF, value.size());
    for (int i = 0; i < FINESSE_SF_NUM; i++)
    {
        // string keyStr;
        // keyStr.assign((char *)(finesse.tmpChunkSF + i * CHUNK_HASH_SIZE), CHUNK_HASH_SIZE);
        key_feature_table_[key].push_back(*(uint64_t *)(finesse.tmpChunkSF + i * CHUNK_HASH_SIZE));
        if (i == 0)
        {
            feature_key_table_[to_string(*(uint64_t *)(finesse.tmpChunkSF + i * CHUNK_HASH_SIZE))].insert(key);
        }
    }
    // for (const uint64_t &sf : super_features)
    // {
    //     feature_key_table_[to_string(sf)].insert(key);
    // }
}

bool FinesseIndexTable::GetSuperFeatures(const string &key, vector<uint64_t> *super_features)
{
    if (key_feature_table_.find(key) == key_feature_table_.end())
    {
        return false;
    }
    *super_features = key_feature_table_[key];
    return true;
}