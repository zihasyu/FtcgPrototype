#pragma once
#ifndef FINESSE_H
#define FINESSE_H

#include <cstdint>
#include <openssl/evp.h>
#include <unordered_map>
#include <map>
#include <vector>
#include <algorithm>
#include "define.h"

#define CHUNK_HASH_SIZE 32
#define FINESSE_SF_NUM 3

using namespace std;
class Finesse
{
public:
    uint8_t *lz4ChunkBuffer;
    uint8_t *hashBuf;
    uint8_t *deltaMaxChunkBuffer;
    EVP_MD_CTX *mdCtx;
    unordered_map<string, vector<int>> *SFindex;
    uint8_t *tmpChunkSF;

    Finesse();
    ~Finesse();

    void GetSF(unsigned char *ptr, EVP_MD_CTX *mdCtx, uint8_t *SF, int dataSize);
    int SF_Find(const char *key, size_t keySize);
    bool SF_Insert(const char *key, size_t keySize, int chunkid);
    void GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash);
};

class FinesseIndexTable
{
public:
    FinesseIndexTable() {};
    ~FinesseIndexTable() {};

    void Put(const string &key, const string &value);
    void Delete(const string &key);

    Finesse finesse;

    map<string, vector<uint64_t>> key_feature_table_;
    unordered_map<string, unordered_set<string>> feature_key_table_;

private:
    void ExecuteDelete(const string &key, const vector<uint64_t> &super_features);
    bool GetSuperFeatures(const string &key, vector<uint64_t> *super_features);
};
#endif
