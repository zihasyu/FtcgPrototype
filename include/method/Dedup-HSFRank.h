#ifndef DEDUP_HSFRANK_H
#define DEDUP_HSFRANK_H

#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"
#include "../xxhash.h"
// design1
#define HIERARCHICAL_SF_NUM 3
#define HIERARCHICAL_FEATURE_SIZE 128 // single hierarchical feature size(bit)

class Dedup_HSFRank : public absMethod
{
private:
    /* data */
    string myName_ = "Dedup_HSFRank";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFA_unfinished_group;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFB_unfinished_group;
    unordered_map<string, vector<set<uint64_t>>> hierarchicalSFC_unfinished_group;
    map<string, vector<uint64_t>> hierarchicalSFC_unfinished_chunk;
    set<uint64_t> unfinishedChunks;

public:
    Dedup_HSFRank();
    Dedup_HSFRank(uint64_t ExchunkSize);
    ~Dedup_HSFRank();

    void ProcessOneTrace();
};
#endif