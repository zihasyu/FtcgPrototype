#ifndef DEDUP_HSF_BIB_H
#define DEDUP_HSF_BIB_H

#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"
#include "../xxhash.h"

#define HIERARCHICAL_SF_NUM 3
#define HIERARCHICAL_FEATURE_SIZE 128 // single hierarchical feature size(bit)

class Dedup_HSH_BIB : public absMethod
{
private:
    /* data */
    string myName_ = "Dedup_HSH_BIB";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

public:
    Dedup_HSH_BIB();
    Dedup_HSH_BIB(uint64_t ExchunkSize);
    ~Dedup_HSH_BIB();

    void ProcessOneTrace();

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);

    /**
     * @description: Group the chunks by hierarchical super features.
     * @param hierarchicalGroups: The hierarchical super features groups.
     * @param minGroupSize: The minimum size of a group.
     * @param k: the group whose size is k should be considered first.
     */
};
#endif