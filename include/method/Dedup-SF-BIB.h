#ifndef Dedup_SF_BIB_H
#define Dedup_SF_BIB_H
#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"
// 0+design3
class Dedup_SF_BIB : public absMethod
{
private:
    /* data */
    string myName_ = "FPLz4SFBIB";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

public:
    Dedup_SF_BIB();
    Dedup_SF_BIB(uint64_t ExchunkSize);
    ~Dedup_SF_BIB();

    void ProcessOneTrace();

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
};
#endif