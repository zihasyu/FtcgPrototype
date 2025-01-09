#ifndef Dedup_SF_H
#define Dedup_SF_H
#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"
// baseline
class Dedup_SF : public absMethod
{
private:
    /* data */
    string myName_ = "Dedup_SF";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;
    set<uint64_t> unfinishedChunks;

public:
    Dedup_SF();
    Dedup_SF(uint64_t ExchunkSize);
    ~Dedup_SF();
    void ProcessOneTrace();
};
#endif