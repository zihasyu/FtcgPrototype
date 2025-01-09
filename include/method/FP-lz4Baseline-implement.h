#ifndef FP_LZ4_BASELINE_IMPLEMENT_H
#define FP_LZ4_BASELINE_IMPLEMENT_H

#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"

class FPLz4BaselineImplement : public absMethod
{
private:
    /* data */
    string myName_ = "FPLz4BaselineImplement";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

public:
    FPLz4BaselineImplement();
    FPLz4BaselineImplement(uint64_t ExchunkSize);
    ~FPLz4BaselineImplement();

    void ProcessOneTrace();

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
};
#endif