#ifndef FP_LZ4_BASELINE_H
#define FP_LZ4_BASELINE_H

#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"

class FPLz4Baseline : public absMethod
{
private:
    /* data */
    string myName_ = "FPLz4Baseline";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

public:
    FPLz4Baseline();
    ~FPLz4Baseline();

    void ProcessOneTrace();

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
};
#endif