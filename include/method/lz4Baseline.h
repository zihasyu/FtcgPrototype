#ifndef LZ4_BASELINE_H
#define LZ4_BASELINE_H

#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"

class lz4Baseline : public absMethod
{
private:
    /* data */
    string myName_ = "lz4Baseline";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

public:
    lz4Baseline();
    ~lz4Baseline();

    void ProcessOneTrace();
};
#endif