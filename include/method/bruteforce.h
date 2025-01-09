#ifndef BRUTEFORCE_H
#define BRUTEFORCE_H
#include "absMethod.h"
#include "../odess_similarity_detection.h"
#include "../NTransFormSF.h"
#include "../finesse.h"

class bruteforce : public absMethod
{
private:
    /* data */
    string myName_ = "FPLz4SFBIB";

    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;

    int experimentChunkCount = 1000;

public:
    bruteforce();
    bruteforce(uint64_t ExchunkSize);
    ~bruteforce();

    void ProcessOneTrace();
};
#endif