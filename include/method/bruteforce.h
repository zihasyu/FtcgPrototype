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
    string fileName;
    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;
    // cluster
    uint8_t *clusterBuffer;
    int clusterCnt = 0;
    uint64_t clusterSize = 0;

    int experimentChunkCount = 1000;

    // time static
    std::chrono::duration<double> featureExtractTime;
    std::chrono::duration<double> clustringTime;

public:
    bruteforce();
    bruteforce(uint64_t ExchunkSize);
    ~bruteforce();

    void ProcessOneTrace();
};
#endif