#ifndef BRUTEFORCE_H
#define BRUTEFORCE_H
#include "absMethod.h"
#include "odess_similarity_detection.h"
#include "NTransFormSF.h"
#include "finesse.h"

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

    // chunk set
    vector<Chunk_t> chunkSet;

    // time static
    std::chrono::duration<double> featureExtractTime;
    std::chrono::duration<double> clustringTime;

public:
    bruteforce();
    bruteforce(uint64_t ExchunkSize);
    ~bruteforce();

    void ProcessOneTrace();

    void SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq);

    void setFileName(string &fileName_) { fileName = fileName_; }

    // 从chunks中找出出现次数最多的特征
    feature_t find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeature);

    feature_t find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature);

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
    void groupmerge(vector<set<string>> &hierarchicalGroups, int targetGroupSize, int k);
};
#endif