#ifndef LZ4_BASELINE_H
#define LZ4_BASELINE_H

#include "absMethod.h"
#include "odess_similarity_detection.h"
#include "NTransFormSF.h"
#include "finesse.h"

class lz4Baseline : public absMethod
{
private:
    /* data */
    string myName_ = "lz4Baseline";
    string fileName;
    // Feature Table
    FeatureIndexTable table;
    NTransIndexTable nTransTable;
    FinesseIndexTable finesseTable;
    // cluster
    uint8_t *clusterBuffer;
    int clusterCnt = 0;
    uint64_t clusterSize = 0;

    // chunk set
    vector<Chunk_t> chunkSet;

    // time static
    std::chrono::duration<double> featureExtractTime;
    std::chrono::duration<double> clustringTime;

public:
    lz4Baseline();
    ~lz4Baseline();

    void ProcessOneTrace();

    void ProcessOneTraceOrigin();

    void SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq);

    void setFileName(string &fileName_) { fileName = fileName_; }

    // 从chunks中找出出现次数最多的特征
    feature_t find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeature);

    feature_t find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature);

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
};
#endif