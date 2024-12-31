#ifndef FP_HIERARCHICAL_SF_H
#define FP_HIERARCHICAL_SF_H

#include "absMethod.h"
#include "odess_similarity_detection.h"
#include "NTransFormSF.h"
#include "finesse.h"
#include "xxhash.h"

#define HIERARCHICAL_SF_NUM 3
#define HIERARCHICAL_FEATURE_SIZE 128 // single hierarchical feature size(bit)

class FPHierarchicalSF : public absMethod
{
private:
    /* data */
    string myName_ = "FPHierarchicalSF";
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
    FPHierarchicalSF();
    FPHierarchicalSF(uint64_t ExchunkSize);
    ~FPHierarchicalSF();

    void ProcessOneTrace();

    void SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq);

    void setFileName(string &fileName_) { fileName = fileName_; }

    // 从chunks中找出出现次数最多的特征
    feature_t find_most_freq_feature(const set<string> chunks, set<feature_t> usedFeature);

    feature_t find_most_correlated_feature(feature_t feature, set<feature_t> usedFeature);

    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);

    /**
     * @description: Group the chunks by hierarchical super features.
     * @param hierarchicalGroups: The hierarchical super features groups.
     * @param minGroupSize: The minimum size of a group.
     * @param k: the group whose size is k should be considered first.
     */
    void groupmerge(vector<set<string>> &hierarchicalGroups, int targetGroupSize, int k);
};
#endif