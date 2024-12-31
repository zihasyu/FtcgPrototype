#ifndef CHARACTER_FREQUENCY_H
#define CHARACTER_FREQUENCY_H

#include "absMethod.h"
#include "odess_similarity_detection.h"
#include "NTransFormSF.h"
#include "finesse.h"
#include "xxhash.h"

class characterFrequency : public absMethod
{
private:
    /* data */
    string myName_ = "characterFrequency";
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

    std::vector<std::vector<float>> features;
    std::vector<std::vector<float>> projections;

public:
    characterFrequency();
    characterFrequency(uint64_t ExchunkSize);
    ~characterFrequency();

    void ProcessOneTrace();

    // 计算字节频率
    std::array<double, 256> calculateFrequency(const Chunk_t &chunk);

    double calculateEntropy(const Chunk_t &chunk);

    // 聚类
    std::vector<set<string>> clusterChunks(const std::vector<Chunk_t> &chunkSet, int clusterSize);
    // 特征提取：将数据块转换为浮点向量
    std::vector<float> extractFeature(const Chunk_t &chunk);
    // 生成投影向量
    std::vector<std::vector<float>> genernateProjection(int featureDim);
    // 构建哈希表
    HashTable buildHashTable(const std::vector<std::vector<float>> &projections, const std::vector<std::vector<float>> &features);
    // 生成哈希签名
    HashSignature hashBlock(const std::vector<float> &feature, const std::vector<std::vector<float>> &projection);
    // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);

    /**
     * @description: Group the chunks by hierarchical super features.
     * @param hierarchicalGroups: The hierarchical super features groups.
     * @param minGroupSize: The minimum size of a group.
     */
    void groupmerge(vector<set<string>> &Groups, int targetGroupSize);
};
#endif