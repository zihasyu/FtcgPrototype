#ifndef ABS_METHOD_H
#define ABS_METHOD_H

#include <string>
#include <chrono>
#include <openssl/evp.h>
#include "../define.h"
#include "../chunker.h"
#include "../lz4.h"
#include "../datawrite.h"
using namespace std;

class absMethod
{
private:
protected:
    string myName_ = "absMethod";
    string fileName;
    bool isDisk = false;
    vector<Chunk_t> chunkSet;
    uint64_t ChunkID = 0;                 // uinque chunk id
    string hashStr;                       // for fp hash
    vector<set<uint64_t>> finishedGroups; // for group
    // cluster
    uint8_t *clusterBuffer;
    int clusterCnt = 0;
    uint64_t clusterSize = 0;
    /*time statistic*/
    std::chrono::duration<double> featureExtractTime;
    std::chrono::duration<double> clustringTime;
    /* Deprecate variables */
    unordered_map<string, set<uint64_t>> FPunfinishedGroups;
    map<uint64_t, set<string>> feature_FP_Table;
    map<int, uint64_t> groupLogicalSize;
    map<int, uint64_t> groupCompressedSize;
    vector<uint64_t> Recipe;

public:
    // util
    uint8_t *readFileBuffer;
    uint8_t *lz4ChunkBuffer;
    Chunker chunker_;

    // statics
    uint64_t totalLogicalSize = 0; // 总逻辑大小
    uint64_t totalCompressedSize = 0;
    uint64_t ChunkNum = 0;
    uint64_t clusterNum = 0;
    EVP_MD_CTX *mdCtx;
    uint8_t *hashBuf;

    unordered_map<string, set<int>> FPindex; // For non Dedup
    unordered_map<string, int> Dedupindex;   // For Dedup
    unordered_map<int, string> chunkid_FP_table;

    bool isLastFile = false;

    // 测试用变量
    uint64_t singeFeature = 0;         // 单个特征
    uint64_t totalFeature = 0;         // 总特征
    uint64_t totalOriginalFeature = 0; // 总原始特征
    uint64_t feature_less_than_16 = 0; // 小于16的特征
    uint64_t notFullGroupNum = 0;      // 不完整的组数
    uint64_t stillNotFullGroupNum = 0; // 二次分组后仍然不完整的组数
    uint64_t groupNum = 0;             // 总组数
    uint64_t corelationIsOne = 0;      // 相关性为1的原始特征数
    uint64_t totalMatrixSize = 0;      // 总矩阵大小
    map<uint, uint> frequency_table;   // 频率表
    uint64_t totalChunkNum = 0;        // 总chunk数
    map<string, set<uint64_t>> FPdeduplicationTable;
    uint64_t compressedChunkNum = 0;

    // 消息队列
    ProduceConsumerQueue<Chunk_t> *recieveQueue;

    // dataWrite
    dataWrite *dataWrite_;
    absMethod();
    absMethod(uint64_t ExchunkSize);
    ~absMethod();
    virtual void ProcessOneTrace() = 0;

    void SetInputMQ(ProduceConsumerQueue<Chunk_t> *mq) { recieveQueue = mq; }
    void GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash);
    /**
     * @brief 生成FastCDC切点
     */

    set<int> FP_Find(string fp);
    bool FP_Insert(string fp, int chunkid);
    bool IsDedup(Chunk_t &chunk); // Dedup and get chunkid
    void ThirdCutPointSizeMax(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end);
    void ThirdCutPointHashMin(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end);
    void ThirdCutPointSizeMax_remove(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end);
    void ThirdCutPointHashMin_remove(const uint8_t *src, const uint64_t len, uint64_t &start, uint64_t &end);

    void setFileName(string &fileName_) { fileName = fileName_; }
    void groupmerge(vector<set<uint64_t>> &sets, int t);

    void SetIsDisk(bool isDisk_) { isDisk = isDisk_; }
    void Chunk_Insert(const Chunk_t &chunk);
    Chunk_t Get_Chunk_Info(int id);
    void CompressionToFinishedGroup();
};
#endif