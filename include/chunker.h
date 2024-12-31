#ifndef CHUNKER_H
#define CHUNKER_H

#include "define.h"
#include "struct.h"
#include "messageQueue.h"
#include "produceconsumerqueue.h"

using namespace std;

enum ChunkTypeNum
{
    FIXED_SIZE_CHUNKING = 0,
    FASTCDC_CHUNKING = 1,
};

class Chunker
{
private:
    /* data */
    string myName_ = "Chunker";

    int chunkType;

    // chunk size settings for FastCDC
    uint64_t avgChunkSize_;
    uint64_t minChunkSize_;
    uint64_t maxChunkSize_;

    // fixed Size Chunking
    uint64_t FixedChunkSize;

    // IO stream
    ifstream chunkingFile_;

    // buffer
    uint8_t *readFileBuffer;
    uint8_t *chunkBuffer;

    // Chunker ID
    uint64_t chunkID = 0;

    // FastCDC
    uint64_t minChunkSize = 1024;
    uint64_t avgChunkSize = 2048;
    uint64_t maxChunkSize = 4096;
    uint64_t normalSize;
    uint32_t bits;
    uint32_t maskS;
    uint32_t maskL;

    // messageQueue
    ProduceConsumerQueue<Chunk_t> *outputMQ_;

public:
    Chunker(int chunkType_);
    Chunker(int chunkType_, uint64_t ExchunkSize);
    ~Chunker();
    // util method
    void LoadChunkFile(string path);
    void ChunkerInit();
    void Chunking();

    void SetOutputMQ(ProduceConsumerQueue<Chunk_t> *outputMQ)
    {
        outputMQ_ = outputMQ;
        return;
    }
    // Chunking Methods
    void FixSizedChunking();

    void FastCDCChunking();

    uint32_t CalNormalSize(const uint32_t min, const uint32_t av, const uint32_t max);
    inline uint32_t DivCeil(uint32_t a, uint32_t b);
    uint32_t GenerateFastCDCMask(uint32_t bits);
    uint32_t CompareLimit(uint32_t input, uint32_t lower, uint32_t upper);
    uint64_t CutPointFastCDC(const uint8_t *src, const uint64_t len);
};
#endif