#include "../../include/method/lz4Compare.h"
// non Dedup
lz4Compare::lz4Compare()
{
    lz4ChunkBuffer = (uint8_t *)malloc(8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
}

lz4Compare::~lz4Compare()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
}

void lz4Compare::ProcessOneTrace()
{
    // int max = 1000;
    // int cnt = 0;
    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        if (recieveQueue->Pop(tmpChunk))
        {
            // if (cnt >= max)
            // {
            //     continue;
            // }
            // cnt++;
            // lz4Compress
            tmpChunk.chunkID = ChunkID++;
            int compressedSize = LZ4_compress_fast((char *)tmpChunk.chunkContent, (char *)lz4ChunkBuffer, tmpChunk.chunkSize, tmpChunk.chunkSize, 3);
            if (compressedSize <= 0)
            {
                compressedSize = tmpChunk.chunkSize;
            }
            ChunkNum++;
            totalCompressedSize += compressedSize;
            totalLogicalSize += tmpChunk.chunkSize;
        }
    }
    tool::Logging(myName_.c_str(), "Total logical size: %lu, total compressed size: %lu, compression ratio: %.2f\n", totalLogicalSize, totalCompressedSize, (double)totalLogicalSize / (double)totalCompressedSize);
    recieveQueue->done_ = false;
    return;
}
