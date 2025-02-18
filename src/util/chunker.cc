#include "../../include/chunker.h"

Chunker::Chunker(int chunkType_)
{

    FixedChunkSize = 8192;
    // specifiy chunk type
    chunkType = chunkType_;
    // init chunker
    ChunkerInit();
}
Chunker::Chunker(int chunkType_, uint64_t ExchunkSize)
{

    FixedChunkSize = 8192;
    // specifiy chunk type
    chunkType = chunkType_;
    // init chunker
    minChunkSize = ExchunkSize / 2;
    avgChunkSize = ExchunkSize;
    maxChunkSize = ExchunkSize * 2;
    cout << "avgChunkSize is " << avgChunkSize << endl;
    ChunkerInit();
}
Chunker::~Chunker()
{
    free(readFileBuffer);
    free(chunkBuffer);
}

void Chunker::LoadChunkFile(string path)
{
    if (chunkingFile_.is_open())
    {
        chunkingFile_.close();
    }

    chunkingFile_.open(path, ios_base::in | ios::binary);
    if (!chunkingFile_.is_open())
    {
        tool::Logging(myName_.c_str(), "open file: %s error.\n",
                      path.c_str());
        exit(EXIT_FAILURE);
    }
    return;
}

void Chunker::Chunking()
{
    switch (chunkType)
    {
    case FIXED_SIZE_CHUNKING:
    {
        FixSizedChunking();
        break;
    }
    case FASTCDC_CHUNKING:
    {
        // FastCDC();
        // break;
    }
    default:
    {
        tool::Logging(myName_.c_str(), "chunking type error.\n");
        exit(EXIT_FAILURE);
    }
    }
    tool::Logging(myName_.c_str(), "thread exit.\n");
    return;
}

void Chunker::ChunkerInit()
{

    switch (chunkType)
    {
    case 0:
        // fixed size chunking]
        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
        chunkBuffer = (uint8_t *)malloc(FixedChunkSize);
    case 1:
        // FastCDC chunking
        // FastCDC init
        normalSize = CalNormalSize(minChunkSize, avgChunkSize, maxChunkSize);
        bits = (uint32_t)round(log2(static_cast<double>(avgChunkSize)));
        maskS = GenerateFastCDCMask(bits + 1);
        maskL = GenerateFastCDCMask(bits - 1);
        break;
    default:
        tool::Logging(myName_.c_str(), "chunk type error.\n");
        exit(EXIT_FAILURE);
        break;
    }
    return;
}

void Chunker::FixSizedChunking()
{
    uint64_t fileSize = 0;
    bool end = false;

    while (!end)
    {
        // read file
        memset((char *)readFileBuffer, 0, sizeof(uint8_t) * READ_FILE_SIZE);
        chunkingFile_.read((char *)readFileBuffer, READ_FILE_SIZE);
        //
        end = chunkingFile_.eof();
        size_t len = chunkingFile_.gcount();
        size_t chunkedSize = 0;
        if (len == 0)
        {
            break;
        }
        fileSize += len;

        size_t remainSize = len;
        while (chunkedSize < len)
        {
            Chunk_t chunk;
            if (remainSize > FixedChunkSize)
            {
                chunk.chunkSize = FixedChunkSize;
                chunk.chunkContent = (uint8_t *)malloc(chunk.chunkSize);
                memcpy(chunk.chunkContent, readFileBuffer + chunkedSize, FixedChunkSize);

                chunkedSize += FixedChunkSize;
                remainSize -= FixedChunkSize;
            }
            else
            {
                chunk.chunkSize = remainSize;
                chunk.chunkContent = (uint8_t *)malloc(chunk.chunkSize);
                memcpy(chunk.chunkContent, readFileBuffer + chunkedSize, remainSize);
                chunkedSize += remainSize;
                remainSize = 0;
            }

            chunk.chunkID = chunkID++;

            if (!outputMQ_->Push(chunk))
            {
                tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
                exit(EXIT_FAILURE);
            }

            if (chunk.chunkID > EXPERIMENT_CHUNK_NUM - 2)
            {
                end = true;
                break;
            }
        }
    }

    outputMQ_->done_ = true;
    tool::Logging(myName_.c_str(), "chunking done.\n");

    return;
}
uint32_t Chunker::CalNormalSize(const uint32_t min, const uint32_t av, const uint32_t max)
{
    uint32_t off = min + DivCeil(min, 2);
    if (off > av)
    {
        off = av;
    }
    uint32_t diff = av - off;
    if (diff > max)
    {
        return max;
    }
    return diff;
}
inline uint32_t Chunker::DivCeil(uint32_t a, uint32_t b)
{
    uint32_t tmp = a / b;
    if (a % b == 0)
    {
        return tmp;
    }
    else
    {
        return (tmp + 1);
    }
}
uint32_t Chunker::GenerateFastCDCMask(uint32_t bits)
{
    uint32_t tmp;
    tmp = (1 << CompareLimit(bits, 1, 31)) - 1;
    return tmp;
}
inline uint32_t Chunker::CompareLimit(uint32_t input, uint32_t lower, uint32_t upper)
{
    if (input <= lower)
    {
        return lower;
    }
    else if (input >= upper)
    {
        return upper;
    }
    else
    {
        return input;
    }
}
uint64_t Chunker::CutPointFastCDC(const uint8_t *src, const uint64_t len)
{
    uint64_t n;
    uint32_t fp = 0;
    uint64_t i;
    i = min(len, static_cast<uint64_t>(minChunkSize));
    n = min(normalSize, len);
    for (; i < n; i++)
    {
        fp = (fp >> 1) + GEAR[src[i]];
        if (!(fp & maskS))
        {
            return (i + 1);
        }
    }

    n = min(static_cast<uint64_t>(maxChunkSize), len);
    for (; i < n; i++)
    {
        fp = (fp >> 1) + GEAR[src[i]];
        if (!(fp & maskL))
        {
            return (i + 1);
        }
    }
    return i;
};