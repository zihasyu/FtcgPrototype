#ifndef MY_STRUCT_H
#define MY_STRUCT_H

#include "define.h"

typedef struct
{
    uint64_t chunkID;
    uint32_t chunkSize;
    uint32_t saveSize;
    uint8_t *chunkContent;
    bool loadFromDisk;
    uint64_t offset;
    uint64_t containerID;
} Chunk_t;

typedef struct
{
    uint64_t size;
    uint64_t chunkNum;
    uint64_t containerID;
    uint8_t data[CONTAINER_MAX_SIZE];
} Container_t;

typedef struct
{
    uint64_t chunkId;
    size_t chunkType;
    uint64_t offset;
} Recipe_t;

typedef struct
{
    uint64_t chunkID;
    uint64_t offset_start;
    uint64_t offset_end;
} representChunk_t;

#endif