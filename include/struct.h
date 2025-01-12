#ifndef MY_STRUCT_H
#define MY_STRUCT_H

#include "define.h"

typedef struct
{
    uint64_t chunkID;
    uint32_t chunkSize; // can remove
    uint8_t *chunkContent;
    bool loadFromDisk;    // only for read from disk
    uint64_t offset;      // only for read from disk
    uint64_t containerID; // only for read from disk
    uint32_t GroupID;
} Chunk_t;

typedef struct
{
    std::set<uint64_t> chunkIDs;
    uint32_t Orisize;
    uint32_t Comsize;
    uint64_t ReOffset;      // For restore
    uint64_t ReContainerID; // For restore
} Group_t;

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