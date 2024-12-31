#include "../../include/datawrite.h"

dataWrite::dataWrite()
{
    // MQ = new MessageQueue<Container_t>(32);
    MultiHeaderBuffer = (uint8_t *)malloc(16 * 512);
    curContainer.size = 0;
    curContainer.containerID = 0;
    curContainer.chunkNum = 0;
    containerCache = new ReadCache();
    // restore
    // lz4SafeChunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
}
dataWrite::~dataWrite()
{
    free(MultiHeaderBuffer);
    delete containerCache;
    // free(lz4SafeChunkBuffer);
}
void dataWrite::PrintBinaryArray(const uint8_t *buffer, size_t buffer_size)
{
    for (size_t i = 0; i < buffer_size; i++)
    {
        fprintf(stdout, "%02x", buffer[i]);
    }
    fprintf(stdout, "\n");
    return;
}

bool dataWrite::Chunk_Insert(Chunk_t chunk)
{
    int tmpSize = 0;
    tmpSize = chunk.chunkSize;

    // cout << "flag is " << static_cast<int>(chunk.deltaFlag) << endl;
    chunkNum++;
    containerSize += chunk.chunkSize;
    curContainer.chunkNum++;
    if (curContainer.size + tmpSize > CONTAINER_MAX_SIZE)
    {
        // TODO put into MQ
        // cout << " curContainer.chunkNum is" << curContainer.chunkNum << " curContainer.containerId is " << curContainer.containerID << endl;
        startTime = std::chrono::high_resolution_clock::now();
        // cout << "push container " << containerNum << " into MQ" << endl;
        // cout << "cur container size is " << curContainer.size << endl;
        // MQ->Push(curContainer);
        string fileName = "./Containers/" + to_string(curContainer.containerID);
        ofstream outfile(fileName, std::ios::binary);
        if (outfile.is_open())
        {
            // cout << "write id is " << tmpContainer.containerId << " size is " << tmpContainer.size << endl;
            outfile.write(reinterpret_cast<const char *>(&curContainer.size), sizeof(curContainer.size));

            outfile.write(reinterpret_cast<const char *>(curContainer.data), curContainer.size);
            // outfile.write(reinterpret_cast<const char *>(&tmpContainer.size), sizeof(tmpContainer.size));
            // outfile << tmpContainer.data;
            outfile.close();
            // cout << "write done" << endl;
        }
        else
        {
            cout << "open file failed" << endl;
        }

        // sleep(1);
        containerNum++;
        containerSize = 0;
        curOffset = 0;
        curContainer.size = 0;
        curContainer.containerID = containerNum;
        curContainer.chunkNum = 0;
        endTime = std::chrono::high_resolution_clock::now();
        writeIOTime += (endTime - startTime);
    }
    // TODO: put chunk into container
    chunk.containerID = containerNum;
    chunk.offset = curOffset;
    // cout << " curContainer.size is " << curContainer.size << " tmpSize is " << tmpSize << " offset is " << curOffset << endl;
    curContainer.size += tmpSize;
    // cout<< "tmp size is " << tmpSize << " curoffset is " << curOffset<<endl;
    memcpy(curContainer.data + curOffset, chunk.chunkContent, tmpSize);
    curOffset += tmpSize;
    // cout << "free chunk " << endl;
    free(chunk.chunkContent);
    // cout << "free chunk done" << endl;
    chunk.chunkContent = nullptr;
    chunklist.push_back(chunk);
    // cout << "chunkset entry id is  " << chunklist[chunk.chunkid].chunkid << endl;
    return true;
}

int dataWrite::Get_Chunk_Num()
{
    return chunkNum;
}

int dataWrite::Get_Container_Num(Chunk_t chunk)
{
    if (containerSize + chunk.chunkSize > CONTAINER_MAX_SIZE)
    {
        return containerNum + 1;
    }
    else
    {
        return containerNum;
    }
}

Chunk_t dataWrite::Get_Chunk_Info(int id)
{
    // TODO: cache read container
    // cout << "chunk list size is " << chunklist.size() << endl;
    int tmpSize = 0;
    tmpSize = chunklist[id].chunkSize;

    string tmpContainerIDcontainerID = to_string(chunklist[id].containerID);
    bool cacheHitResult = containerCache->ExistsInCache(tmpContainerIDcontainerID);
    if (cacheHitResult)
    {

        chunklist[id].loadFromDisk = false;
        startTime = std::chrono::high_resolution_clock::now();
        string tmpContainer;
        tmpContainer.assign(CONTAINER_MAX_SIZE, 0);
        uint8_t *tmpContainerData = containerCache->ReadFromCache(tmpContainerIDcontainerID);
        // memcpy((uint8_t *)tmpContainer.c_str(), tmpContainerData, CONTAINER_MAX_SIZE);
        // memcpy(chunklist[id].chunkContent, tmpContainer.c_str() + chunklist[id].offset, tmpSize);
        // chunklist[id].chunkContent = tmpContainerData + chunklist[id].offset;
        chunklist[id].chunkContent = tmpContainerData + chunklist[id].offset;
        // chunklist[id].chunkContent = (uint8_t *)malloc(chunklist[id].chunkSize);
        // memcpy(chunklist[id].chunkContent, lz4SafeChunkBuffer, tmpSize);

        // chunklist[id].loadFromDisk = true;//if decode lz4, this is true

        // cout << "read from cache and size is " << chunklist[id].chunkSize << endl;
        //  memcpy(chunklist[id].chunkContent, tmpContainerData + chunklist[id].offset, tmpSize);
        cacheHitTimes++;
        endTime = std::chrono::high_resolution_clock::now();
        readCacheTime += (endTime - startTime);
    }
    // TODO: if cache miss, read from file
    else if (chunklist[id].containerID != containerNum)
    {
        chunklist[id].chunkContent = (uint8_t *)malloc(chunklist[id].chunkSize);
        startTime = std::chrono::high_resolution_clock::now();
        string fileName = "./Containers/" + tmpContainerIDcontainerID;
        // cout << fileName << endl;
        ifstream infile(fileName, ios::binary);
        if (infile.is_open())
        {
            uint64_t size;
            infile.read((char *)&size, sizeof(uint64_t));
            //     Allocate memory for the container
            string container;
            container.assign(size, 0);

            // Read the entire container
            infile.read((char *)container.c_str(), size);
            // Copy the required data to chunkContent
            // cout << "offset is " << chunklist[id].offset << "chunkSize is " << chunklist[id].chunkSize << endl;

            // base chunk & lz4 compress
            // int decompressedSize = LZ4_decompress_safe((char *)(container.c_str() + chunklist[id].offset), (char *)lz4SafeChunkBuffer, chunklist[id].chunkSize, CONTAINER_MAX_SIZE);
            // memcpy(chunklist[id].chunkContent, (char *)(container.c_str() + chunklist[id].offset), chunklist[id].chunkSize);
            memcpy(chunklist[id].chunkContent, (uint8_t *)(container.c_str() + chunklist[id].offset), tmpSize);

            // Add the container to the cache
            startTime2 = std::chrono::high_resolution_clock::now();
            containerCache->InsertToCache(tmpContainerIDcontainerID, (uint8_t *)container.c_str(), size);
            endTime2 = std::chrono::high_resolution_clock::now();
            UpdateCacheTime += (endTime2 - startTime2);
            infile.close();
            // sleep(0.5);
        }
        endTime = std::chrono::high_resolution_clock::now();
        readIOTime += (endTime - startTime);
        loadContainerTimes++;
        chunklist[id].loadFromDisk = true;
        // cout << "read from disk and size is " << chunklist[id].chunkSize << endl;
    }
    else
    {
        chunklist[id].loadFromDisk = false;
        if (chunklist[id].containerID == containerNum)
        {
            chunklist[id].chunkContent = curContainer.data + chunklist[id].offset;
            // chunklist[id].chunkContent = (uint8_t *)malloc(chunklist[id].chunkSize);
            // memcpy(chunklist[id].chunkContent, lz4SafeChunkBuffer, tmpSize);
            // chunklist[id].loadFromDisk = true;
        }
        else
        {
            cout << "open file failed" << endl;
        }
    }

    return chunklist[id];
}

void dataWrite::writeContainers()
{
    Container_t tmpContainer;
    bool jobDoneFlag = false;

    while (true)
    {
        if (MQ->done_ && MQ->IsEmpty())
        {
            jobDoneFlag = true;
        }
        // consume a message
        if (MQ->Pop(tmpContainer))
        {
            //  write a container
            // cout << "write a container " << tmpContainer.containerID << endl;
            string fileName = "./Containers/" + to_string(tmpContainer.containerID);
            ofstream outfile(fileName);
            if (outfile.is_open())
            {
                // cout << "write id is " << tmpContainer.containerID << " size is " << tmpContainer.size << endl;
                outfile.write(reinterpret_cast<const char *>(&tmpContainer.size), sizeof(tmpContainer.size));

                outfile.write(reinterpret_cast<const char *>(tmpContainer.data), tmpContainer.size);
                // outfile.write(reinterpret_cast<const char *>(&tmpContainer.size), sizeof(tmpContainer.size));
                // outfile << tmpContainer.data;
                outfile.close();
                // cout << "write done" << endl;
            }
            else
            {
                cout << "open file failed" << endl;
            }
        }

        if (jobDoneFlag)
        {
            break;
        }
    }
    // cout << "write done" << endl;

    return;
}

void dataWrite::ProcessLastContainer()
{
    // if (curContainer.size != 0)
    // {
    //     MQ->Push(curContainer);
    // }
    // MQ->done_ = true;
    if (curContainer.size != 0)
    {
        string fileName = "./Containers/" + to_string(curContainer.containerID);
        ofstream outfile(fileName);
        if (outfile.is_open())
        {
            outfile.write(reinterpret_cast<const char *>(&curContainer.size), sizeof(curContainer.size));
            outfile.write(reinterpret_cast<const char *>(curContainer.data), curContainer.size);

            outfile.close();
        }
        else
        {
            cout << "open file failed" << endl;
        }
    }

    return;
}

Chunk_t dataWrite::Get_Chunk_MetaInfo(int id)
{
    // std::lock_guard<std::mutex> lock(mtx);
    if (chunklist.size() < id)
    {
        cout << "errorrrr!" << endl;
        cout << "size is " << chunklist.size() << " id is " << id << endl;
    }
    auto ret = chunklist[id];
    return ret;
}

void dataWrite::PrintMetrics()
{
    cout << "load container times: " << loadContainerTimes << endl;
    cout << "cache hit times: " << cacheHitTimes << endl;

    auto readIOTimeMin = std::chrono::duration_cast<std::chrono::seconds>(readIOTime);
    cout << "Read IO time: " << readIOTimeMin.count() << " seconds" << endl;
    auto writeIOTimeMin = std::chrono::duration_cast<std::chrono::seconds>(writeIOTime);
    cout << "Write IO time: " << writeIOTimeMin.count() << " seconds" << endl;
    auto UpdateCacheTimeMin = std::chrono::duration_cast<std::chrono::seconds>(UpdateCacheTime);
    cout << "Update Cache time: " << UpdateCacheTimeMin.count() << "seconds" << endl;
    auto readCacheTimeMin = std::chrono::duration_cast<std::chrono::seconds>(readCacheTime);
    cout << "Read Cache time: " << readCacheTimeMin.count() << "seconds" << endl;
    return;
}

void dataWrite::SetFilename(string name)
{
    filename.assign(name);
    return;
}
