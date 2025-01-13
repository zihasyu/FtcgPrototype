#include "../../include/method/Dedup-HSFRank-BIB.h"

Dedup_HSFRank_BIB::Dedup_HSFRank_BIB()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSFRank_BIB::Dedup_HSFRank_BIB(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

Dedup_HSFRank_BIB::~Dedup_HSFRank_BIB()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void Dedup_HSFRank_BIB::ProcessOneTrace()
{
    while (true)
    {
        if (recieveQueue->done_ && recieveQueue->IsEmpty())
        {
            break;
        }
        Chunk_t tmpChunk;
        string hashStr;
        if (recieveQueue->Pop(tmpChunk))
        {
            // calculate feature
            if (!IsDedup(tmpChunk))
            {
                table.PutHSFRank(std::to_string(tmpChunk.chunkID), (char *)tmpChunk.chunkContent);
                hierarchicalSFC_unfinished_chunk[table.key_hierarchicalSF_table[to_string(tmpChunk.chunkID)][2]].push_back(tmpChunk.chunkID);
                tmpChunk.isGrouped = false;
                unfinishedChunkNum++;
                Chunk_Insert(tmpChunk);
                totalChunkNum++;
                // todo:add to decipe
            }
            else
            {
                // todo:add to decipe
                totalChunkNum++;
            }
        }
    }
    if (!isLastFile)
    {
        recieveQueue->done_ = false;
        return;
    }
    tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
    totalFeature += table.original_feature_key_table.size();
    set<uint64_t> finishedChunks;
    ofstream out("../frequencyTable.txt", ios::app);

    tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());
    tool::Logging(myName_.c_str(), "FP finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "FP unfinished chunk num is %d\n", unfinishedChunkNum);
    // HSFRank group
    for (auto &it : table.hierarchicalSFA_C_table)
    {
        vector<string> SFCs(it.second.begin(), it.second.end());
        sort(SFCs.begin(), SFCs.end(), [&](const string &a, const string &b)
             { return a > b; });
        tmpGroup.clear();
        for (auto sf_c : SFCs)
        {
            for (auto groups : hierarchicalSFC_unfinished_chunk[sf_c])
            {
                tmpGroup.insert(groups);
                if (tmpGroup.size() >= MAX_GROUP_SIZE)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        finishedChunks.insert(id);
                        chunkSet[id].isGrouped = true;
                        unfinishedChunkNum--;
                    }
                    tmpGroup.clear();
                }
            }
        }
        if (tmpGroup.size() > 1)
        {
            unfinishedGroups.push_back(tmpGroup);
            for (auto id : tmpGroup)
            {
                finishedChunks.insert(id);
                chunkSet[id].isGrouped = true;
                unfinishedChunkNum--;
            }
            tmpGroup.clear();
        }
        else
        {
            for (auto id : tmpGroup)
            {
                // chunkSet[id].isGrouped = false;
                // unfinishedChunkNum++;
            }
            tmpGroup.clear();
        }
    }

    int k = 0;
    for (auto id : chunkSet)
    {
        if (id.isGrouped == false)
            k++;
    }
    tool::Logging(myName_.c_str(), "unfinished chunk num is %d\n", k);

    tool::Logging(myName_.c_str(), "HSF Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "HSF Unfinished chunk num is %d\n", unfinishedChunkNum);

    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    for (auto it : unfinishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "first group size, frequency" << endl;
    out << 1 << "," << unfinishedChunkNum << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    // BIB group
    FeatureIndexTable representTable;
    map<super_feature_t, vector<set<uint64_t>>> Rfeature_unfinishedGroup;

    for (auto chunk : chunkSet)
    {
        if (chunk.isGrouped == true)
        {
            continue;
        }
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        Chunk_t GroupTmpChunk = Get_Chunk_Info(chunk.chunkID);
        representChunk_t representChunk;
        representChunk.chunkID = chunk.chunkID;
        ThirdCutPointHashMin(GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize, representChunk.offset_start, representChunk.offset_end);
        // ThirdCutPointHashMin_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);

        string representChunkContent((char *)GroupTmpChunk.chunkContent + representChunk.offset_start, representChunk.offset_end - representChunk.offset_start);
        representTable.Put(to_string(chunk.chunkID), (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[to_string(chunk.chunkID)][0]].push_back({chunk.chunkID});
        if (GroupTmpChunk.loadFromDisk)
        {
            free(GroupTmpChunk.chunkContent);
        }
    }
    for (auto group = unfinishedGroups.begin(); group != unfinishedGroups.end(); group++)
    {
        auto id = *group->begin();

        representChunk_t representChunk;
        representChunk.chunkID = id;
        // ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
        Chunk_t GroupTmpChunk = Get_Chunk_Info(id);
        ThirdCutPointHashMin(GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize, representChunk.offset_start, representChunk.offset_end);
        string representChunkContent((char *)GroupTmpChunk.chunkContent + representChunk.offset_start, representChunk.offset_end - representChunk.offset_start);
        representTable.Put(to_string(id), (char *)representChunkContent.c_str());
        Rfeature_unfinishedGroup[representTable.key_feature_table_[to_string(id)][0]].push_back(*group);
        for (auto id : *group)
        {
            chunkSet[id].isGrouped = false;
            unfinishedChunkNum++;
            finishedChunks.erase(id);
        }
    }

    for (auto it : representTable.feature_key_table_)
    {
        tmpGroup.clear();
        // if (it.first == 0 || Rfeature_unfinishedGroup[it.first].size() > 0)
        if (true)
        {
            for (auto group : Rfeature_unfinishedGroup[it.first])
            {
                if (tmpGroup.size() + group.size() > MAX_GROUP_SIZE && tmpGroup.size() > 0)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        chunkSet[id].isGrouped = true;
                        unfinishedChunkNum--;
                        // finishedChunks.erase(id);
                        finishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
                tmpGroup.insert(group.begin(), group.end());
            }
            if (tmpGroup.size() > 1)
            {
                finishedGroups.push_back(tmpGroup);
                for (auto id : tmpGroup)
                {
                    chunkSet[id].isGrouped = true;
                    unfinishedChunkNum--;
                    finishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
        }
        tmpGroup.clear();
    }
    tool::Logging(myName_.c_str(), "third Finished chunk num is %d\n", finishedChunks.size());
    tool::Logging(myName_.c_str(), "third Unfinished chunk num is %d\n", unfinishedChunkNum);
    frequency_table.clear();
    for (auto it : finishedGroups)
    {
        frequency_table[it.size()]++;
    }
    out << "third group size, frequency" << endl;
    for (auto it : frequency_table)
    {
        out << it.first << ", " << it.second << endl;
    }

    FinalMerge();
    groupNum += finishedGroups.size();
    for (const auto &it : finishedGroups)
    {
        compressedChunkNum += it.size();
    }
    tool::Logging(myName_.c_str(), "compressed chunk num is %d\n", compressedChunkNum);
    tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
    totalLogicalSize = compressedChunkNum * 8 * 1024;
    CompressionToFinishedGroup();
    out << "group size, logical size, compressed size, ratio" << endl;
    for (auto it : groupLogicalSize)
    {
        out << it.first << ", " << it.second << ", " << groupCompressedSize[it.first] << ", " << (double)it.second / (double)groupCompressedSize[it.first] << endl;
    }
    out.close();
    // calculate the throughput
    double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
    double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
    tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    recieveQueue->done_ = false;
    return;
}