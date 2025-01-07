#include "../../include/method/FP-lz4Baseline-implement.h"

FPLz4BaselineImplement::FPLz4BaselineImplement()
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}
FPLz4BaselineImplement::FPLz4BaselineImplement(uint64_t ExchunkSize) : absMethod(ExchunkSize)
{
    lz4ChunkBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE);
    // 16 * 8KiB per Cluster
    clusterBuffer = (uint8_t *)malloc(16 * 8 * 1024);
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

FPLz4BaselineImplement::~FPLz4BaselineImplement()
{
    free(lz4ChunkBuffer);
    free(readFileBuffer);
    free(clusterBuffer);
    EVP_MD_CTX_free(mdCtx);
    free(hashBuf);
}

void FPLz4BaselineImplement::ProcessOneTrace()
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
            stringstream ss;
            ss << tmpChunk.chunkID;
            auto start = std::chrono::high_resolution_clock::now();
            table.PutOrignals(ss.str(), (char *)tmpChunk.chunkContent);
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            //  nTransTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            // finesseTable.Put(ss.str(), (char *)tmpChunk.chunkContent);
            auto end = std::chrono::high_resolution_clock::now();
            featureExtractTime += end - start;
            // table.Put(ss.str(), (char *)tmpChunk.chunkContent);
            GenerateHash(mdCtx, tmpChunk.chunkContent, tmpChunk.chunkSize, hashBuf);
            hashStr.assign((char *)hashBuf, CHUNK_HASH_SIZE);
            FP_Insert(hashStr, tmpChunk.chunkID);

            Chunk_Insert(tmpChunk);

            totalChunkNum++;
        }
    }
    if (isLastFile)
    {
        // auto start = std::chrono::high_resolution_clock::now();
        // for (auto it : table.original_feature_key_table)
        // {
        //     for (auto id : it.second)
        //     {
        //     }
        // }
        // auto end = std::chrono::high_resolution_clock::now();
        // clustringTime += end - start;

        // vector<feature_t> sorted_original_features = table.sortFeatureBySetSize();

        totalFeature += table.original_feature_key_table.size();
        vector<set<string>> finishedGroups;
        unordered_map<string, set<string>> FPunfinishedGroups;
        set<string> finishedChunks;
        set<string> unfinishedChunks;
        vector<set<string>> adjGroups;
        set<string> tmpGroup; // 16一组chunkid
        ofstream out("../frequencyTable.txt", ios::app);
        if (!out.is_open())
        {
            tool::Logging(myName_.c_str(), "open file failed\n");
            return;
        }
        map<feature_t, set<string>> feature_FP_Table;
        // set<string> usedChunks;

        // tool::Logging(myName_.c_str(), "chunk num is %d\n", table.original_feature_key_table.size());
        tool::Logging(myName_.c_str(), "feature num is %d\n", table.original_feature_key_table.size());

        // 根据FP进行初次分组
        frequency_table.clear();
        for (auto it : this->FPindex)
        {
            frequency_table[it.second.size()]++;
            auto element = it.second.begin();
            auto id = *element;
            feature_FP_Table[table.original_key_feature_table_[to_string(id)]].insert(it.first);
            tmpGroup.clear();
            if (it.second.size() >= 16)
            {
                for (auto id : it.second)
                {
                    tmpGroup.insert(to_string(id));
                    finishedChunks.insert(to_string(id));
                    if (tmpGroup.size() == 16)
                    {
                        finishedGroups.push_back(tmpGroup);
                        tmpGroup.clear();
                    }
                }
                if (tmpGroup.size() > 0)
                {
                    for (auto id : tmpGroup)
                    {
                        finishedChunks.erase(id);
                        unfinishedChunks.insert(id);
                    }
                    FPunfinishedGroups[it.first].insert(tmpGroup.begin(), tmpGroup.end());
                    tmpGroup.clear();
                }
            }
            else
            {
                for (auto id : it.second)
                {
                    tmpGroup.insert(to_string(id));
                    unfinishedChunks.insert(to_string(id));
                }
                FPunfinishedGroups[it.first].insert(tmpGroup.begin(), tmpGroup.end());
                tmpGroup.clear();
            }
        }
        out << "FP group size, frequency" << endl;
        for (auto it : frequency_table)
        {
            out << it.first << ", " << it.second << endl;
        }
        tool::Logging(myName_.c_str(), "FP finished chunk num is %d\n", finishedChunks.size());
        tool::Logging(myName_.c_str(), "FP unfinished chunk num is %d\n", unfinishedChunks.size());
        // 对于每一个feature进行分组
        for (auto it : feature_FP_Table)
        {
            tmpGroup.clear();
            if (it.first == 0)
            {
                for (auto fp : it.second)
                {
                    if (FPunfinishedGroups[fp].size() + tmpGroup.size() > 16 && tmpGroup.size() > 0)
                    {
                        finishedGroups.push_back(tmpGroup);
                        for (auto id : tmpGroup)
                        {
                            unfinishedChunks.erase(id);
                            finishedChunks.insert(id);
                        }
                        tmpGroup.clear();
                    }
                    tmpGroup.insert(FPunfinishedGroups[fp].begin(), FPunfinishedGroups[fp].end());
                }
                if (tmpGroup.size() > 1)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.erase(id);
                        finishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
                else
                {
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
                continue;
            }
            // 优先把FP相同的chunk放在一组
            vector<bool> usedGroup(it.second.size(), false);
            for (auto fp1 = it.second.begin(); fp1 != it.second.end(); fp1++)
            {
                if (usedGroup[distance(it.second.begin(), fp1)])
                {
                    continue;
                }
                tmpGroup.clear();
                usedGroup[distance(it.second.begin(), fp1)] = true;
                tmpGroup.insert(FPunfinishedGroups[*fp1].begin(), FPunfinishedGroups[*fp1].end());
                for (auto fp2 = next(fp1); fp2 != it.second.end(); fp2++)
                {

                    if (usedGroup[distance(it.second.begin(), fp2)] || tmpGroup.size() + FPunfinishedGroups[*fp2].size() > 16)
                    {
                        continue;
                    }
                    tmpGroup.insert(FPunfinishedGroups[*fp2].begin(), FPunfinishedGroups[*fp2].end());
                    usedGroup[distance(it.second.begin(), fp2)] = true;
                    if (tmpGroup.size() == 16)
                    {
                        break;
                    }
                }
                if (tmpGroup.size() > 1)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.erase(id);
                        finishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
                else
                {
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
            }
            if (tmpGroup.size() > 1)
            {
                finishedGroups.push_back(tmpGroup);
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.erase(id);
                    finishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
            else
            {
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
        }

        frequency_table.clear();
        for (auto it : finishedGroups)
        {
            frequency_table[it.size()]++;
        }
        out << "first group size, frequency" << endl;
        out << 1 << "," << unfinishedChunks.size() << endl;
        for (auto it : frequency_table)
        {
            out << it.first << ", " << it.second << endl;
        }

        tool::Logging(myName_.c_str(), "total chunk num is %d\n", totalChunkNum);
        tool::Logging(myName_.c_str(), "second Finished chunk num is %d\n", finishedChunks.size());
        tool::Logging(myName_.c_str(), "seconds Unfinished chunk num is %d\n", unfinishedChunks.size());

        // out << chunkSet[10049].chunkContent << endl;
        // out << "---------------------------------------------------------------------------------" << endl;
        // out << chunkSet[10099].chunkContent << endl;
        // return;

        // 对每一个1块进行FastCDC,找到一个代表块,代表块的chunkid为原来块的id+
        vector<representChunk_t> representChunkSet;
        FeatureIndexTable representTable;
        for (auto id : unfinishedChunks)
        {
            uint64_t start = 0;
            uint64_t end = 0;
            ThirdCutPointSizeMax(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
            // ThirdCutPointSizeMax_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
            // ThirdCutPointHashMin(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
            // ThirdCutPointHashMin_remove(chunkSet[stoull(id)].chunkContent, chunkSet[stoull(id)].chunkSize, start, end);
            representChunk_t representChunk;
            representChunk.chunkID = stoull(id);
            representChunk.offset_start = start;
            representChunk.offset_end = end;
            representChunkSet.push_back(representChunk);
            string representChunkContent((char *)chunkSet[stoull(id)].chunkContent + start, end - start);
            representTable.PutOrignals(id, (char *)representChunkContent.c_str());
        }
        tmpGroup.clear();
        for (auto it : representTable.original_feature_key_table)
        {
            for (auto id : it.second)
            {
                tmpGroup.insert(id);
                if (tmpGroup.size() == 16)
                {
                    finishedGroups.push_back(tmpGroup);
                    for (auto id : tmpGroup)
                    {
                        unfinishedChunks.erase(id);
                        finishedChunks.insert(id);
                    }
                    tmpGroup.clear();
                }
            }
            if (tmpGroup.size() > 1)
            {
                finishedGroups.push_back(tmpGroup);
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.erase(id);
                    finishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
            else
            {
                for (auto id : tmpGroup)
                {
                    unfinishedChunks.insert(id);
                }
                tmpGroup.clear();
            }
        }
        frequency_table.clear();
        for (auto it : finishedGroups)
        {
            frequency_table[it.size()]++;
        }
        out << "third group size, frequency" << endl;
        out << 1 << "," << unfinishedChunks.size() << endl;
        for (auto it : frequency_table)
        {
            out << it.first << ", " << it.second << endl;
        }

        tool::Logging(myName_.c_str(), "third Finished chunk num is %d\n", finishedChunks.size());
        tool::Logging(myName_.c_str(), "third Unfinished chunk num is %d\n", unfinishedChunks.size());
        // 将大小为1的分组任意两两组合压缩
        // for (auto id1 = unfinishedChunks.begin(); id1 != unfinishedChunks.end() && distance(unfinishedChunks.begin(), id1) < 1000; id1++)
        // {
        //     int m = distance(unfinishedChunks.begin(), id1);
        //     int n = 0;
        //     int impl_logical_size = 0;
        //     int impl_compressed_size = 0;
        //     double impl_ratio1 = 0;
        //     double impl_ratio2 = 0;
        //     double impl_ratio3 = 0;
        //     double average_ratio = 0;
        //     bool flag = false;
        //     // 单独压缩块1
        //     memcpy(clusterBuffer, chunkSet[stoull(*id1)].chunkContent, chunkSet[stoull(*id1)].chunkSize);
        //     impl_logical_size = chunkSet[stoull(*id1)].chunkSize;
        //     int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, impl_logical_size, impl_logical_size, 3);
        //     if (compressedSize <= 0)
        //     {
        //         compressedSize = impl_logical_size;
        //     }
        //     impl_compressed_size = compressedSize;
        //     impl_ratio1 = (double)impl_logical_size / (double)impl_compressed_size;
        //     for (auto id2 = next(id1); id2 != unfinishedChunks.end() && distance(unfinishedChunks.begin(), id2) < 1000; id2++)
        //     {
        //         n = distance(unfinishedChunks.begin(), id2);
        //         // 单独压缩块2
        //         memcpy(clusterBuffer, chunkSet[stoull(*id2)].chunkContent, chunkSet[stoull(*id2)].chunkSize);
        //         impl_logical_size = chunkSet[stoull(*id2)].chunkSize;
        //         compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, impl_logical_size, impl_logical_size, 3);
        //         if (compressedSize <= 0)
        //         {
        //             compressedSize = impl_logical_size;
        //         }
        //         impl_compressed_size = compressedSize;
        //         impl_ratio2 = (double)impl_logical_size / (double)impl_compressed_size;
        //         // 合并压缩
        //         memcpy(clusterBuffer, chunkSet[stoull(*id1)].chunkContent, chunkSet[stoull(*id1)].chunkSize);
        //         memcpy(clusterBuffer + chunkSet[stoull(*id1)].chunkSize, chunkSet[stoull(*id2)].chunkContent, chunkSet[stoull(*id2)].chunkSize);
        //         impl_logical_size = chunkSet[stoull(*id1)].chunkSize + chunkSet[stoull(*id2)].chunkSize;
        //         compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, impl_logical_size, impl_logical_size, 3);
        //         if (compressedSize <= 0)
        //         {
        //             compressedSize = impl_logical_size;
        //         }
        //         impl_compressed_size = compressedSize;
        //         impl_ratio3 = (double)impl_logical_size / (double)impl_compressed_size;
        //         if (impl_ratio3 > 2.0605)
        //         {
        //             out << *id1 << " " << *id2 << " " << chunkSet[stoull(*id1)].chunkSize << " " << chunkSet[stoull(*id2)].chunkSize << " ";
        //             out << impl_ratio1 << " " << impl_ratio2 << " " << impl_ratio3 << endl;
        //             // 写入两个块的所有特征
        //             // if (!flag)
        //             // {
        //             //     out << impl_ratio3 << endl;
        //             //     for (auto it : table.original_key_features_table_[*id1])
        //             //     {
        //             //         out << to_string(it) << ",";
        //             //     }
        //             //     out << endl;
        //             //     flag = true;
        //             // }
        //             // for (auto it : table.original_key_features_table_[*id2])
        //             // {
        //             //     out << to_string(it) << ",";
        //             // }
        //             // out << endl;
        //         }
        //     }
        //     if (flag)
        //     {
        //         out << endl;
        //     }
        // }
        // return;
        // tmpGroup.clear();
        // vector<set<string>> tmpGroups;
        // int count = 0;
        // for (auto it : unfinishedChunks)
        // {
        //     count++;
        //     if (count > 1000)
        //     {
        //         break;
        //     }
        //     tmpGroup.insert(it);
        //     if (tmpGroup.size() == 16)
        //     {
        //         tmpGroups.push_back(tmpGroup);
        //         tmpGroup.clear();
        //     }
        // }
        // if (tmpGroup.size() > 0)
        // {
        //     tmpGroups.push_back(tmpGroup);
        //     tmpGroup.clear();
        // }
        for (auto id : unfinishedChunks)
        {
            tmpGroup.insert(id);
            if (tmpGroup.size() == 16)
            {
                finishedGroups.push_back(tmpGroup);
                tmpGroup.clear();
            }
        }
        if (tmpGroup.size() > 0)
        {
            finishedGroups.push_back(tmpGroup);
            tmpGroup.clear();
        }
        // second_group(unFullGroups, finishedGroups);
        // tool::Logging(myName_.c_str(), "third Finished group num is %d\n", finishedGroups.size());
        groupNum += finishedGroups.size();
        // 对Group进行排序
        // sort(finishedGroups.begin(), finishedGroups.end(), [](const set<string> &a, const set<string> &b)
        //      { return a.size() > b.size(); });

        // 对group中的chunk以16为一组进行压缩

        // // 验证group中的chunk是否有重复
        // set<string> allChunks;
        // for (auto group : finishedGroups)
        // {
        //     for (auto id : group)
        //     {
        //         if (allChunks.find(id) != allChunks.end())
        //         {
        //             tool::Logging(myName_.c_str(), "chunk %s is in two group\n", id.c_str());
        //         }
        //         allChunks.insert(id);
        //     }
        // }
        int tmpchunkNum = 0;
        for (auto it : finishedGroups)
        {
            tmpchunkNum += it.size();
        }
        tool::Logging(myName_.c_str(), "tmp chunk num is %d\n", tmpchunkNum);
        tool::Logging(myName_.c_str(), "%d chunk with feature is zero\n", table.original_feature_key_table[0].size());
        totalLogicalSize = 0;
        totalCompressedSize = 0;
        map<int, uint64_t> groupLogicalSize;
        map<int, uint64_t> groupCompressedSize;
        for (auto group : finishedGroups)
        {
            if (!groupLogicalSize[group.size()])
            {
                groupLogicalSize[group.size()] = 0;
            }
            if (!groupCompressedSize[group.size()])
            {
                groupCompressedSize[group.size()] = 0;
            }
            for (auto id : group)
            {

                auto start = std::chrono::high_resolution_clock::now();
                Chunk_t GroupTmpChunk = Get_Chunk_Info(stoull(id));
                memcpy(clusterBuffer + clusterSize, GroupTmpChunk.chunkContent, GroupTmpChunk.chunkSize);
                if (GroupTmpChunk.loadFromDisk)
                {
                    free(GroupTmpChunk.chunkContent);
                }
                auto end = std::chrono::high_resolution_clock::now();
                clustringTime += end - start;

                clusterCnt++;
                ChunkNum++;
                // clusterSize += GroupTmpChunk.chunkSize;
                clusterSize += chunkSet[stoull(id)].chunkSize;
            }
            groupLogicalSize[group.size()] += clusterSize;
            // do lz4 compression
            int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);
            if (compressedSize <= 0)
            {
                compressedSize = clusterSize;
            }
            totalCompressedSize += compressedSize;
            totalLogicalSize += clusterSize;
            clusterNum++;
            clusterCnt = 0;
            clusterSize = 0;

            groupCompressedSize[group.size()] += compressedSize;
        }

        out << "group size, logical size, compressed size, ratio" << endl;
        for (auto it : groupLogicalSize)
        {
            out << it.first << ", " << it.second << ", " << groupCompressedSize[it.first] << ", " << (double)it.second / (double)groupCompressedSize[it.first] << endl;
        }
        out.close();

        // process last cluster
        if (clusterCnt > 0)
        {
            int compressedSize = LZ4_compress_fast((char *)clusterBuffer, (char *)lz4ChunkBuffer, clusterSize, clusterSize, 3);

            if (compressedSize <= 0)
            {
                compressedSize = clusterSize;
            }
            totalCompressedSize += compressedSize;
            totalLogicalSize += (clusterCnt * 8 * 1024);
            clusterCnt = 0;
        }

        // tool::Logging(myName_.c_str(), "%d feature with only one chunk and total feature num is %d\n", singeFeature, table.original_feature_key_table.size());

        // calculate the throughput
        double totalTimeInSeconds = featureExtractTime.count() + clustringTime.count();
        double throughput = (double)totalLogicalSize / (double)(totalTimeInSeconds * (1 << 30)); // 转换为GiB/s
        tool::Logging(myName_.c_str(), "Throughput is %f GiB/s\n", throughput);
    }
    recieveQueue->done_ = false;
    return;
}
