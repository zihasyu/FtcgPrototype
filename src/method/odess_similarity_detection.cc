#include "../../include/odess_similarity_detection.h"
#include "../../include/gear_matrix.h"

#include <cassert>
#include <random>
#include <iostream>
#include <algorithm>
void FeatureIndexTable::Delete(const string &key)
{
    SuperFeatures super_features;
    if (GetSuperFeatures(key, &super_features))
    {
        ExecuteDelete(key, super_features);
    }
}

void FeatureIndexTable::ExecuteDelete(const string &key,
                                      const SuperFeatures &super_features)
{
    for (const super_feature_t &sf : super_features)
    {
        feature_key_table_[sf].erase(key);
    }
    key_feature_table_.erase(key);
}

void FeatureIndexTable::Put(const string &key, const string &value)
{
    SuperFeatures super_features;
    // delete old feature if it exits so we can insert a new one
    Delete(key);

    super_features = feature_generator_.GenerateSuperFeatures(value);
    // cout << super_features[0] << " " << super_features[1] << " " << super_features[2] << endl;
    key_feature_table_[key] = super_features;
    // for (const super_feature_t &sf : super_features)
    // {
    //     feature_key_table_[sf].insert(key);
    // }
    feature_key_table_[super_features[0]].insert(key);
    // std::cout << "feature key table size is : " << feature_key_table_.size() <<std::endl;
}

void FeatureIndexTable::PutOrignal(const string &key, const string &value)
{
    feature_t feature;

    feature = feature_generator_.GenerateFeature(value);
    original_key_feature_table_[key] = feature;
    original_feature_key_table[feature].insert(key);
}

/**
 * @description: Put the original signals of a record into the table.
 * @param &key the key of the record.
 * @param &value the value of the record.
 * @return void
 */
void FeatureIndexTable::PutOrignals(const string &key, const string &value)
{
    vector<feature_t> feature;

    feature = feature_generator_.GenerateFeatures(value);
    for (int i = 0; i < 12; i++)
    {
        original_key_features_table_[key].insert(feature[i]);
        original_features_key_table[feature[i]].insert(key);
    }
    original_key_feature_table_[key] = feature.back();
    original_feature_key_table[feature.back()].insert(key);
}

void FeatureIndexTable::PutHierarchicalSF(const string &key, const string &value)
{
    vector<feature_t> feature;
    vector<super_feature_t> super_features(3);
    vector<string> hierarchicalSF(3);

    feature = feature_generator_.GenerateFeatures(value);
    original_key_feature_table_[key] = feature.back();
    original_feature_key_table[feature.back()].insert(key);

    super_features = feature_generator_.GetSuperFeatures();
    key_feature_table_[key] = super_features;
    feature_key_table_[super_features[0]].insert(key);

    hierarchicalSF[0] = to_string(feature.back());
    hierarchicalSF[1] = to_string(XXH64(&feature[8], sizeof(feature_t) * 3, 0x7fcaf1));
    hierarchicalSF[2] = to_string(XXH64(&feature[0], sizeof(feature_t) * 8, 0x7fcaf1));
    // hierarchicalSFA_key_table[hierarchicalSF[0]].insert(key);
    // hierarchicalSFB_key_table[hierarchicalSF[0] + hierarchicalSF[1]].insert(key);
    // hierarchicalSFC_key_table[hierarchicalSF[0] + hierarchicalSF[1] + hierarchicalSF[2]].insert(key);
    hierarchicalSFA_B_table[hierarchicalSF[0]].insert(hierarchicalSF[0] + hierarchicalSF[1]);
    hierarchicalSFB_C_table[hierarchicalSF[0] + hierarchicalSF[1]].insert(hierarchicalSF[0] + hierarchicalSF[1] + hierarchicalSF[2]);
    hierarchicalSFA_C_table[hierarchicalSF[0]].insert(hierarchicalSF[0] + hierarchicalSF[1] + hierarchicalSF[2]);
    key_hierarchicalSF_table[key] = {hierarchicalSF[0], hierarchicalSF[0] + hierarchicalSF[1], hierarchicalSF[0] + hierarchicalSF[1] + hierarchicalSF[2]};
}
void FeatureIndexTable::PutHSFRank(const string &key, const string &value)
{
    Rank_feature = feature_generator_.GenerateFeatures(value);
    original_key_feature_table_[key] = Rank_feature.back();
    original_feature_key_table[Rank_feature.back()].insert(key);

    Rank_super_features = feature_generator_.GetSuperFeatures();
    key_feature_table_[key] = Rank_super_features;
    feature_key_table_[Rank_super_features[0]].insert(key);

    Rank_hierarchicalSF[0] = to_string(Rank_feature.back());
    Rank_hierarchicalSF[1] = to_string(XXH64(&Rank_feature[8], sizeof(feature_t) * 3, 0x7fcaf1));
    Rank_hierarchicalSF[2] = to_string(XXH64(&Rank_feature[0], sizeof(feature_t) * 8, 0x7fcaf1));
    hierarchicalSFA_C_table[Rank_hierarchicalSF[0]].insert(Rank_hierarchicalSF[0] + Rank_hierarchicalSF[1] + Rank_hierarchicalSF[2]);
    key_hierarchicalSF_table[key] = {Rank_hierarchicalSF[0], Rank_hierarchicalSF[0] + Rank_hierarchicalSF[1], Rank_hierarchicalSF[0] + Rank_hierarchicalSF[1] + Rank_hierarchicalSF[2]};
}
bool FeatureIndexTable::GetSuperFeatures(const string &key,
                                         SuperFeatures *super_features)
{
    auto it = key_feature_table_.find(key);
    if (it == key_feature_table_.end())
    {
        return false;
    }
    else
    {
        *super_features = it->second;
        return true;
    }
}

size_t FeatureIndexTable::CountAllSimilarRecords() const
{
    size_t num = 0;
    unordered_set<string> similar_keys;
    for (const auto &it : feature_key_table_)
    {
        auto keys = it.second;

        // If there are more than one records have the same feature,
        // thoese keys are considered similar
        if (keys.size() > 1)
        {
            for (const string &key : keys)
                similar_keys.emplace(move(key));
        }
    }
    return similar_keys.size();
}

string FeatureIndexTable::GetSimilarRecordsKeys(const string &key)
{

    SuperFeatures super_features;
    if (!GetSuperFeatures(key, &super_features))
    {
        return "not found";
    }
    std::cout << "super feature found!" << std::endl;
    for (const super_feature_t &sf : super_features)
    {
        for (string similar_key : feature_key_table_[sf])
        {
            if (similar_key != key)
            {
                return similar_key;
            }
        }
    }

    return "not found";
    // for (const string &similar_key : similar_keys) {
    //   Delete(similar_key);
    // }
    // ExecuteDelete(key, super_features);
}

string FeatureIndexTable::GetSimilarRecordKey(const SuperFeatures &superfeatures)
{
    for (const super_feature_t &sf : superfeatures)
    {
        if (auto it = feature_key_table_.find(sf) != feature_key_table_.end())
        {
            for (string similar_key : feature_key_table_[sf])
            {
                return similar_key;
            }
        }
    }
    return "not found";
}

FeatureGenerator::FeatureGenerator(feature_t sample_mask, size_t feature_number,
                                   size_t super_feature_number)
    : kSampleRatioMask(sample_mask), kFeatureNumber(feature_number),
      kSuperFeatureNumber(super_feature_number)
{
    assert(kFeatureNumber % kSuperFeatureNumber == 0);

    std::random_device rd;
    std::default_random_engine e(0);
    std::uniform_int_distribution<feature_t> dis(0, UINT64_MAX);

    features_.resize(kFeatureNumber);
    random_transform_args_a_.resize(kFeatureNumber);
    random_transform_args_b_.resize(kFeatureNumber);

    for (size_t i = 0; i < kFeatureNumber; ++i)
    {
#ifdef FIX_TRANSFORM_ARGUMENT_TO_KEEP_SAME_SIMILARITY_DETECTION_BETWEEN_TESTS
        random_transform_args_a_[i] = GEARmx[i];
        random_transform_args_b_[i] = GEARmx[kFeatureNumber + i];
#else
        random_transform_args_a_[i] = dis(e);
        random_transform_args_b_[i] = dis(e);
#endif
        features_[i] = 0;
    }
}

void FeatureGenerator::OdessResemblanceDetect(const string &value)
{
    feature_t hash = 0;
    for (size_t i = 0; i < value.size(); ++i)
    {
        hash = (hash << 1) + GEARmx[static_cast<uint8_t>(value[i])];
        if (!(hash & kSampleRatioMask))
        {
            for (size_t j = 0; j < kFeatureNumber; ++j)
            {
                feature_t transform_res =
                    hash * random_transform_args_a_[j] + random_transform_args_b_[j];
                if (transform_res > features_[j])
                    features_[j] = transform_res;
            }
        }
    }
}

void FeatureGenerator::OrginalFeatureDetect(const string &value)
{
    feature_t hash = 0;
    for (size_t i = 0; i < value.size(); ++i)
    {
        hash = (hash << 1) + GEARmx[static_cast<uint8_t>(value[i])];
        if (!(hash & kSampleRatioMask))
        {
            for (size_t j = 0; j < kFeatureNumber; ++j)
            {
                feature_t transform_res =
                    hash * random_transform_args_a_[j] + random_transform_args_b_[j];
                if (transform_res > features_[j])
                    features_[j] = transform_res;
            }
        }
    }
}

SuperFeatures FeatureGenerator::MakeSuperFeatures()
{
    if (kSuperFeatureNumber == kFeatureNumber)
        return CopyFeaturesAsSuperFeatures();
    else
        return GroupFeaturesAsSuperFeatures();
}

SuperFeatures FeatureGenerator::CopyFeaturesAsSuperFeatures()
{
    SuperFeatures super_features(features_.begin(), features_.end());
    return super_features;
}

// Divede features into groups, then use the group hash as the super feature
SuperFeatures FeatureGenerator::GroupFeaturesAsSuperFeatures()
{
    SuperFeatures super_features(kSuperFeatureNumber);
    for (size_t i = 0; i < kSuperFeatureNumber; ++i)
    {
        size_t group_len = kFeatureNumber / kSuperFeatureNumber;
        super_features[i] = XXH64(&features_[i * group_len],
                                  sizeof(feature_t) * group_len, 0x7fcaf1);
    }
    return super_features;
}

void FeatureGenerator::CleanFeatures()
{
    for (size_t i = 0; i < kFeatureNumber; ++i)
    {
        features_[i] = 0;
    }
}

SuperFeatures FeatureGenerator::GenerateSuperFeatures(const string &value)
{
    CleanFeatures();
    OdessResemblanceDetect(value);
    return MakeSuperFeatures();
}

SuperFeatures FeatureGenerator::GetSuperFeatures()
{
    return MakeSuperFeatures();
}
feature_t FeatureGenerator::GenerateFeature(const string &value)
{
    CleanFeatures();
    OdessResemblanceDetect(value);
    sort(features_.begin(), features_.end());
    return features_.back();
}

/**
 * @description: Generate the features of a value. The feature is used to detect
 * similarity.
 * @param &value the value of record.
 * @return vector<feature_t> the features of the value.
 */
vector<feature_t> FeatureGenerator::GenerateFeatures(const string &value)
{
    CleanFeatures();
    OdessResemblanceDetect(value);
    sort(features_.begin(), features_.end());
    return features_;
}

vector<feature_t> FeatureIndexTable::sortFeatureBySetSize()
{
    vector<feature_t> sorted_features;
    for (auto it : original_feature_key_table)
    {
        sorted_features.push_back(it.first);
    }

    // sort the features by the size of the key set in descending order
    sort(sorted_features.begin(), sorted_features.end(), [&](feature_t a, feature_t b)
         { return original_feature_key_table[a].size() < original_feature_key_table[b].size(); });
    return sorted_features;
}

void FeatureIndexTable::initOriginalFeatureCorelationMatrix()
{
    for (auto it : original_key_features_table_)
    {
        for (auto feature : it.second)
        {
            for (auto feature2 : it.second)
            {
                // if (!original_features_corelation_matrix[feature][feature2] && feature != feature2)
                // {
                //     original_features_corelation_matrix[feature][feature2] = 0;
                // }
                if (feature != feature2)
                {
                    original_features_corelation_matrix[feature][feature2] += 1;
                }
            }
        }
    }
}