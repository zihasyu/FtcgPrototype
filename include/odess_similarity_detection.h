#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include "xxhash.h"

using namespace std;

typedef uint64_t feature_t;
typedef unsigned long long super_feature_t;
typedef vector<super_feature_t> SuperFeatures;

// The Mask has X bits of 1's, so the sample rate is 1/(2^X). It means the
// number of sampled chunks to generate feature will be 1/(2^X) of the all
// sliding window chunks.

// 1/(2^9)=1/512
const feature_t k1_512RatioMask = 0x0100400303410010;

// 1/(2^8)=1/256
const feature_t k1_256RatioMask = 0x0100400303410000;

// 1/(2^7)=1/128
const feature_t k1_128RatioMask = 0x0000400303410000;
// 1/(2^5)=1/32
const feature_t k1_32RatioMask = 0x0001000100010011;
// 1/(2^2)=1/4
const feature_t k1_4RatioMask = 0x0000000100000001;

// 1/(2^0)=1
const feature_t k1_1RatioMask = 0x0000000000000000;

// #define FIX_TRANSFORM_ARGUMENT_TO_KEEP_SAME_SIMILARITY_DETECTION_BETWEEN_TESTS

#define HierarchicalSFNum 3

class FeatureGenerator
{
public:
  static const feature_t kDefaultSampleRatioMask = k1_32RatioMask;
  static const size_t kDefaultFeatureNumber = 12;
  static const size_t kDefaultSuperFeatureNumber = 3;

  /**
   * @description: Detect records similarity. Then we can use delta compression
   * to compress the similar values.
   */
  FeatureGenerator(feature_t sample_mask = kDefaultSampleRatioMask,
                   size_t feature_number = kDefaultFeatureNumber,
                   size_t super_feature_number = kDefaultSuperFeatureNumber);

  SuperFeatures GenerateSuperFeatures(const string &value);

  SuperFeatures GetSuperFeatures();

  feature_t GenerateFeature(const string &value);

  vector<feature_t> GenerateFeatures(const string &value);

private:
  /**
   * @summary: Use Odess method to calculate the features of a value. The
   * feature is used to detect similarity.
   * @description:  Use Gear hash to calculate the rolling hash of the values.
   * Use content defined method to sample some of chunks hash values. Use
   * different tramsformation methods to sample the hash value as the similarity
   * feature. If two value has a same feature, we consider they are similar.
   * @param &value the value of record.
   */
  void OdessResemblanceDetect(const string &value);

  void OrginalFeatureDetect(const string &value);

  /**
   * @description: Divide the features into kSuperFeatureNumber groups. Use
   * xxhash on each groups of feature to generate hash value as super feature.
   */
  SuperFeatures MakeSuperFeatures();
  SuperFeatures GroupFeaturesAsSuperFeatures();
  SuperFeatures CopyFeaturesAsSuperFeatures();
  void CleanFeatures();

  vector<feature_t> features_;
  vector<feature_t> random_transform_args_a_;
  vector<feature_t> random_transform_args_b_;

  const feature_t kSampleRatioMask;
  // The super feature are used for similarity detection. The more of super
  // features a record have, the bigger feature index table will be.
  const size_t kFeatureNumber;
  const size_t kSuperFeatureNumber;
};

class FeatureIndexTable
{
public:
  FeatureIndexTable() : Rank_hierarchicalSF(3), Rank_super_features(3) {};
  FeatureIndexTable(feature_t sample_mask, size_t feature_number,
                    size_t super_feature_number)
      : feature_generator_(sample_mask, feature_number, super_feature_number), Rank_hierarchicalSF(3), Rank_super_features(3) {};

  // generate the super features of the value
  // index the key-feature
  void Put(const string &key, const string &value);

  void PutOrignal(const string &key, const string &value);

  void PutOrignals(const string &key, const string &value);

  void PutHierarchicalSF(const string &key, const string &value);
  void PutHSFRank(const string &key, const string &value);
  // 初始化子特征之间的相似度矩阵
  void initOriginalFeatureCorelationMatrix();

  // sort the features of each key
  vector<feature_t> sortFeatureBySetSize();
  vector<feature_t> Rank_feature;
  vector<string> Rank_hierarchicalSF;
  vector<super_feature_t> Rank_super_features;
  // Delete (key, feature_number of super feature) pair and
  // feature_number of (super feature,key) pairs
  void Delete(const string &key);

  // Use key to find all similar records by searching the key-feature table.
  // After that, remove key from the key-feature table
  string GetSimilarRecordsKeys(const string &key);

  string GetSimilarRecordKey(const SuperFeatures &sf);

  // count all similar records that can be delta compressed
  size_t CountAllSimilarRecords() const;
  FeatureGenerator feature_generator_;

  unordered_map<feature_t, set<string>> original_feature_key_table;
  unordered_map<feature_t, set<string>> original_features_key_table;
  map<string, feature_t> original_key_feature_table_;
  map<string, set<feature_t>> original_key_features_table_;
  // vector<feature_t> sorted_original_features;

  // 子特征之间的相似度矩阵
  unordered_map<feature_t, unordered_map<feature_t, int>> original_features_corelation_matrix;

  // 层次SF
  unordered_map<string, set<string>> hierarchicalSFA_key_table;
  unordered_map<string, set<string>> hierarchicalSFB_key_table;
  vector<string> hierarchicalSFC;
  unordered_map<string, set<string>> hierarchicalSFA_B_table;
  unordered_map<string, set<string>> hierarchicalSFB_C_table;
  unordered_map<string, set<string>> hierarchicalSFA_C_table;
  unordered_map<string, vector<string>> key_hierarchicalSF_table;

  unordered_map<super_feature_t, unordered_set<string>> feature_key_table_;
  map<string, SuperFeatures> key_feature_table_;

private:
  // unordered_map<super_feature_t, unordered_set<string>> feature_key_table_;
  // // unordered_map<feature_t, unordered_set<string>> original_feature_key_table;
  // map<string, SuperFeatures> key_feature_table_;
  // map<string, feature_t> orignal_key_feature_table_;

  void ExecuteDelete(const string &key, const SuperFeatures &super_features);

  bool GetSuperFeatures(const string &key, SuperFeatures *super_features);
};