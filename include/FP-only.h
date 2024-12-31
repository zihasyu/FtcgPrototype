#ifndef FP_ONLY_H
#define FP_ONLY_H

#include "absMethod.h"
#include "odess_similarity_detection.h"
#include "NTransFormSF.h"
#include "finesse.h"

class FPOnly : public absMethod
{
private:
  /* data */
  string myName_ = "FPOnly";
  string fileName;
  // Feature Table
  FeatureIndexTable table;
  NTransIndexTable nTransTable;
  FinesseIndexTable finesseTable;
  // cluster
  uint8_t *clusterBuffer;
  int clusterCnt = 0;
  uint64_t clusterSize = 0;

  // chunk set

  // time static
  std::chrono::duration<double> featureExtractTime;
  std::chrono::duration<double> clustringTime;

public:
  FPOnly();
  ~FPOnly();

  void ProcessOneTrace();

  // void second_group(map<feature_t, set<string>> unFullGroups, vector<set<string>> &group);
};
#endif