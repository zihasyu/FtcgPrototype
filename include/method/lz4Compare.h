#ifndef LZ4_COMPARE_H
#define LZ4_COMPARE_H

#include "absMethod.h"

class lz4Compare : public absMethod
{
private:
    /* data */
    string myName_ = "lz4Compare";

public:
    lz4Compare();
    ~lz4Compare();

    void ProcessOneTrace();
};
#endif