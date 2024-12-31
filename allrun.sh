#!/bin/bash

# 定义所有情况的数组
dataSet=("CentOS" "LKT" "WEB" "vmdk" "chromium")
size=("1024")

# 遍历数组并执行 run.sh 脚本
for i in ${dataSet[@]}
do 
    for j in ${size[@]}
    do
        ./run.sh $i $j
    done
done