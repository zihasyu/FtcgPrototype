
size=1024

mSet=(3)
dataSet=("/home/public/SXL/data/dataGeneration/multiuser_modifications" "/mnt/dataset2/LKT" "/mnt/dataset2/WEB" "/mnt/dataset2/vmdk" "/mnt/dataset2/chromium")
# dataSet=("/mnt/dataset2/ThunderbirdTar/")

cd bin
for i in ${mSet[@]}
do     
    for j in ${dataSet[@]}
    do
        echo "Running LZ4Cluster on $j with size $size and m $i">>../output.log
        echo ./LZ4Cluster -i $j -m $i -c 0 -n 4 -s $size>>../output.log
        ./LZ4Cluster -i $j -m $i -c 0 -n 4 -s $size>>../output.log 2>&1
        # 输出分割线
        echo "--------------------------------------------------" >>../output.log
    done
done


