
dir=''
size=1024

if [ $# -lt 1 ]; then
    echo "Usage: $0 {CentOS|LKT|cassandra|WEB|vmdk|Log} {512|1024|2048}"
    exit 1
fi

case $1 in
    CentOS)
        dir='/home/public/SXL/data/dataGeneration/multiuser_modifications'
        ;;
    LKT)
        dir='/mnt/dataset2/LKT'
        ;;
    Cassandra)
        dir='/mnt/dataset2/cassandra'
        ;;
    WEB)
        dir='/mnt/dataset2/WEB'
        ;;
    vmdk)
        dir='/mnt/dataset2/vmdk'
        ;;
    chromium)
        dir='/mnt/dataset2/chromium'
        ;;
    Log)
        dir='/mnt/dataset2/ThunderbirdTar'
        ;;
    *)
        echo "Usage: $0 {CentOS|LKT|Cassandra|WEB|vmdk|Log} {512|1024|2048}"
        exit 1
        ;;
esac

if [ $# -eq 2 ]; then
    size=$2
fi

cd bin
echo "Running LZ4Cluster on $1 with size $size">>../output.log
./Ftcg -i $dir -m 1 -c 0 -n 2 -s $size>>../output.log 2>&1
# echo "--------------------------------------------------" >>../output.log
# ./LZ4Cluster -i $dir -m 1 -c 0 -n 1 -s $size>>../output.log 2>&1
# 输出分割线
echo "--------------------------------------------------" >>../output.log