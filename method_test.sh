cd bin
chunktype=0
method=0



path=/mnt/dataset2/cassandra
name=_cassandra
num=4
./Ftcg  -i $path -c $chunktype -m $method -n $num  >method_$name.txt

path=/mnt/dataset2/LKT
name=_LKT
# num=4
./Ftcg  -i $path -c $chunktype -m $method -n $num  >method_$name.txt


path=/mnt/dataset2/WEB
name=_WEB
# num=4
./Ftcg  -i $path -c $chunktype -m $method -n $num  >method_$name.txt

path=/mnt/dataset2/chromium
name=_chromium
# num=4
./Ftcg  -i $path -c $chunktype -m $method -n $num  >method_$name.txt
