cd bin

path=/mnt/dataset2/cassandra
name=_cassandra
num=4
./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt

path=/mnt/dataset2/LKT
name=_LKT
# num=4

./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt

path=/mnt/dataset2/WEB
name=_WEB
# num=4
./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt
path=/mnt/dataset2/chromium
name=_chromium
# num=4
./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt
