cd bin

path=/home/public/SXL/data/dataGeneration/multiuser_modifications
name=_multiuser_modifications
num=4
./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt

cd bin

path=/mnt/dataset2/vmdk
name=_vmdk
num=4
./Ftcg  -i $path -c 0 -m 1 -n $num  >Baseline$name.txt
./Ftcg  -i $path -c 0 -m 2 -n $num  >Design1$name.txt
./Ftcg -i $path -c 0 -m 3 -n $num  >Design2$name.txt
./Ftcg  -i $path -c 0 -m 4 -n $num  >Design3$name.txt