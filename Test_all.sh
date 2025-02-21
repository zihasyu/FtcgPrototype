run_ftcg() {
  local path=$1
  local name=$2
  local num=$3
  sudo echo 3 > /proc/sys/vm/drop_caches
  ./Ftcg -i $path -c 0 -m 1 -n $num > Baseline$name.txt
  sudo echo 3 > /proc/sys/vm/drop_caches
  ./Ftcg -i $path -c 0 -m 2 -n $num > Design1$name.txt
  sudo echo 3 > /proc/sys/vm/drop_caches
  ./Ftcg -i $path -c 0 -m 3 -n $num > Design2$name.txt
  sudo echo 3 > /proc/sys/vm/drop_caches
  # ./Ftcg -i $path -c 0 -m 4 -n $num > Design3$name.txt
  # sudo echo 3 > /proc/sys/vm/drop_caches
}

fileNum=4
cd bin
run_ftcg /mnt/dataset2/cassandra _cassandra $fileNum
run_ftcg /mnt/dataset2/LKT _LKT $fileNum
run_ftcg /mnt/dataset2/WEB _WEB $fileNum
run_ftcg /mnt/dataset2/chromium _chromium $fileNum
run_ftcg /mnt/dataset2/automake_tarballs _automake 16
run_ftcg /mnt/dataset2/bash_tarballs _bash 8
run_ftcg /mnt/dataset2/coreutils_tarballs _coreutils 16
run_ftcg /mnt/dataset2/fdisk_tarballs _fdisk 16
run_ftcg /mnt/dataset2/glibc_tarballs _glibc $fileNum
run_ftcg /mnt/dataset2/smalltalk_tarballs _smalltalk $fileNum