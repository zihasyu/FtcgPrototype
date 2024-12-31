# 获取脚本所在的绝对路径
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# 完全删除并重新创建 build 目录
if [ -d "$SCRIPT_DIR/build" ]; then
    echo "Cleaning the build directory"
    rm -rf "$SCRIPT_DIR/build"
fi
echo "Creating the build directory"
mkdir -p "$SCRIPT_DIR/build"

# 进入 build 目录，运行 cmake 和 make
cd "$SCRIPT_DIR/build"
cmake ..
make -j4

# 清理 ./bin 目录下的 .txt 文件，并确保 Containers 和 restoreFile 目录存在
cd "$SCRIPT_DIR/bin"
rm -f *.txt
mkdir -p Containers restoreFile

# 如果 "./bin/Containers" 目录存在，则清理其内容
if [ -d "Containers" ]; then
    echo "Cleaning the Containers directory"
    rm -rf Containers/*
    echo "Done!"
fi