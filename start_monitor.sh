#!/bin/bash
# DolphinScheduler 文件监听服务启动脚本

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐬 DolphinScheduler 文件监听服务启动脚本${NC}"
echo "========================================"

# 项目根目录
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# 默认配置
WATCH_DIR="${1:-test_files}"
WORKERS="${2:-3}"

echo -e "${GREEN}配置信息:${NC}"
echo "  项目目录: $PROJECT_DIR"
echo "  监听目录: $WATCH_DIR"
echo "  上传线程: $WORKERS"
echo "  虚拟环境: $PROJECT_DIR/dolphinscheduler-env"
echo

# 检查监听目录
if [ ! -d "$WATCH_DIR" ]; then
    echo -e "${RED}❌ 错误: 监听目录不存在: $WATCH_DIR${NC}"
    exit 1
fi

# 检查虚拟环境
if [ ! -d "dolphinscheduler-env" ]; then
    echo -e "${RED}❌ 错误: 虚拟环境不存在: dolphinscheduler-env${NC}"
    exit 1
fi

# 检查必要文件
echo -e "${BLUE}📋 检查依赖...${NC}"
required_files=(
    "file_upload.py"
    "config.py"
)

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}❌ 缺少必要文件: $file${NC}"
        exit 1
    else
        echo -e "  ✅ $file"
    fi
done

# 检查并安装依赖
echo -e "${BLUE}🔧 检查虚拟环境依赖...${NC}"

# 在虚拟环境中检查并安装watchdog
dolphinscheduler-env/bin/python3 -c "
import sys
try:
    from watchdog.observers import Observer
    print('✅ watchdog 已安装')
except ImportError:
    print('❌ 需要安装 watchdog')
    import subprocess
    subprocess.run([sys.executable, '-m', 'pip', 'install', 'watchdog'])
    print('✅ watchdog 安装完成')
"

# 启动监听服务
cd "$PROJECT_DIR"
dolphinscheduler-env/bin/python3 file_listener_service.py "$WATCH_DIR" -w "$WORKERS" 2>&1 | tee "$LOG_FILE"