#!/bin/bash
# DolphinScheduler 文件上传工具使用示例

echo "=== DolphinScheduler 文件上传工具使用示例 ==="
echo ""

# 1. 创建虚拟环境
echo "1. 创建虚拟环境..."
python3 -m venv dolphinscheduler-env
source dolphinscheduler-env/bin/activate

# 2. 安装依赖
echo "2. 安装依赖..."
pip install -r requirements.txt


# 3. 上传测试文件
echo "3. 上传测试文件..."
python upload.py test_files -c config.json

echo ""
echo "=== 上传完成 ==="
echo "您可以检查 DolphinScheduler 中的资源管理页面查看上传的文件"