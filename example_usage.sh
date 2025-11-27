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

# 3. 创建配置文件
echo "3. 创建配置文件..."
python upload.py --create-config

echo ""
echo "请编辑 config.json 文件，填入正确的 DolphinScheduler 连接信息："
echo "- base_url: DolphinScheduler 服务地址"
echo "- token: 访问令牌"
echo "- tenant_id: 租户 ID"
echo ""

# 4. 等待用户编辑配置
read -p "配置文件编辑完成后，按回车继续..."

# 5. 创建测试目录和文件
echo "5. 创建测试文件..."
mkdir -p test_files/subdir
echo "这是一个测试文件" > test_files/test1.txt
echo "另一个测试文件" > test_files/test2.txt
echo "子目录中的文件" > test_files/subdir/test3.txt
echo "# 这是一个Markdown文件" > test_files/readme.md

# 6. 上传测试文件
echo "6. 上传测试文件..."
python upload.py test_files -c config.json

echo ""
echo "=== 上传完成 ==="
echo "您可以检查 DolphinScheduler 中的资源管理页面查看上传的文件"