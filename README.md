# DolphinScheduler 文件上传工具

一个用于批量上传文件到 DolphinScheduler 的 Python 工具，支持目录遍历、文件存在性检查和增量上传。

## 功能特性

- ✅ **批量上传**: 遍历本地目录下所有文件进行上传
- ✅ **存在检查**: 自动检查文件是否已存在，避免重复上传
- ✅ **增量同步**: 只上传新增或修改的文件
- ✅ **进度显示**: 实时显示上传进度和统计信息
- ✅ **错误处理**: 完善的错误处理和重试机制
- ✅ **日志记录**: 详细的日志记录到文件和终端
- ✅ **配置灵活**: 支持配置文件自定义各种参数

## 环境要求

- Python 3.7+
- DolphinScheduler 服务运行中
- 具有相应权限的访问令牌

## 安装步骤

### 1. 创建虚拟环境

```bash
# 创建虚拟环境
python3 -m venv dolpinscheduler-env

# 激活虚拟环境
# Linux/Mac:
source dolpinscheduler-env/bin/activate
# Windows:
dolphinscheduler-env\\Scripts\\activate
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 创建配置文件

```bash
python upload.py --create-config
```

这将创建一个 `config.json` 文件，请根据实际情况修改配置：

```json
{
  "base_url": "http://localhost:12345/dolphinscheduler",
  "token": "your_access_token_here",
  "tenant_id": 1,
  "parent_resource_id": -1,
  "log_level": "INFO"
}
```

## 配置说明

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `base_url` | DolphinScheduler 服务地址 | `http://localhost:12345/dolphinscheduler` |
| `token` | 访问令牌（通过登录获取） | `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...` |
| `tenant_id` | 租户 ID | `1` |
| `parent_resource_id` | 父资源 ID，-1 表示根目录 | `-1` |
| `log_level` | 日志级别：DEBUG, INFO, WARNING, ERROR | `"INFO"` |

## 使用方法

### 基本用法

```bash
# 上传指定目录下的所有文件
python upload.py /path/to/your/files

# 使用自定义配置文件
python upload.py /path/to/your/files -c /path/to/config.json

# 设置最大并发数（默认5）
python upload.py /path/to/your/files -w 10
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `directory` | 要上传的本地目录路径（必需） | - |
| `-c, --config` | 配置文件路径 | `config.json` |
| `-w, --workers` | 最大并发上传数 | `5` |
| `--create-config` | 创建示例配置文件 | - |

### 获取访问令牌

1. 登录 DolphinScheduler 管理界面
2. 进入用户中心或安全设置
3. 生成或复制访问令牌
4. 将令牌填入配置文件的 `token` 字段

## 工作流程

1. **文件收集**: 递归遍历指定目录，收集所有文件
2. **存在性检查**: 对每个文件检查是否已在 DolphinScheduler 中存在
3. **文件上传**: 上传不存在的文件
4. **结果统计**: 显示上传统计信息和错误详情

## 输出示例

```
2024-01-01 10:00:00,000 - INFO - 开始上传目录: /home/user/files
2024-01-01 10:00:01,000 - INFO - 发现 25 个文件
上传文件: 100%|██████████| 25/25 [00:30<00:00, 1.20s/file, 当前: config.json]
2024-01-01 10:00:31,000 - INFO - 上传完成! 总计: 25, 成功: 20, 跳过: 3, 失败: 2
2024-01-01 10:00:31,000 - ERROR - 失败的文件:
2024-01-01 10:00:31,000 - ERROR -   - 上传失败: large_file.zip, HTTP 413: Request Entity Too Large
2024-01-01 10:00:31,000 - ERROR -   - 上传异常: secret.txt, 错误: [Errno 13] Permission denied

上传耗时: 31.23 秒
```

## 日志文件

程序会在当前目录创建 `uploader.log` 日志文件，记录详细的操作信息：

- 文件上传结果
- 网络请求详情
- 错误信息和堆栈
- 性能统计数据

## 故障排除

### 常见问题

1. **连接超时**
   ```
   检查文件存在性失败: file.txt, 错误: Connection timeout
   ```
   - 检查 DolphinScheduler 服务是否正常运行
   - 验证 `base_url` 配置是否正确
   - 检查网络连接和防火墙设置

2. **认证失败**
   ```
   上传失败: file.txt, 错误: HTTP 401: Unauthorized
   ```
   - 验证 `token` 是否有效且未过期
   - 检查租户 ID 是否正确
   - 确认用户具有文件上传权限

3. **文件过大**
   ```
   上传失败: large_file.zip, HTTP 413: Request Entity Too Large
   ```
   - 检查 DolphinScheduler 的文件大小限制配置
   - 考虑分拆大文件或压缩后上传

4. **权限不足**
   ```
   上传异常: secret.txt, 错误: [Errno 13] Permission denied
   ```
   - 检查本地文件读取权限
   - 验证 DolphinScheduler 中的目标目录权限

### 调试技巧

1. **启用调试日志**：
   ```json
   {
     "log_level": "DEBUG"
   }
   ```

2. **测试单个文件**：
   ```bash
   # 创建只包含一个文件的测试目录
   mkdir test && cp test.txt test/
   python upload.py test
   ```

3. **检查 API 连接**：
   ```bash
   curl -H "Authorization: Bearer YOUR_TOKEN" \
        http://localhost:12345/dolphinscheduler/resources/list?tenantId=1
   ```

## 性能优化

1. **调整并发数**: 根据 DolphinScheduler 服务器性能调整 `-w` 参数
2. **批量上传**: 避免频繁的小文件上传，考虑打包
3. **网络优化**: 在局域网环境中运行可获得更好性能
4. **缓存机制**: 程序会缓存已检查的文件信息，重复运行更快

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！