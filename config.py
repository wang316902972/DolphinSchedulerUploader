"""
DolphinScheduler 上传工具配置文件
包含所有可配置的参数，便于维护和修改
"""

# DolphinScheduler 服务器配置
BASE_URL = "http://14.103.67.28:12345/dolphinscheduler"

# 认证配置 - 使用Token认证
ACCESS_TOKEN = "621a68a6631741b2c36919b0bb94c85d"  # 替换为实际的Access Token

# 备用：保持原有cookie配置以便兼容
AUTH_COOKIE = {
    "sessionId": "621a68a6631741b2c36919b0bb94c85d"  # 替换为实际的Session ID
}

# 资源上传参数 - 使用online-create端点用于支持在线查看的文件类型
UPLOAD_PATH = "/resources/online-create"
RESOURCE_TYPE = "FILE"
TENANT_ID = 21  # 租户ID
PARENT_DIR_ID = 97  # 父目录ID，根目录通常为-1或0
CURRENT_DIR = ""

# 请求配置
REQUEST_TIMEOUT = 300  # 请求超时时间（秒）
VERIFY_SSL = False  # 是否验证SSL证书

# 日志配置
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# 文件处理配置
CHUNK_SIZE = 8192  # 文件读取块大小
SUPPORTED_EXTENSIONS = [".jar", ".zip", ".tar", ".gz"]  # 支持的文件扩展名

# 错误重试配置
MAX_RETRIES = 3  # 最大重试次数
RETRY_DELAY = 1  # 重试延迟（秒）

# 批量上传配置
BATCH_SIZE = 10  # 批量处理大小
MAX_CONCURRENT_UPLOADS = 5  # 最大并发上传数

def get_auth_config():
    """
    获取认证配置

    Returns:
        dict: 认证配置字典
    """
    return {
        "token": ACCESS_TOKEN,
        "cookie": AUTH_COOKIE,
        "timeout": REQUEST_TIMEOUT,
        "verify": VERIFY_SSL
    }

def get_upload_config():
    """
    获取上传配置

    Returns:
        dict: 上传配置字典
    """
    return {
        "base_url": BASE_URL,
        "upload_path": UPLOAD_PATH,
        "resource_type": RESOURCE_TYPE,
        "parent_dir_id": PARENT_DIR_ID,
        "current_dir": CURRENT_DIR
    }

def get_request_config():
    """
    获取请求配置

    Returns:
        dict: 请求配置字典
    """
    return {
        "timeout": REQUEST_TIMEOUT,
        "verify": VERIFY_SSL,
        "max_retries": MAX_RETRIES,
        "retry_delay": RETRY_DELAY
    }

def get_file_config():
    """
    获取文件处理配置

    Returns:
        dict: 文件处理配置字典
    """
    return {
        "chunk_size": CHUNK_SIZE,
        "supported_extensions": SUPPORTED_EXTENSIONS,
        "batch_size": BATCH_SIZE
    }

def get_batch_config():
    """
    获取批量上传配置

    Returns:
        dict: 批量上传配置字典
    """
    return {
        "max_concurrent_uploads": MAX_CONCURRENT_UPLOADS,
        "batch_size": BATCH_SIZE
    }

def get_log_config():
    """
    获取日志配置

    Returns:
        dict: 日志配置字典
    """
    return {
        "level": LOG_LEVEL,
        "format": LOG_FORMAT
    }

def validate_config():
    """
    验证配置参数的有效性

    Returns:
        tuple: (is_valid: bool, error_message: str)
    """
    errors = []

    # 验证BASE_URL
    if not BASE_URL or not BASE_URL.startswith(('http://', 'https://')):
        errors.append("BASE_URL 必须是有效的HTTP/HTTPS URL")

    # 验证认证配置
    if not AUTH_COOKIE or not isinstance(AUTH_COOKIE, dict):
        errors.append("AUTH_COOKIE 必须是非空的字典")

    # 验证数值参数
    if REQUEST_TIMEOUT <= 0:
        errors.append("REQUEST_TIMEOUT 必须大于0")

    if MAX_RETRIES < 0:
        errors.append("MAX_RETRIES 必须大于等于0")

    if CHUNK_SIZE <= 0:
        errors.append("CHUNK_SIZE 必须大于0")

    if BATCH_SIZE <= 0:
        errors.append("BATCH_SIZE 必须大于0")

    if MAX_CONCURRENT_UPLOADS <= 0:
        errors.append("MAX_CONCURRENT_UPLOADS 必须大于0")

    # 验证扩展名列表
    if not isinstance(SUPPORTED_EXTENSIONS, list) or not SUPPORTED_EXTENSIONS:
        errors.append("SUPPORTED_EXTENSIONS 必须是非空的列表")

    if errors:
        return False, "; ".join(errors)

    return True, "配置验证通过"

# 配置使用示例
if __name__ == "__main__":
    print("=== DolphinScheduler 上传工具配置 ===")
    print(f"BASE_URL: {BASE_URL}")
    print(f"UPLOAD_PATH: {UPLOAD_PATH}")
    print(f"RESOURCE_TYPE: {RESOURCE_TYPE}")
    print(f"PARENT_DIR_ID: {PARENT_DIR_ID}")
    print(f"REQUEST_TIMEOUT: {REQUEST_TIMEOUT}")
    print(f"MAX_RETRIES: {MAX_RETRIES}")
    print(f"SUPPORTED_EXTENSIONS: {SUPPORTED_EXTENSIONS}")
    print(f"MAX_CONCURRENT_UPLOADS: {MAX_CONCURRENT_UPLOADS}")

    # 验证配置
    is_valid, message = validate_config()
    print(f"\n配置验证: {'✓ 通过' if is_valid else '✗ 失败'}")
    if not is_valid:
        print(f"错误信息: {message}")