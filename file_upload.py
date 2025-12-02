#!/usr/bin/env python3
"""
DolphinScheduler 真实文件上传工具

功能:
1. 使用Base64编码上传文件到DolphinScheduler资源系统
2. 支持文件完整性验证和重复检查
3. 批量上传和目录结构保持
4. 详细的进度跟踪和错误处理

要求: Python 3.7+
依赖包: requests, tqdm
"""

import os
import sys
import json
import hashlib
import logging
import mimetypes
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import argparse

import requests
from tqdm import tqdm

# 导入配置文件
try:
    import config
    print("成功导入配置文件: config.py")
except ImportError as e:
    print(f"错误: 无法导入配置文件 config.py: {e}")
    print("请确保 config.py 文件存在且可访问")
    sys.exit(1)


class DolphinSchedulerFileUploader:
    """DolphinScheduler 真实文件上传器"""

    def __init__(self, use_config_file: bool = False, config_file: str = "config.json"):
        """
        初始化文件上传器

        Args:
            use_config_file: 是否使用传统JSON配置文件
            config_file: 配置文件路径
        """
        # 首先设置基本的日志配置（临时）
        self._setup_basic_logging()

        if use_config_file:
            # 使用传统JSON配置文件（向后兼容）
            self.config = self._load_config(config_file)
            self.auth_type = self.config.get('auth_type', 'token_header')
            self.timeout = self.config.get('timeout', 300)
            self.max_retries = self.config.get('max_retries', 3)
        else:
            # 使用新的config.py配置文件
            self._load_from_module_config()
            self.use_config_file = False

        # 创建会话
        self.session = requests.Session()

        # 重新设置完整的日志配置（基于配置文件）
        self._setup_logging()

        # 根据认证类型设置请求头
        self._setup_authentication()

        # 文件存在检查缓存
        self.existing_files_cache = set()

    def _setup_basic_logging(self):
        """设置基本日志配置（初始化时使用）"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)

    def _load_from_module_config(self):
        """
        从config.py模块加载配置
        """
        # 验证配置
        is_valid, error_msg = config.validate_config()
        if not is_valid:
            raise ValueError(f"配置验证失败: {error_msg}")

        # 获取各类配置
        upload_config = config.get_upload_config()
        auth_config = config.get_auth_config()
        request_config = config.get_request_config()

        # 合并配置
        self.config = {
            'base_url': upload_config['base_url'],
            'tenant_id': 21,  # 从现有配置推断
            'tenant_code': 'dt_ads_biz',  # 从现有配置推断
            'parent_resource_id': upload_config['parent_dir_id'],
            'log_level': config.get_log_config()['level'],
            'token': auth_config.get('token') or auth_config.get('cookie', {}).get('sessionId')  # 优先使用token，fallback到sessionId
        }

        # 请求配置
        self.timeout = request_config['timeout']
        self.verify_ssl = request_config['verify']

        # 上传特定配置
        self.upload_path = upload_config['upload_path']
        self.resource_type = upload_config['resource_type']
        self.current_dir = upload_config['current_dir']

        # 认证类型：统一使用token_header方式
        self.auth_type = 'token_header'

        self.logger.info(f"从config.py加载配置成功: {upload_config['base_url']}")
        self.logger.info(f"使用Token认证: {self.config['token'][:20]}..." if self.config.get('token') else "警告: 未配置Token")
        # 临时设置DEBUG级别
        self.logger.setLevel(logging.DEBUG)

    def _setup_authentication(self):
        """
        设置认证信息 - 统一使用Token Header方式
        """
        # 清除其他认证方式的header
        self.session.headers.pop('Authorization', None)

        # 统一使用token header认证
        if self.config.get('token'):
            self.session.headers.update({
                'token': self.config['token']
            })
            self.logger.info(f"使用Token头认证: token={self.config['token'][:20]}...")
        else:
            self.logger.error("未找到有效的token配置")
            raise ValueError("认证配置错误: 未找到token")

    def _load_config(self, config_file: str) -> Dict:
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 验证必要配置
            required_fields = ['base_url', 'token', 'tenant_id']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"配置文件缺少必要字段: {field}")

            return config

        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {config_file}")
        except json.JSONDecodeError:
            raise ValueError(f"配置文件格式错误: {config_file}")

    def _setup_logging(self):
        """设置完整的日志配置"""
        log_level = self.config.get('log_level', 'INFO')
        log_format = '%(asctime)s - %(levelname)s - %(message)s'

        # 清除现有的handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # 重新配置日志
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=[
                logging.FileHandler('file_uploader.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True  # 强制重新配置
        )

        # 重新获取logger实例
        self.logger = logging.getLogger(__name__)
        self.logger.info("日志系统初始化完成")

    def _get_file_md5(self, file_path: str) -> str:
        """计算文件MD5值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_content_type(self, file_path: str) -> str:
        """获取文件的MIME类型"""
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type is None:
            # 如果无法猜测类型，使用默认类型
            ext = os.path.splitext(file_path)[1].lower()
            type_mapping = {
                '.txt': 'text/plain',
                '.py': 'text/x-python',
                '.sql': 'application/sql',
                '.json': 'application/json',
                '.xml': 'application/xml',
                '.yml': 'application/x-yaml',
                '.yaml': 'application/x-yaml',
                '.properties': 'text/plain',
                '.sh': 'application/x-sh',
                '.bat': 'application/x-msdownload',
                '.jar': 'application/java-archive',
                '.zip': 'application/zip',
                '.tar': 'application/x-tar',
                '.gz': 'application/gzip',
                '.md': 'text/markdown'
            }
            return type_mapping.get(ext, 'application/octet-stream')
        return mime_type

    def _check_file_exists(self, file_name: str, file_size: int, file_md5: str) -> bool:
        """
        检查文件是否已存在

        Args:
            file_name: 文件名
            file_size: 文件大小
            file_md5: 文件MD5值

        Returns:
            bool: 文件是否已存在
        """
        # 检查缓存
        cache_key = f"{file_name}_{file_size}_{file_md5}"
        if cache_key in self.existing_files_cache:
            return True

        try:
            # 使用基础资源URL - 避免重复路径
            url = f"{self.config['base_url']}/resources"
            # 确保URL中没有重复的/dolphinscheduler
            url = url.replace('/dolphinscheduler/dolphinscheduler', '/dolphinscheduler')

            params = {
                'tenantId': self.config['tenant_id'],
                'searchVal': file_name,  # 使用searchVal参数进行模糊搜索
                'page': 1,
                'pageSize': 10
            }

            # 统一使用token header认证
            headers = {
                'token': self.config['token'],
                'Content-Type': 'application/json'
            }

            # 使用配置的超时时间
            timeout = getattr(self, 'timeout', 30)

            response = self.session.get(url, params=params, headers=headers, timeout=timeout, verify=getattr(self, 'verify_ssl', True))
            response.raise_for_status()

            data = response.json()
            self.logger.debug(f"文件存在检查响应: {data}")
            if data.get('code') == 0 and data.get('data'):
                for resource in data['data']:
                    if (resource.get('alias') == file_name and
                        resource.get('size') == file_size):
                        # 添加到缓存
                        self.existing_files_cache.add(cache_key)
                        self.logger.info(f"文件已存在，跳过: {file_name}")
                        return True

            return False

        except requests.exceptions.RequestException as e:
            self.logger.error(f"检查文件存在性失败: {file_name}, 错误: {e}")
            # 网络错误时假设文件不存在，尝试上传
            return False

    def _upload_single_file(self, file_path: str, relative_path: str, parent_id: int = None) -> Tuple[bool, str]:
        """
        上传单个文件（使用真实文件上传）

        Args:
            file_path: 文件完整路径
            relative_path: 相对路径
            parent_id: 父资源ID，如果为None则使用配置中的默认值

        Returns:
            Tuple[bool, str]: (是否成功, 消息)
        """
        # 如果未指定parent_id，使用配置中的默认值
        if parent_id is None:
            parent_id = self.config.get('parent_resource_id', -1)

        try:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_md5 = self._get_file_md5(file_path)

            # 检查文件是否已存在
            if self._check_file_exists(file_name, file_size, file_md5):
                return True, f"文件已存在，跳过: {relative_path}"

            # 1. 准备文件上传（不需要Base64编码）
            self.logger.info(f"准备上传文件: {file_path} (大小: {file_size} 字节)")
            self.logger.debug(f"文件MD5: {file_md5}")

            # 2. 准备请求参数
            # 从文件名中提取后缀，并处理DolphinScheduler API要求
            suffix = os.path.splitext(file_name)[1].lstrip('.')

            # DolphinScheduler API requires lowercase file suffixes!
            # CRITICAL: Must use lowercase, not uppercase
            supported_suffixes = ['jar', 'zip', 'tar', 'gz', 'py', 'sql', 'json', 'xml',
                                'properties', 'yml', 'yaml', 'sh', 'bat', 'md', 'txt']

            # 使用小写后缀 - DolphinScheduler API 期望小写!
            # For unsupported types, default to 'txt' (lowercase)
            if suffix:
                mapped_suffix = suffix.lower() if suffix.lower() in supported_suffixes else 'txt'
            else:
                mapped_suffix = 'txt'

            self.logger.debug(f"原始后缀: '{suffix}', 映射后缀: '{mapped_suffix}'")

            # 使用配置的上传路径
            upload_url = f"{self.config['base_url']}{self.upload_path}"
       

            # 构造查询参数
            # 支持在线查看的文件类型列表 (必须为小写)
            online_viewable_suffixes = ['txt', 'py', 'sql', 'sh', 'md', 'json', 'xml', 'properties', 'yml', 'yaml',
                                       'jar', 'zip', 'tar', 'gz', 'bat']

            # 构造基本参数
            # CRITICAL FIX: Remove extension from fileName to prevent double extensions
            # DolphinScheduler will append the suffix based on the 'suffix' parameter
            file_name_without_ext = os.path.splitext(file_name)[0]
            
            # 构造表单数据
            form_data = {
                "currentDir": "",  # 空字符串而不是'/'
                "description": f"Uploaded via File API - {relative_path}",
                "name": file_name,  # 使用原始文件名
                "pid": str(parent_id),  # 确保转换为字符串
                "type": "FILE"
            }

            # 统一使用token header认证
            headers = {
                'token': self.config['token']
            }

            self.logger.info(f"正在上传文件 {file_name} (大小: {file_size} 字节)...")
            self.logger.debug(f"目标URL: {upload_url}")

            # 2. 直接读取文件内容作为二进制数据进行上传
            with open(file_path, "rb") as f:
                file_content_raw = f.read()

            self.logger.debug(f"文件大小: {len(file_content_raw)} 字节")

            # 3. 发送POST请求 (带重试机制)
            # 直接将文件内容作为字符串上传到content参数
            timeout = getattr(self, 'timeout', 300)

            files = {
                'file': (file_name, file_content_raw, self._get_content_type(file_path))
            }

            self.logger.debug(f"上传参数: {form_data}")
            self.logger.debug(f"请求头: {headers}")

            response = requests.post(
                upload_url,
                data=form_data,  # 包含content参数的表单数据
                headers=headers,
                files=files,
                timeout=timeout,
                verify=getattr(self, 'verify_ssl', True)
            )

            # 4. 处理响应
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    # 添加到缓存
                    self.existing_files_cache.add(f"{file_name}_{file_size}_{file_md5}")
                    self.logger.info(f"上传成功: {relative_path}")
                    self.logger.debug(f"响应结果: {result}")
                    return True, f"上传成功: {relative_path}"
                else:
                    error_msg = result.get('msg', '未知错误')
                    self.logger.error(f"上传失败: {relative_path}, 错误: {error_msg}")
                    self.logger.error(f"完整响应: {result}")
                    self.logger.error(f"表单参数: {form_data}")
                    return False, f"上传失败: {relative_path}, 错误: {error_msg}"
            elif response.status_code == 401:
                # 专门处理401错误
                self.logger.error(f"认证失败 (401): {relative_path}")
                self.logger.error(f"Token: {self.config['token'][:20]}...")
                self.logger.error(f"表单参数: {form_data}")
                self.logger.error(f"响应内容: {response.text}")
                return False, f"认证失败 (401): {relative_path}, 请检查token是否有效"
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(f"上传失败: {relative_path}, {error_msg}")
                self.logger.error(f"请求头: {headers}")
                self.logger.error(f"请求URL: {upload_url}")
                self.logger.error(f"响应状态码: {response.status_code}")
                self.logger.error(f"响应头: {dict(response.headers)}")
                self.logger.debug(f"响应内容: {response.text}")
                return False, f"上传失败: {relative_path}, {error_msg}"

        except FileNotFoundError:
            error_msg = f"文件未找到: {file_path}"
            self.logger.error(error_msg)
            return False, f"上传异常: {relative_path}, 错误: {error_msg}"
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"上传异常: {relative_path}, 错误: {error_msg}")
            return False, f"上传异常: {relative_path}, 错误: {error_msg}"

    def _collect_files(self, directory: str) -> List[Tuple[str, str]]:
        """
        收集目录下所有文件

        Args:
            directory: 本地目录路径

        Returns:
            List[Tuple[str, str]]: [(完整路径, 相对路径)]
        """
        files = []
        directory_path = Path(directory)

        if not directory_path.exists():
            raise FileNotFoundError(f"目录不存在: {directory}")

        if not directory_path.is_dir():
            raise ValueError(f"路径不是目录: {directory}")

        # 遍历目录
        for file_path in directory_path.rglob('*'):
            if file_path.is_file():
                # 计算相对路径
                relative_path = file_path.relative_to(directory_path)
                # 使用路径作为文件名，保持目录结构
                relative_name = str(relative_path).replace(os.sep, '/')

                files.append((str(file_path), relative_name))

        self.logger.info(f"发现 {len(files)} 个文件")
        return files

    def upload_to_directory(self, directory: str, parent_resource: str = None,
                          max_workers: int = 5) -> Dict:
        """
        上传整个目录到指定父资源目录

        Args:
            directory: 本地目录路径
            parent_resource: 父资源名称或路径，如果为None则使用配置中的parent_resource_id
            max_workers: 最大并发数

        Returns:
            Dict: 上传结果统计
        """
        self.logger.info(f"开始上传目录: {directory}")

        # 如果指定了父资源名称，先查找其ID
        parent_id = self.config.get('parent_resource_id', -1)
        if parent_resource:
            # 简单实现：直接使用配置中的parent_resource_id
            # 在实际使用中，可以扩展为搜索父资源功能
            self.logger.info(f"使用父资源: {parent_resource} (ID: {parent_id})")

        self.logger.info(f"上传到父资源ID: {parent_id}")

        # 收集文件
        files = self._collect_files(directory)

        if not files:
            self.logger.warning("目录中没有找到文件")
            return {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'errors': []}

        # 统计信息
        stats = {
            'total': len(files),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }

        # 使用进度条
        with tqdm(files, desc="上传文件", unit="file") as pbar:
            for file_path, relative_path in pbar:
                pbar.set_postfix_str(f"当前: {relative_path}")

                success, message = self._upload_single_file(file_path, relative_path, parent_id)

                if success:
                    if "跳过" in message:
                        stats['skipped'] += 1
                    else:
                        stats['success'] += 1
                else:
                    stats['failed'] += 1
                    stats['errors'].append(message)

        # 输出统计信息
        self.logger.info(f"上传完成! 总计: {stats['total']}, "
                        f"成功: {stats['success']}, "
                        f"跳过: {stats['skipped']}, "
                        f"失败: {stats['failed']}")

        if stats['errors']:
            self.logger.error("失败的文件:")
            for error in stats['errors']:
                self.logger.error(f"  - {error}")
        return stats

def create_sample_config():
    """创建示例配置文件"""
    config = {
        "base_url": "http://14.103.67.28:12345",
        "token": "your_access_token_here",
        "tenant_id": 21,
        "parent_resource_id": -1,
        "log_level": "INFO",
        "auth_type": "token_header"  # 可选: "token_header", "bearer", "query_param"
    }

    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print("示例配置文件已创建: config.json")
    print("请编辑配置文件，填入正确的DolphinScheduler连接信息")
    print("\n认证配置说明:")
    print("- token: DolphinScheduler 访问令牌")
    print("- tenant_id: 租户ID (从错误日志看应该是21)")
    print("- base_url: DolphinScheduler 服务地址 (不要包含 /dolphinscheduler)")
    print("- auth_type: 认证方式")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='DolphinScheduler 真实文件上传工具')
    parser.add_argument('directory', nargs='?', help='要上传的本地目录路径')
    parser.add_argument('-c', '--config', default='config.json',
                       help='传统JSON配置文件路径 (默认: config.json)')
    parser.add_argument('-w', '--workers', type=int, default=5,
                       help='最大并发上传数 (默认: 5)')
    parser.add_argument('--create-config', action='store_true',
                       help='创建示例配置文件')
    parser.add_argument('--test-connection', action='store_true',
                       help='测试连接功能')
    parser.add_argument('-p', '--parent', type=str,
                       help='指定父资源目录名称（如：package、utils等），上传到该目录下')
    parser.add_argument('--use-config-file', action='store_true',
                       help='使用传统JSON配置文件而不是config.py模块')

    args = parser.parse_args()

    if args.create_config:
        create_sample_config()
        return

    # 检查是否提供了目录参数
    if not args.directory:
        parser.error("必须提供要上传的目录路径，或使用 --test-connection 测试连接功能")

    try:
        # 创建上传器
        use_config_file = args.use_config_file
        if use_config_file:
            print(f"使用传统配置文件: {args.config}")
            uploader = DolphinSchedulerFileUploader(use_config_file=True, config_file=args.config)
        else:
            print("使用config.py模块配置")
            uploader = DolphinSchedulerFileUploader(use_config_file=False)


        # 设置工作线程数（优先使用命令行参数）
        if args.workers != 5:
            workers = args.workers
        elif hasattr(config, 'get_batch_config'):
            workers = config.get_batch_config()['max_concurrent_uploads']
        else:
            workers = 5

        print(f"使用并发数: {workers}")

        # 开始上传
        start_time = datetime.now()
        stats = uploader.upload_to_directory(args.directory, getattr(args, 'parent', None), workers)
        end_time = datetime.now()

        duration = (end_time - start_time).total_seconds()
        print(f"\n上传耗时: {duration:.2f} 秒")

        # 根据结果设置退出码
        if stats['failed'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()