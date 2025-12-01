#!/usr/bin/env python3
"""
DolphinScheduler 文件上传工具

功能:
1. 遍历本地指定目录下的所有文件
2. 检查文件是否在DolphinScheduler中已存在
3. 上传新文件到DolphinScheduler
4. 提供详细的日志和错误处理

要求: Python 3.7+
依赖包: requests, tqdm
"""

import os
import sys
import json
import hashlib
import logging
import base64
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


class DolphinSchedulerUploader:
    """DolphinScheduler 文件上传器"""

    def __init__(self, use_config_file: bool = False, config_file: str = "config.json"):
        """
        初始化上传器

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

    def _setup_authentication(self):
        """
        设置认证信息 - 统一使用Token Header方式
        """
        # 清除其他认证方式的header
        self.session.headers.pop('Authorization', None)
        
        # 统一使用token header认证
        if self.config.get('token'):
            self.session.headers.update({
                'token': self.config['token'],
                'Content-Type': 'application/json'
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
                logging.FileHandler('uploader.log', encoding='utf-8'),
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
            # 使用配置的URL和上传路径
            if hasattr(self, 'upload_path'):
                url = f"{self.config['base_url']}{self.upload_path.replace('/online-create', '')}"
            else:
                url = f"{self.config['base_url']}/resources"

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
        上传单个文件

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

            # 1. 读取文件内容并进行Base64编码
            self.logger.info(f"正在读取文件: {file_path}")

            with open(file_path, "rb") as f:
                file_content = f.read()

            # Base64编码
            encoded_content = base64.b64encode(file_content).decode("utf-8")
            self.logger.debug(f"文件已编码，长度: {len(encoded_content)} 字符")

            # 2. 准备请求参数
            # 从文件名中提取后缀，并处理DolphinScheduler API要求
            suffix = os.path.splitext(file_name)[1].lstrip('.')

            # DolphinScheduler API特殊处理：常见文件类型映射
            suffix_mapping = {
                'jar': 'JAR',
                'zip': 'ZIP',
                'tar': 'TAR',
                'gz': 'GZ',
                'py': 'PY',
                'sql': 'SQL',
                'json': 'JSON',
                'xml': 'XML',
                'properties': 'PROPERTIES',
                'yml': 'YAML',
                'yaml': 'YAML',
                'sh': 'SH',
                'bat': 'BAT',
                'md': 'MD',
                'txt': 'TXT'
            }

            # 使用映射后的后缀，如果没有映射则使用大写
            mapped_suffix = suffix_mapping.get(suffix.lower(), suffix.upper()) if suffix else ''

            self.logger.debug(f"原始后缀: '{suffix}', 映射后缀: '{mapped_suffix}'")

            # 使用配置的上传路径
            if hasattr(self, 'upload_path'):
                upload_url = f"{self.config['base_url']}{self.upload_path}"
            else:
                upload_url = f"{self.config['base_url']}/resources/online-create"

            # 构造查询参数
            params = {
                "content": encoded_content,
                "currentDir": getattr(self, 'current_dir', '/'),
                "description": f"Uploaded via API - {relative_path}",
                "fileName": file_name,
                "pid": parent_id,
                "suffix": suffix,
                "type": getattr(self, 'resource_type', 'FILE')
            }

            # 统一使用token header认证
            headers = {
                'token': self.config['token']
            }

            self.logger.info(f"正在上传文件 {file_name}...")
            self.logger.debug(f"目标URL: {upload_url}")
            self.logger.debug(f"参数 (不含content): { {k: v for k, v in params.items() if k != 'content'} }")

            # 3. 发送POST请求
            # 使用multipart/form-data格式提交数据
            timeout = getattr(self, 'timeout', 300)

            # 使用files参数提交multipart/form-data
            response = requests.post(
                upload_url,
                data=params,  # 将参数作为表单数据
                headers=headers,
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
                    self.logger.error(f"请求参数: {params}")
                    return False, f"上传失败: {relative_path}, 错误: {error_msg}"
            elif response.status_code == 401:
                # 专门处理401错误
                self.logger.error(f"认证失败 (401): {relative_path}")
                self.logger.error(f"Token: {self.config['token'][:20]}...")
                self.logger.debug(f"请求数据: {params}")
                self.logger.debug(f"响应内容: {response.text}")
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

    def query_resources(self, resource_id: int = -1, page_no: int = 1, page_size: int = 20,
                      resource_type: str = "FILE", search_val: str = None) -> Dict:
        """
        查询DolphinScheduler资源列表

        Args:
            resource_id: 父资源ID，默认-1为根目录
            page_no: 页码，默认1
            page_size: 每页大小，默认20
            resource_type: 资源类型，默认FILE
            search_val: 搜索关键词，可选

        Returns:
            Dict: API响应结果
        """
        try:
            url = f"{self.config['base_url']}/resources"
            params = {
                'id': resource_id,
                'pageNo': page_no,
                'pageSize': page_size,
                'type': resource_type
            }

            # 添加搜索参数
            if search_val:
                params['searchVal'] = search_val

            # 统一使用token header认证
            headers = {
                'token': self.config['token'],
                'Content-Type': 'application/json'
            }

            self.logger.info(f"查询资源列表: {url}")
            self.logger.debug(f"查询参数: {params}")
            self.logger.debug(f"请求头: {headers}")

            # 使用配置的超时时间
            timeout = getattr(self, 'timeout', 30)

            # 使用session对象确保认证一致性
            response = self.session.get(url, params=params, headers=headers, timeout=timeout, verify=getattr(self, 'verify_ssl', True))
            response.raise_for_status()

            data = response.json()

            if data.get('code') == 0:
                total = data.get('data', {}).get('total', 0)
                resources = data.get('data', {}).get('totalList', [])
                self.logger.info(f"查询成功: 找到 {total} 个资源")
                return data
            else:
                error_msg = data.get('msg', '未知错误')
                self.logger.error(f"查询资源失败: {error_msg}")
                return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"查询资源网络错误: {e}")
            return {'code': -1, 'msg': f'网络错误: {str(e)}', 'data': None}
        except Exception as e:
            self.logger.error(f"查询资源异常: {e}")
            return {'code': -1, 'msg': f'异常: {str(e)}', 'data': None}

    def list_root_resources(self, page_size: int = 20) -> List[Dict]:
        """
        查询根目录下的所有资源

        Args:
            page_size: 每页大小，默认20

        Returns:
            List[Dict]: 资源列表
        """
        result = self.query_resources(resource_id=-1, page_size=page_size, resource_type="FILE")

        if result.get('code') == 0:
            return result.get('data', {}).get('totalList', [])
        else:
            self.logger.error(f"获取根目录资源失败: {result.get('msg')}")
            return []

    def search_resources(self, keyword: str, resource_id: int = -1, page_size: int = 20) -> List[Dict]:
        """
        搜索资源

        Args:
            keyword: 搜索关键词
            resource_id: 搜索范围（父资源ID），默认-1为根目录
            page_size: 每页大小，默认20

        Returns:
            List[Dict]: 匹配的资源列表
        """
        result = self.query_resources(
            resource_id=resource_id,
            search_val=keyword,
            page_size=page_size,
            resource_type="FILE"
        )

        if result.get('code') == 0:
            resources = result.get('data', {}).get('totalList', [])
            self.logger.info(f"搜索 '{keyword}' 找到 {len(resources)} 个结果")
            return resources
        else:
            self.logger.error(f"搜索资源失败: {result.get('msg')}")
            return []

    def get_resource_info(self, resource_name: str, search_scope: int = -1) -> Optional[Dict]:
        """
        根据资源名称获取资源信息

        Args:
            resource_name: 资源名称
            search_scope: 搜索范围（父资源ID），默认-1为根目录

        Returns:
            Optional[Dict]: 资源信息，如果未找到返回None
        """
        resources = self.search_resources(resource_name, search_scope, page_size=50)

        for resource in resources:
            if resource.get('alias') == resource_name:
                self.logger.info(f"找到资源: {resource_name} (ID: {resource.get('id')})")
                return resource

        self.logger.warning(f"未找到资源: {resource_name}")
        return None

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

    def find_parent_resource(self, parent_name: str, search_scope: int = -1) -> Optional[int]:
        """
        查找父资源ID

        Args:
            parent_name: 父资源名称或路径
            search_scope: 搜索范围，默认-1为根目录

        Returns:
            Optional[int]: 父资源ID，如果未找到返回None
        """
        # 首先尝试精确匹配
        parent_resource = self.get_resource_info(parent_name, search_scope)
        if parent_resource:
            self.logger.info(f"找到父资源: {parent_name} (ID: {parent_resource['id']})")
            return parent_resource['id']

        # 如果精确匹配失败，尝试模糊搜索
        resources = self.search_resources(parent_name, search_scope, page_size=50)
        if resources:
            # 选择最匹配的资源
            best_match = resources[0]  # 取第一个匹配结果
            self.logger.info(f"模糊匹配找到父资源: {best_match['alias']} (ID: {best_match['id']})")
            return best_match['id']

        self.logger.error(f"未找到父资源: {parent_name}")
        return None

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
            parent_id = self.find_parent_resource(parent_resource)
            if parent_id is None:
                self.logger.error(f"无法找到父资源: {parent_resource}")
                return {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'errors': [f'父资源不存在: {parent_resource}']}

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
        "tenant_id": 4,
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
    print("- tenant_id: 租户ID (从错误日志看应该是4)")
    print("- base_url: DolphinScheduler 服务地址 (不要包含 /dolphinscheduler)")
    print("- auth_type: 认证方式")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='DolphinScheduler 文件上传工具')
    parser.add_argument('directory', nargs='?', help='要上传的本地目录路径')
    parser.add_argument('-c', '--config', default='config.json',
                       help='传统JSON配置文件路径 (默认: config.json)')
    parser.add_argument('-w', '--workers', type=int, default=5,
                       help='最大并发上传数 (默认: 5)')
    parser.add_argument('--create-config', action='store_true',
                       help='创建示例配置文件')
    parser.add_argument('--test-query', action='store_true',
                       help='测试资源查询功能')
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
        parser.error("必须提供要上传的目录路径，或使用 --test-query 测试查询功能")

    try:
        # 创建上传器
        use_config_file = args.use_config_file
        if use_config_file:
            print(f"使用传统配置文件: {args.config}")
            uploader = DolphinSchedulerUploader(use_config_file=True, config_file=args.config)
        else:
            print("使用config.py模块配置")
            uploader = DolphinSchedulerUploader(use_config_file=False)

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