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
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import argparse

import requests
from tqdm import tqdm


class DolphinSchedulerUploader:
    """DolphinScheduler 文件上传器"""

    def __init__(self, config_file: str = "config.json"):
        """
        初始化上传器

        Args:
            config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.session = requests.Session()

        # 设置请求头
        self.session.headers.update({
            'Authorization': f"Bearer {self.config['token']}",
            'Content-Type': 'application/json'
        })

        # 设置日志
        self._setup_logging()

        # 文件存在检查缓存
        self.existing_files_cache = set()

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
        """设置日志"""
        log_level = self.config.get('log_level', 'INFO')
        log_format = '%(asctime)s - %(levelname)s - %(message)s'

        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=[
                logging.FileHandler('uploader.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        self.logger = logging.getLogger(__name__)

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
            url = f"{self.config['base_url']}/dolphinscheduler/resources/list"
            params = {
                'tenantId': self.config['tenant_id'],
                'searchVal': file_name,
                'page': 1,
                'pageSize': 10
            }

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
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

    def _upload_single_file(self, file_path: str, relative_path: str) -> Tuple[bool, str]:
        """
        上传单个文件

        Args:
            file_path: 文件完整路径
            relative_path: 相对路径

        Returns:
            Tuple[bool, str]: (是否成功, 消息)
        """
        try:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_md5 = self._get_file_md5(file_path)

            # 检查文件是否已存在
            if self._check_file_exists(file_name, file_size, file_md5):
                return True, f"文件已存在，跳过: {relative_path}"

            # 执行上传
            upload_url = f"{self.config['base_url']}/dolphinscheduler/resources/online"

            with open(file_path, 'rb') as f:
                files = {'file': (file_name, f, 'application/octet-stream')}
                data = {
                    'type': 'FILE',
                    'name': relative_path,
                    'pid': self.config.get('parent_resource_id', -1)
                }

                response = self.session.post(
                    upload_url,
                    files=files,
                    data=data,
                    timeout=300  # 5分钟超时
                )

            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    # 添加到缓存
                    self.existing_files_cache.add(f"{file_name}_{file_size}_{file_md5}")
                    self.logger.info(f"上传成功: {relative_path}")
                    return True, f"上传成功: {relative_path}"
                else:
                    error_msg = result.get('msg', '未知错误')
                    self.logger.error(f"上传失败: {relative_path}, 错误: {error_msg}")
                    return False, f"上传失败: {relative_path}, 错误: {error_msg}"
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                self.logger.error(f"上传失败: {relative_path}, {error_msg}")
                return False, f"上传失败: {relative_path}, {error_msg}"

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

    def upload_directory(self, directory: str, max_workers: int = 5) -> Dict:
        """
        上传整个目录

        Args:
            directory: 本地目录路径
            max_workers: 最大并发数

        Returns:
            Dict: 上传结果统计
        """
        self.logger.info(f"开始上传目录: {directory}")

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

                success, message = self._upload_single_file(file_path, relative_path)

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
        "base_url": "http://localhost:12345/dolphinscheduler",
        "token": "your_access_token_here",
        "tenant_id": 1,
        "parent_resource_id": -1,
        "log_level": "INFO"
    }

    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print("示例配置文件已创建: config.json")
    print("请编辑配置文件，填入正确的DolphinScheduler连接信息")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='DolphinScheduler 文件上传工具')
    parser.add_argument('directory', help='要上传的本地目录路径')
    parser.add_argument('-c', '--config', default='config.json',
                       help='配置文件路径 (默认: config.json)')
    parser.add_argument('-w', '--workers', type=int, default=5,
                       help='最大并发上传数 (默认: 5)')
    parser.add_argument('--create-config', action='store_true',
                       help='创建示例配置文件')

    args = parser.parse_args()

    if args.create_config:
        create_sample_config()
        return

    try:
        # 创建上传器
        uploader = DolphinSchedulerUploader(args.config)

        # 开始上传
        start_time = datetime.now()
        stats = uploader.upload_directory(args.directory, args.workers)
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