#!/usr/bin/env python3
"""
DolphinScheduler 文件监听服务

基于 file_upload.py 实现的文件监听自动上传服务
监听指定目录的文件变化，自动上传新增/修改的文件到 DolphinScheduler

要求: Python 3.7+
依赖包: watchdog, requests, config (自定义模块)
"""

import os
import sys
import time
import logging
import hashlib
import threading
from pathlib import Path
from typing import Dict, List, Set, Optional
from datetime import datetime
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

# 第三方库
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent
    print("✅ watchdog 库加载成功")
except ImportError as e:
    print(f"❌ 错误: 缺少 watchdog 库，请安装: pip install watchdog")
    print(f"详细错误: {e}")
    sys.exit(1)

# 导入现有的文件上传器
try:
    from file_upload import DolphinSchedulerFileUploader
    print("✅ 成功导入文件上传模块: file_upload.py")
except ImportError as e:
    print(f"❌ 错误: 无法导入文件上传模块 file_upload.py: {e}")
    print("请确保 file_upload.py 文件存在且可访问")
    sys.exit(1)


class FileUploadHandler(FileSystemEventHandler):
    """文件系统事件处理器"""

    def __init__(self, listener_service):
        super().__init__()
        self.listener_service = listener_service
        self.logger = listener_service.logger

    def on_modified(self, event):
        """文件修改事件处理"""
        if not event.is_directory:
            self._handle_file_change(event.src_path, "修改")

    def on_created(self, event):
        """文件创建事件处理"""
        if not event.is_directory:
            self._handle_file_change(event.src_path, "创建")

    def _handle_file_change(self, file_path: str, event_type: str):
        """处理文件变化"""
        # 跳过临时文件和不需要上传的文件类型
        if self._should_skip_file(file_path):
            return

        # 添加小延迟避免文件写入未完成
        time.sleep(0.5)

        # 检查文件是否稳定（大小不再变化）
        if self._is_file_stable(file_path):
            self.logger.info(f"检测到文件{event_type}: {file_path}")
            self.listener_service.add_upload_task(file_path)
        else:
            self.logger.debug(f"文件尚未稳定，跳过: {file_path}")

    def _should_skip_file(self, file_path: str) -> bool:
        """判断是否应该跳过文件"""
        file_name = os.path.basename(file_path)
        file_dir = os.path.dirname(file_path)

        # 跳过临时文件
        temp_patterns = [
            '.tmp', '.temp', '.swp', '.lock', '.part',
            '~', '.bak', '.backup', '#', '.DS_Store'
        ]

        for pattern in temp_patterns:
            if file_name.endswith(pattern) or file_name.startswith(pattern):
                self.logger.debug(f"跳过临时文件: {file_path}")
                return True

        # 跳过系统隐藏文件
        if file_name.startswith('.') and file_name not in ['.env', '.gitignore']:
            self.logger.debug(f"跳过隐藏文件: {file_path}")
            return True

        # 跳过日志文件（避免无限循环）
        if file_name.endswith('.log'):
            self.logger.debug(f"跳过日志文件: {file_path}")
            return True

        # 跳过特定目录
        skip_dirs = ['__pycache__', '.git', '.svn', 'node_modules']
        for skip_dir in skip_dirs:
            if skip_dir in file_dir.split(os.sep):
                self.logger.debug(f"跳过目录中的文件: {file_path}")
                return True

        return False

    def _is_file_stable(self, file_path: str, max_attempts: int = 3) -> bool:
        """检查文件是否稳定（大小不再变化）"""
        try:
            for attempt in range(max_attempts):
                size1 = os.path.getsize(file_path)
                time.sleep(0.2)  # 短暂等待
                size2 = os.path.getsize(file_path)

                if size1 == size2:
                    return True

                self.logger.debug(f"文件大小不稳定，重试 {attempt + 1}/{max_attempts}: {file_path}")

            self.logger.warning(f"文件大小检查超过最大次数，假设不稳定: {file_path}")
            return False

        except (OSError, FileNotFoundError):
            self.logger.debug(f"文件访问失败，可能已被删除: {file_path}")
            return False


class FileUploadListener:
    """文件监听上传服务"""

    def __init__(self, watch_directory: str, use_config_file: bool = False,
                 config_file: str = "config.json", max_workers: int = 3):
        """初始化文件监听服务"""
        self.watch_directory = Path(watch_directory)
        self.max_workers = max_workers

        # 验证监听目录
        if not self.watch_directory.exists():
            raise FileNotFoundError(f"监听目录不存在: {watch_directory}")

        if not self.watch_directory.is_dir():
            raise ValueError(f"监听路径不是目录: {watch_directory}")

        # 设置日志
        self._setup_logging()

        # 初始化文件上传器
        try:
            self.uploader = DolphinSchedulerFileUploader(
                use_config_file=use_config_file,
                config_file=config_file
            )
            self.logger.info("文件上传器初始化成功")
        except Exception as e:
            self.logger.error(f"文件上传器初始化失败: {e}")
            raise

        # 上传任务队列
        self.upload_queue = Queue()
        self.processing_files: Set[str] = set()  # 正在处理的文件
        self.uploaded_files: Dict[str, str] = {}  # 已上传文件的MD5记录

        # 线程控制
        self.observer: Optional[Observer] = None
        self.upload_executor: Optional[ThreadPoolExecutor] = None
        self.running = False

        # 统计信息
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'failed_count': 0,
            'skipped_count': 0,
            'start_time': None
        }

        self.logger.info(f"文件监听服务初始化完成，监听目录: {watch_directory}")

    def _setup_logging(self):
        """设置日志配置"""
        log_file = f"file_listener_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # 清除现有的handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"日志系统初始化完成，日志文件: {log_file}")

    def _calculate_file_md5(self, file_path: str) -> str:
        """计算文件MD5值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_relative_path(self, file_path: str) -> str:
        """获取相对于监听目录的路径"""
        return str(Path(file_path).relative_to(self.watch_directory))

    def add_upload_task(self, file_path: str):
        """添加上传任务到队列"""
        # 检查是否已在处理中
        if file_path in self.processing_files:
            self.logger.debug(f"文件已在处理中，跳过: {file_path}")
            return

        # 检查文件是否与上次上传的内容相同
        try:
            current_md5 = self._calculate_file_md5(file_path)
            if file_path in self.uploaded_files and self.uploaded_files[file_path] == current_md5:
                self.logger.info(f"文件内容未变化，跳过上传: {file_path}")
                return

            # 添加到上传队列
            self.upload_queue.put(file_path)
            self.logger.info(f"添加上传任务: {file_path}")

        except Exception as e:
            self.logger.error(f"处理上传任务失败: {file_path}, 错误: {e}")

    def _upload_worker(self):
        """上传工作线程"""
        while self.running:
            try:
                # 从队列获取任务，设置超时避免永久阻塞
                file_path = self.upload_queue.get(timeout=1)

                # 标记为处理中
                self.processing_files.add(file_path)
                relative_path = self._get_relative_path(file_path)

                self.logger.info(f"开始处理上传任务: {relative_path}")
                self.stats['total_processed'] += 1

                try:
                    # 检查文件是否仍然存在
                    if not os.path.exists(file_path):
                        self.logger.warning(f"文件已被删除，跳过上传: {relative_path}")
                        self.stats['skipped_count'] += 1
                        continue

                    # 执行上传
                    success, message = self.uploader._upload_single_file(file_path, relative_path)

                    if success:
                        # 记录成功上传的文件MD5
                        current_md5 = self._calculate_file_md5(file_path)
                        self.uploaded_files[file_path] = current_md5
                        self.stats['success_count'] += 1
                        self.logger.info(f"上传成功: {relative_path}")
                    else:
                        self.stats['failed_count'] += 1
                        self.logger.error(f"上传失败: {relative_path}, 原因: {message}")

                except Exception as e:
                    self.stats['failed_count'] += 1
                    self.logger.error(f"上传异常: {relative_path}, 错误: {e}")

                finally:
                    # 清理处理状态
                    self.processing_files.discard(file_path)
                    self.upload_queue.task_done()

            except Empty:
                # 队列空，继续等待
                continue
            except Exception as e:
                self.logger.error(f"上传工作线程异常: {e}")

    def start(self):
        """启动监听服务"""
        try:
            self.logger.info("启动文件监听服务...")
            # 启动上传工作线程池
            self.upload_executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="UploadWorker")
            for i in range(self.max_workers):
                self.upload_executor.submit(self._upload_worker)

            self.logger.info(f"已启动 {self.max_workers} 个上传工作线程")

            # 启动文件监听器
            event_handler = FileUploadHandler(self)
            self.observer = Observer()
            self.observer.schedule(event_handler, str(self.watch_directory), recursive=True)

            self.running = True
            self.stats['start_time'] = datetime.now()

            self.observer.start()
            self.logger.info(f"文件监听服务已启动，监听目录: {self.watch_directory}")
            self.logger.info("按 Ctrl+C 停止服务")

            return True

        except Exception as e:
            self.logger.error(f"启动服务失败: {e}")
            return False

    def stop(self):
        """停止监听服务"""
        try:
            self.logger.info("正在停止文件监听服务...")

            # 停止监听
            self.running = False

            # 停止文件监听器
            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=5)
                self.logger.info("文件监听器已停止")

            # 停止上传线程池
            if self.upload_executor:
                self.upload_executor.shutdown(wait=True, timeout=10)
                self.logger.info("上传线程池已停止")

            # 输出统计信息
            self._print_stats()

            self.logger.info("文件监听服务已停止")

        except Exception as e:
            self.logger.error(f"停止服务时发生异常: {e}")

    def _print_stats(self):
        """打印统计信息"""
        if self.stats['start_time']:
            duration = (datetime.now() - self.stats['start_time']).total_seconds()
        else:
            duration = 0

        self.logger.info("=" * 50)
        self.logger.info("服务运行统计:")
        self.logger.info(f"  运行时间: {duration:.2f} 秒")
        self.logger.info(f"  总处理任务: {self.stats['total_processed']}")
        self.logger.info(f"  成功上传: {self.stats['success_count']}")
        self.logger.info(f"  跳过任务: {self.stats['skipped_count']}")
        self.logger.info(f"  失败任务: {self.stats['failed_count']}")
        self.logger.info(f"  当前队列: {self.upload_queue.qsize()}")
        self.logger.info(f"  正在处理: {len(self.processing_files)}")
        self.logger.info("=" * 50)

    def run(self):
        """运行监听服务（阻塞直到停止）"""
        if not self.start():
            return False

        try:
            # 保持主线程运行
            while self.running:
                time.sleep(1)

                # 定期输出统计信息（每60秒）
                if self.stats['start_time']:
                    elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
                    if int(elapsed) % 60 == 0:
                        self._print_stats()

        except KeyboardInterrupt:
            self.logger.info("收到停止信号...")
        finally:
            self.stop()

        return True


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='DolphinScheduler 文件监听上传服务')
    parser.add_argument('directory', help='要监听的目录路径')
    parser.add_argument('-c', '--config', default='config.json',
                       help='传统JSON配置文件路径 (默认: config.json)')
    parser.add_argument('-w', '--workers', type=int, default=3,
                       help='最大并发上传数 (默认: 3)')
    parser.add_argument('--use-config-file', action='store_true',
                       help='使用传统JSON配置文件而不是config.py模块')
    parser.add_argument('--test-connection', action='store_true',
                       help='仅测试连接，不启动监听')

    args = parser.parse_args()

    try:
        # 验证监听目录
        if not os.path.exists(args.directory):
            print(f"错误: 监听目录不存在: {args.directory}")
            return 1

        if not os.path.isdir(args.directory):
            print(f"错误: 监听路径不是目录: {args.directory}")
            return 1

        # 创建监听服务
        listener = FileUploadListener(
            watch_directory=args.directory,
            use_config_file=args.use_config_file,
            config_file=args.config,
            max_workers=args.workers
        )

        # 启动监听服务
        success = listener.run()
        return 0 if success else 1

    except KeyboardInterrupt:
        print("\n服务已停止")
        return 0
    except Exception as e:
        print(f"程序执行失败: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())