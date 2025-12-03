#!/usr/bin/env python3
"""
文件监听上传工具
监听 test_files 目录，当有新文件时自动激活虚拟环境并上传到 DolphinScheduler
"""

import os
import sys
import subprocess
import logging
import time
from pathlib import Path

# 添加当前目录到 Python 路径
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# 导入文件上传模块
try:
    from file_upload import DolphinSchedulerFileUploader
    print("✅ 成功导入文件上传模块")
except ImportError as e:
    print(f"错误: 无法导入文件上传模块: {e}")
    print("请确保 file_upload.py 在同一目录或 Python 路径中")
    sys.exit(1)
except ModuleNotFoundError as e:
    if "tqdm" in str(e):
        print("⚠️  缺少 tqdm 模块，但这不会影响上传功能")
        print("✅ 继续执行上传（无进度条显示）")

    # 创建简化版上传器
    from file_upload import DolphinSchedulerFileUploader as BaseUploader

    class SimpleUploader(BaseUploader):
        """简化版上传器，不依赖 tqdm"""

        def upload_to_directory(self, directory: str, parent_resource: str = None, max_workers: int = 5):
            """上传目录（简化版）"""
            print(f"🚀 开始上传目录: {directory}")

            # 收集文件
            files = []
            for file_path in Path(directory).rglob('*'):
                if file_path.is_file():
                    relative_path = str(file_path.relative_to(directory)).replace(os.sep, '/')
                    files.append((str(file_path), relative_path))

            print(f"📁 发现 {len(files)} 个文件")

            # 上传文件（不使用 tqdm）
            for file_path, relative_path in files:
                success, message = self._upload_single_file(file_path, relative_path)

                if success:
                    print(f"✅ {message}")
                else:
                    print(f"❌ {message}")


class FileMonitorUploader:
    """文件监听上传器"""

    def __init__(self, watch_dir: str = "test_files", venv_path: str = "dolphinscheduler-env"):
        """
        初始化文件监听上传器

        Args:
            watch_dir: 监听的目录路径
            venv_path: 虚拟环境路径
        """
        self.watch_dir = Path(watch_dir)
        self.venv_path = Path(venv_path)
        self.processed_files = set()
        self.running = False

        self._setup_logging()

    def _setup_logging(self):
        """设置日志配置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('file_monitor.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _check_virtual_env(self):
        """检查虚拟环境是否存在"""
        if not self.venv_path.exists():
            self.logger.error(f"虚拟环境不存在: {self.venv_path}")
            raise FileNotFoundError(f"虚拟环境不存在: {self.venv_path}")

        venv_python = self.venv_path / "bin" / "python"
        if not venv_python.exists():
            self.logger.error(f"虚拟环境Python不存在: {venv_python}")
            raise FileNotFoundError(f"虚拟环境Python不存在: {venv_python}")

        self.logger.info(f"虚拟环境检查通过: {self.venv_path}")

    def _init_uploader(self):
        """初始化上传器（需要在虚拟环境中）"""
        try:
            # 检查配置文件
            config_py = current_dir / "config.py"
            if not config_py.exists():
                self.logger.warning("配置文件 dolphinscheduler/config.py 不存在，请确保配置正确")

            # 尝试初始化上传器
            self.uploader = SimpleUploader()
            self.logger.info("文件上传器初始化成功")

        except Exception as e:
            self.logger.error(f"文件上传器初始化失败: {e}")
            raise

    def _activate_venv_and_upload(self, file_path: str, relative_path: str) -> bool:
        """激活虚拟环境并上传文件"""
        try:
            self.logger.info(f"开始上传文件: {relative_path}")

            # 在虚拟环境中执行上传
            venv_python = str(self.venv_path / "bin" / "python")

            # 创建上传脚本
            upload_script = f'''import sys
import os
sys.path.insert(0, "{str(current_dir)}")
try:
    from file_upload import DolphinSchedulerFileUploader
    uploader = DolphinSchedulerFileUploader(use_config_file=False)
    success, message = uploader._upload_single_file("{file_path}", "{relative_path}")
    if success:
        print(f"SUCCESS:True - {{message}}")
    else:
        print(f"SUCCESS:False - {{message}}")
except Exception as e:
    import traceback
    print(f"ERROR:{{str(e)}}")
    print("TRACEBACK:")
    traceback.print_exc()
    sys.exit(1)
'''

            # 写入临时脚本文件
            script_file = current_dir / "temp_upload.py"
            with open(script_file, 'w', encoding='utf-8') as f:
                f.write(upload_script)

            try:
                # 在虚拟环境中执行上传
                result = subprocess.run(
                    [venv_python, str(script_file)],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    cwd=str(current_dir)
                )

                # 输出完整的stdout和stderr用于调试
                if result.stdout:
                    self.logger.debug(f"脚本输出: {result.stdout}")
                if result.stderr:
                    self.logger.debug(f"脚本错误: {result.stderr}")

                if result.returncode == 0:
                    output = result.stdout.strip()
                    if "SUCCESS:True" in output:
                        # 提取消息部分
                        success_msg = output.split("SUCCESS:True - ", 1)[-1].split("\n")[0]
                        self.logger.info(f"上传成功: {success_msg}")
                        return True
                    elif "SUCCESS:False" in output:
                        fail_msg = output.split("SUCCESS:False - ", 1)[-1].split("\n")[0]
                        self.logger.warning(f"上传跳过: {fail_msg}")
                        return False
                    else:
                        self.logger.error(f"上传失败 - 输出: {output}")
                        return False
                else:
                    self.logger.error(f"上传脚本执行失败 (返回码: {result.returncode})")
                    self.logger.error(f"标准输出: {result.stdout}")
                    self.logger.error(f"标准错误: {result.stderr}")
                    return False

            finally:
                # 清理临时脚本
                if script_file.exists():
                    script_file.unlink()

        except subprocess.TimeoutExpired:
            self.logger.error(f"上传超时: {relative_path}")
            return False
        except Exception as e:
            self.logger.error(f"上传过程中发生异常: {relative_path}, 错误: {e}")
            import traceback
            self.logger.error(f"异常堆栈: {traceback.format_exc()}")
            return False

    def check_directory(self, directory):
        """检查目录是否存在"""
        if not Path(directory).exists():
            self.logger.error(f"❌ 目录不存在: {directory}")
            return False
        return True

    def _check_new_files(self):
        """检查是否有新文件"""
        new_files = []
        
        try:
            # 遍历监听目录
            for file_path in self.watch_dir.rglob('*'):
                if file_path.is_file():
                    # 获取文件的绝对路径字符串
                    abs_path = str(file_path.absolute())
                    
                    # 如果文件未被处理过
                    if abs_path not in self.processed_files:
                        # 计算相对路径
                        relative_path = str(file_path.relative_to(self.watch_dir)).replace(os.sep, '/')
                        new_files.append((abs_path, relative_path))
                        # 标记为已处理
                        self.processed_files.add(abs_path)
        
        except Exception as e:
            self.logger.error(f"检查新文件时出错: {e}")
        
        return new_files

    def start_monitoring(self, check_interval: int = 5):
        """开始监听文件变化"""
        self.logger.info(f"📁 监听目录: {self.watch_dir}")
        self.logger.info(f"🐍 虚拟环境: {self.venv_path}")
        self.logger.info(f"⏰ 检查间隔: {check_interval} 秒")
        self.running = True

        # 检查目录是否存在
        if not self.check_directory(self.watch_dir):
            self.logger.error("监听目录不存在")
            return

        # 开始监听
        try:
            while self.running:
                new_files = self._check_new_files()

                if new_files:
                    self.logger.info(f"发现 {len(new_files)} 个新文件")

                    for file_path, relative_path in new_files:
                        self.logger.info(f"📤 正在上传: {relative_path}")
                        success = self._activate_venv_and_upload(file_path, relative_path)

                        if success:
                            self.logger.info(f"✅ 文件上传成功: {relative_path}")
                        else:
                            self.logger.error(f"❌ 文件上传失败: {relative_path}")

                time.sleep(check_interval)

        except KeyboardInterrupt:
            self.logger.info("📋 停止监听...")
            self.running = False
        except Exception as e:
            self.logger.error(f"监听过程出错: {e}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='文件监听上传工具')
    parser.add_argument('--watch-dir', default='test_files', help='监听目录路径')
    parser.add_argument('--venv-path', default='dolphinscheduler-env', help='虚拟环境路径')
    parser.add_argument('--interval', type=int, default=5, help='检查间隔秒数')
    parser.add_argument('--test-upload', action='store_true', help='测试上传功能')

    args = parser.parse_args()

    try:
        monitor = FileMonitorUploader(
            watch_dir=args.watch_dir,
            venv_path=args.venv_path
        )

        if args.test_upload:
            success = monitor.start_monitoring(0)  # 测试模式：只运行一次检查
            if success:
                print("✅ 测试上传成功!")
            else:
                print("❌ 测试上传失败!")
        else:
            print(f"📁 监听目录: {Path(args.watch_dir).absolute()}")
            print(f"🐍 虚拟环境: {Path(args.venv_path).absolute()}")
            print(f"⏰ 检查间隔: {args.interval} 秒")
            print("按 Ctrl+C 停止监听")
            monitor.start_monitoring(args.interval)

    except KeyboardInterrupt:
        print("\\n📋 监听已停止")
    except Exception as e:
        print(f"❌ 程序执行失败: {e}")


if __name__ == '__main__':
    sys.exit(main())