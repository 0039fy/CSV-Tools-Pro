import sys
import os
import time
import math
import mmap
import chardet
import warnings
import subprocess
import re
import tempfile
import shutil

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QPushButton, QLineEdit, QCheckBox, QFileDialog,
    QMessageBox, QTreeWidget, QTreeWidgetItem, QTextEdit, QProgressBar,
    QFrame, QSizePolicy, QSpacerItem, QGroupBox, QComboBox,
    QSpinBox, QDoubleSpinBox, QMenu, QProgressDialog, QInputDialog,
    QDialog, QStyle
)
from PySide6.QtCore import Qt, Signal, QThread, QTimer, QSize, QEvent, QUrl
from PySide6.QtGui import QIcon, QFont, QPalette, QColor, QBrush, QLinearGradient, QGradient, QAction, QDesktopServices

warnings.filterwarnings('ignore')

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class WorkerThread(QThread):
    progress = Signal(str)
    completed = Signal(tuple)
    error = Signal(str)
    progress_detail = Signal(dict)
    progress_percentage = Signal(int)

    def __init__(self, task_type, *args, **kwargs):
        super().__init__()
        self.task_type = task_type
        self.args = args
        self.kwargs = kwargs
        self.cancelled = False
        self.mm = None
        self.temp_dir = None

    def run(self):
        try:
            if self.task_type == 'merge':
                result = self.merge_files()
            elif self.task_type == 'split':
                result = self.split_files()
            elif self.task_type == 'extract':
                result = self.extract_data()
            elif self.task_type == 'extract_info':
                result = self.extract_info()
            elif self.task_type == 'get_column_values':
                result = self.get_column_values()

            if not self.cancelled:
                self.completed.emit(result)
        except Exception as e:
            self.error.emit(str(e))
        finally:
            if self.mm:
                try:
                    self.mm.close()
                except:
                    pass
                self.mm = None
            if self.temp_dir:
                try:
                    shutil.rmtree(self.temp_dir, ignore_errors=True)
                except:
                    pass
                self.temp_dir = None

    def merge_files(self):
        files, output_path, keep_header = self.args
        start_time = time.time()
        for file_path in files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")

        total_size = sum(os.path.getsize(f) for f in files)
        processed_size = 0

        with open(output_path, 'wb', buffering=1024 * 1024) as outfile:
            for i, file_path in enumerate(files):
                if self.cancelled: break
                file_size = os.path.getsize(file_path)
                self.progress.emit(
                    f"处理文件 {i + 1}/{len(files)}: {os.path.basename(file_path)} ({self.format_file_size(file_size)})")

                with open(file_path, 'rb') as f:
                    if i == 0:
                        if keep_header:
                            while True:
                                chunk = f.read(1024 * 1024)
                                if not chunk: break
                                outfile.write(chunk)
                                processed_size += len(chunk)
                        else:
                            f.readline()
                            while True:
                                chunk = f.read(1024 * 1024)
                                if not chunk: break
                                outfile.write(chunk)
                                processed_size += len(chunk)
                    else:
                        f.readline()
                        while True:
                            chunk = f.read(1024 * 1024)
                            if not chunk: break
                            outfile.write(chunk)
                            processed_size += len(chunk)

                if total_size > 0:
                    percent = int((processed_size / total_size) * 100)
                    self.progress_percentage.emit(percent)

        elapsed_time = time.time() - start_time
        output_size = os.path.getsize(output_path)
        return elapsed_time, output_size, output_path

    def split_files(self):
        file_path, output_dir, split_method, param_value, prefix = self.args
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        encoding = self.detect_encoding(file_path)
        self.progress.emit(f"检测到编码: {encoding}")
        file_size = os.path.getsize(file_path)
        self.progress.emit(f"文件大小: {self.format_file_size(file_size)}")

        if split_method == "按行数分割":
            rows_per_file = param_value
            self.progress.emit(f"按行数分割，每份: {rows_per_file:,} 行")
            return self.split_by_lines_fast(file_path, output_dir, rows_per_file, prefix, file_size, encoding)
        elif split_method == "按文件数分割":
            num_files = param_value
            self.progress.emit(f"按文件数分割，分成: {num_files} 个文件")
            return self.split_by_file_count_fast(file_path, output_dir, num_files, prefix, file_size, encoding)
        elif split_method == "按大小分割":
            target_size = param_value * 1024 * 1024
            self.progress.emit(f"按大小分割，每份: {param_value} MB")
            return self.split_by_size_fast(file_path, output_dir, target_size, prefix, file_size, encoding)
        return (0, 0, 0, [])

    def split_by_lines_fast(self, file_path, output_dir, rows_per_file, prefix, file_size, encoding):
        start_time = time.time()
        try:
            with open(file_path, 'rb') as f:
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                header_end = self.mm.find(b'\n', 0, min(1024 * 1024, len(self.mm)))
                if header_end == -1:
                    header_end = len(self.mm)
                    header_data = self.mm[:]
                    data_start = len(self.mm)
                else:
                    header_data = self.mm[:header_end + 1]
                    data_start = header_end + 1

                self.progress.emit("正在统计总行数...")
                total_lines = 0
                chunk_size = 1024 * 1024 * 10
                for i in range(0, len(self.mm), chunk_size):
                    if self.cancelled: break
                    chunk_end = min(i + chunk_size, len(self.mm))
                    chunk = self.mm[i:chunk_end]
                    total_lines += chunk.count(b'\n')
                    if i % (chunk_size * 2) == 0:
                        percent = int((i / len(self.mm)) * 50)
                        self.progress_percentage.emit(percent)

                if len(self.mm) > 0 and self.mm[-1] != b'\n'[0]:
                    total_lines += 1
                data_lines = total_lines - 1 if header_end < len(self.mm) else total_lines
                self.progress.emit(f"文件总行数: {total_lines:,} (数据行: {data_lines:,})")

                if data_lines <= rows_per_file:
                    self.progress.emit("文件行数不足，无需分割")
                    output_file = os.path.join(output_dir, f"{self.clean_filename(prefix)}_1.csv")
                    with open(output_file, 'wb') as outfile:
                        outfile.write(self.mm[:])
                    elapsed_time = time.time() - start_time
                    return elapsed_time, 1, total_lines, [output_file]
                else:
                    num_files = math.ceil(data_lines / rows_per_file)
                    self.progress.emit(f"将分割为 {num_files} 个文件")
                    return self.do_split_fast(self.mm, header_data, data_start, data_lines, rows_per_file, num_files,
                                              output_dir, prefix, start_time, encoding)
        finally:
            if self.mm:
                try:
                    self.mm.close()
                except:
                    pass
                self.mm = None

    def split_by_file_count_fast(self, file_path, output_dir, num_files, prefix, file_size, encoding):
        start_time = time.time()
        try:
            with open(file_path, 'rb') as f:
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                header_end = self.mm.find(b'\n', 0, min(1024 * 1024, len(self.mm)))
                if header_end == -1:
                    header_end = len(self.mm)
                    header_data = self.mm[:]
                    data_start = len(self.mm)
                else:
                    header_data = self.mm[:header_end + 1]
                    data_start = header_end + 1

                self.progress.emit("正在统计总行数...")
                total_lines = 0
                chunk_size = 1024 * 1024 * 10
                for i in range(0, len(self.mm), chunk_size):
                    if self.cancelled: break
                    chunk_end = min(i + chunk_size, len(self.mm))
                    chunk = self.mm[i:chunk_end]
                    total_lines += chunk.count(b'\n')
                    if i % (chunk_size * 2) == 0:
                        percent = int((i / len(self.mm)) * 50)
                        self.progress_percentage.emit(percent)

                if len(self.mm) > 0 and self.mm[-1] != b'\n'[0]:
                    total_lines += 1
                data_lines = total_lines - 1 if header_end < len(self.mm) else total_lines
                self.progress.emit(f"文件总行数: {total_lines:,} (数据行: {data_lines:,})")

                rows_per_file = math.ceil(data_lines / num_files)
                self.progress.emit(f"每份约 {rows_per_file:,} 行")

                return self.do_split_fast(self.mm, header_data, data_start, data_lines, rows_per_file, num_files,
                                          output_dir, prefix, start_time, encoding)
        finally:
            if self.mm:
                try:
                    self.mm.close()
                except:
                    pass
                self.mm = None

    def split_by_size_fast(self, file_path, output_dir, target_size, prefix, file_size, encoding):
        start_time = time.time()
        try:
            with open(file_path, 'rb') as f:
                self.mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                header_end = self.mm.find(b'\n', 0, min(1024 * 1024, len(self.mm)))
                if header_end == -1:
                    header_end = len(self.mm)
                    header_data = self.mm[:]
                    data_start = len(self.mm)
                else:
                    header_data = self.mm[:header_end + 1]
                    data_start = header_end + 1

                output_files = []
                safe_prefix = self.clean_filename(prefix)
                current_pos = data_start
                file_index = 1
                total_data = len(self.mm) - data_start

                while current_pos < len(self.mm) and not self.cancelled:
                    if current_pos + target_size >= len(self.mm):
                        file_end = len(self.mm)
                    else:
                        search_start = max(current_pos, current_pos + target_size - 1024)
                        search_end = min(len(self.mm), current_pos + target_size + 1024 * 1024)
                        if search_start >= search_end:
                            file_end = len(self.mm)
                        else:
                            file_end = self.mm.find(b'\n', search_start, search_end)
                            if file_end == -1:
                                file_end = len(self.mm)
                            else:
                                file_end += 1

                    if file_end <= current_pos:
                        file_end = min(len(self.mm), current_pos + 1024)

                    output_file = os.path.join(output_dir, f"{safe_prefix}_{file_index:04d}.csv")
                    output_files.append(output_file)

                    if file_index == 1 or file_index % 10 == 0:
                        progress_info = {
                            'current_file': file_index,
                            'total_files': math.ceil(total_data / target_size),
                            'filename': os.path.basename(output_file),
                            'progress': f"({file_index}/{math.ceil(total_data / target_size)})",
                            'status': 'generating'
                        }
                        self.progress_detail.emit(progress_info)

                    with open(output_file, 'wb') as outfile:
                        if header_data:
                            outfile.write(header_data)
                        if file_end > current_pos:
                            outfile.write(self.mm[current_pos:file_end])

                    current_pos = file_end
                    file_index += 1

                    if total_data > 0 and (file_index % 10 == 0 or file_index >= math.ceil(total_data / target_size)):
                        percent = 50 + int(((current_pos - data_start) / total_data) * 50)
                        self.progress_percentage.emit(percent)

                if self.cancelled:
                    return (0, 0, 0, [])

                elapsed_time = time.time() - start_time

                # 修复：统计总行数
                total_lines = 0
                if self.mm:
                    # 使用循环统计行数，而不是count方法
                    chunk_size = 1024 * 1024 * 10
                    for i in range(0, len(self.mm), chunk_size):
                        if self.cancelled: break
                        chunk_end = min(i + chunk_size, len(self.mm))
                        chunk = self.mm[i:chunk_end]
                        total_lines += chunk.count(b'\n')

                    # 检查最后一行是否以换行符结尾
                    if len(self.mm) > 0 and self.mm[-1] != b'\n'[0]:
                        total_lines += 1

                return elapsed_time, len(output_files), total_lines, output_files
        finally:
            if self.mm:
                try:
                    self.mm.close()
                except:
                    pass
                self.mm = None

    def do_split_fast(self, mm, header_data, data_start, data_lines, rows_per_file, num_files, output_dir, prefix,
                      start_time, encoding):
        output_files = []
        safe_prefix = self.clean_filename(prefix)
        current_line = 0
        current_file_start = data_start

        for file_index in range(1, num_files + 1):
            if self.cancelled: break

            lines_in_this_file = min(rows_per_file, data_lines - current_line)
            lines_found = 0
            search_pos = current_file_start
            file_end = current_file_start

            while lines_found < lines_in_this_file and search_pos < len(mm):
                next_newline = mm.find(b'\n', search_pos)
                if next_newline == -1:
                    file_end = len(mm)
                    lines_found += 1
                    break
                file_end = next_newline + 1
                lines_found += 1
                if lines_found == lines_in_this_file:
                    break
                search_pos = next_newline + 1

            output_file = os.path.join(output_dir, f"{safe_prefix}_{file_index:04d}.csv")
            output_files.append(output_file)

            if file_index == 1 or file_index % 3 == 0 or file_index == num_files:
                progress_info = {
                    'current_file': file_index,
                    'total_files': num_files,
                    'filename': os.path.basename(output_file),
                    'progress': f"({file_index}/{num_files})",
                    'status': 'generating'
                }
                self.progress_detail.emit(progress_info)

            with open(output_file, 'wb') as outfile:
                if header_data:
                    outfile.write(header_data)
                if file_end > current_file_start:
                    outfile.write(mm[current_file_start:file_end])

            current_line += lines_found
            current_file_start = file_end

            if file_index % 5 == 0 or file_index == num_files:
                total_progress = 50 + int((file_index / num_files) * 50)
                self.progress_percentage.emit(total_progress)

        elapsed_time = time.time() - start_time
        total_lines = (1 if data_start < len(mm) else 0) + data_lines
        return elapsed_time, len(output_files), total_lines, output_files

    def extract_data(self):
        csv_path, clean_column, value, output_path, column_mapping, column_types = self.args
        start_time = time.time()
        try:
            self.temp_dir = tempfile.mkdtemp()
            conn = duckdb.connect()
            conn.execute("SET memory_limit='8GB'")
            conn.execute("SET threads=8")
            conn.execute("SET preserve_insertion_order=false")
            conn.execute("SET enable_progress_bar=false")

            original_column = column_mapping.get(clean_column, clean_column) if column_mapping else clean_column
            quoted_column = self._escape_column_name(original_column)
            col_type = column_types.get(clean_column, "VARCHAR")
            self.progress.emit(f"开始提取: {clean_column} = '{value}' (类型: {col_type})")
            self.progress_percentage.emit(10)
            clean_search_value = self._clean_value(value)

            if clean_search_value == '':
                if 'INT' in col_type or 'FLOAT' in col_type or 'DOUBLE' in col_type or 'NUMERIC' in col_type or 'BIGINT' in col_type:
                    query = f"SELECT * FROM '{csv_path}' WHERE {quoted_column} IS NULL OR {quoted_column} = 0 OR {quoted_column} = 0.0"
                else:
                    query = f"SELECT * FROM '{csv_path}' WHERE {quoted_column} IS NULL OR TRIM(REPLACE(REPLACE({quoted_column}, '\t', ''), '\n', '')) = ''"
            else:
                try:
                    num_value = float(clean_search_value)
                    if 'FLOAT' in col_type or 'DOUBLE' in col_type:
                        query = f"SELECT * FROM '{csv_path}' WHERE ABS(TRY_CAST({quoted_column} AS FLOAT) - {num_value}) < 0.000001"
                    else:
                        query = f"SELECT * FROM '{csv_path}' WHERE TRY_CAST({quoted_column} AS FLOAT) = {num_value}"
                except:
                    escaped_value = clean_search_value.replace("'", "''")
                    if 'INT' in col_type or 'FLOAT' in col_type or 'DOUBLE' in col_type or 'NUMERIC' in col_type or 'BIGINT' in col_type:
                        query = f"SELECT * FROM '{csv_path}' WHERE TRY_CAST({quoted_column} AS VARCHAR) = '{escaped_value}'"
                    else:
                        query = f"SELECT * FROM '{csv_path}' WHERE TRIM(REPLACE(REPLACE({quoted_column}, '\t', ''), '\n', '')) = '{escaped_value}'"

            self.progress.emit(f"执行查询: {query}")
            self.progress_percentage.emit(30)

            count_result = conn.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()
            total_rows = count_result[0] if count_result else 0

            if total_rows == 0:
                conn.close()
                self.progress_percentage.emit(100)
                return (0, 0, 0, "", "")

            self.progress.emit(f"匹配到 {total_rows:,} 行数据")
            self.progress_percentage.emit(50)

            if output_path:
                temp_output = os.path.join(self.temp_dir, "temp_export.csv")
                self.progress.emit("正在导出数据...")
                self.progress_percentage.emit(70)
                conn.execute(f"COPY ({query}) TO '{temp_output}' WITH (HEADER, DELIMITER ',')")
                self.progress.emit("正在处理输出文件...")
                self.progress_percentage.emit(85)
                with open(temp_output, 'r', encoding='utf-8') as f_in, \
                        open(output_path, 'w', encoding='utf-8-sig', newline='') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                with open(output_path, 'r', encoding='utf-8-sig') as f:
                    next(f)
                    total_rows = sum(1 for _ in f)

                if os.path.exists(temp_output):
                    os.remove(temp_output)
            else:
                result_df = conn.execute(query).df()
                total_rows = len(result_df)
                output_path = None

            query_time = time.time() - start_time
            conn.close()
            self.progress_percentage.emit(100)
            return (total_rows, query_time, output_path, query, clean_search_value)
        except Exception as e:
            raise Exception(f"提取失败: {str(e)}")

    def extract_info(self):
        csv_path = self.args[0]
        try:
            conn = duckdb.connect()
            conn.execute("SET memory_limit='8GB'")
            conn.execute("SET threads=8")
            conn.execute("SET preserve_insertion_order=false")
            conn.execute("SET enable_progress_bar=false")

            try:
                result = conn.execute(f"SELECT COUNT(*) as total_rows FROM '{csv_path}'").fetchone()
                total_rows = result[0] if result else 0
            except:
                total_rows = 0

            conn.execute(f"CREATE TEMP TABLE temp_csv AS SELECT * FROM '{csv_path}' LIMIT 1000")
            type_result = conn.execute("PRAGMA table_info(temp_csv)").fetchall()
            column_types_raw = {row[1]: row[2] for row in type_result}

            result = conn.execute(f"SELECT * FROM '{csv_path}' LIMIT 100")
            original_columns = [desc[0] for desc in result.description]

            clean_columns = []
            column_mapping = {}
            column_types = {}

            for col in original_columns:
                if col is None:
                    clean_col = ""
                else:
                    clean_col = re.sub(r'["\']', '', col)
                    clean_col = re.sub(r'\s+', '', clean_col)
                    if clean_col == "":
                        clean_col = f"列_{len(clean_columns) + 1}"

                clean_columns.append(clean_col)
                column_mapping[clean_col] = col
                column_types[clean_col] = column_types_raw.get(col, "VARCHAR")

            file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            conn.close()

            return {
                'path': csv_path,
                'total_rows': total_rows,
                'columns': clean_columns,
                'original_columns': original_columns,
                'column_mapping': column_mapping,
                'column_types': column_types,
                'file_size': file_size_mb
            }
        except Exception as e:
            raise Exception(f"读取CSV文件失败: {str(e)}")

    def get_column_values(self):
        csv_path, column, limit, column_mapping = self.args
        try:
            conn = duckdb.connect()
            conn.execute("SET memory_limit='4GB'")
            conn.execute("SET threads=4")

            original_column = column_mapping.get(column, column) if column_mapping else column
            quoted_column = self._escape_column_name(original_column)
            self.progress.emit(f"正在获取 '{column}' 的示例值...")

            total_rows = conn.execute(f"SELECT COUNT(*) FROM '{csv_path}'").fetchone()[0]
            sample_size = min(10000, total_rows)

            base_query = f"SELECT {quoted_column} FROM '{csv_path}' WHERE {quoted_column} IS NOT NULL"

            if total_rows > sample_size:
                query = f"SELECT DISTINCT {quoted_column} FROM ({base_query} ORDER BY RANDOM() LIMIT {sample_size}) AS sampled_data ORDER BY {quoted_column} LIMIT {limit * 2}"
            else:
                query = f"SELECT DISTINCT {quoted_column} FROM ({base_query}) AS all_data ORDER BY {quoted_column} LIMIT {limit * 2}"

            result = conn.execute(query).fetchall()
            conn.close()

            values = []
            seen = set()
            for row in result:
                if row[0] is None: continue
                clean_val = self._clean_value(str(row[0]))
                if clean_val and clean_val not in seen:
                    seen.add(clean_val)
                    values.append(clean_val)
                    if len(values) >= limit: break
            return values
        except Exception as e:
            raise Exception(f"获取列值失败: {str(e)}")

    @staticmethod
    def _escape_column_name(column_name):
        if column_name is None: return ""
        escaped_col = column_name.replace('"', '""')
        return f'"{escaped_col}"'

    @staticmethod
    def _clean_value(value):
        if value is None: return ""
        return re.sub(r'[\t\r\n]', '', str(value)).strip()

    @staticmethod
    def detect_encoding(file_path, sample_size=1024 * 1024):
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(min(sample_size, os.path.getsize(file_path)))
                result = chardet.detect(raw_data)
                encoding = result['encoding'] or 'utf-8'
                if result['confidence'] < 0.5:
                    if raw_data[:3] == b'\xef\xbb\xbf':
                        encoding = 'utf-8-sig'
                    elif raw_data[:2] == b'\xff\xfe':
                        encoding = 'utf-16-le'
                    elif raw_data[:2] == b'\xfe\xff':
                        encoding = 'utf-16-be'
                    else:
                        for enc in ['utf-8', 'gbk', 'gb18030', 'latin-1']:
                            try:
                                raw_data.decode(enc)
                                encoding = enc
                                break
                            except:
                                pass
        except:
            encoding = 'utf-8'
        return encoding

    @staticmethod
    def clean_filename(filename):
        illegal_chars = '<>:"/\\|?*'
        for char in illegal_chars:
            filename = filename.replace(char, '_')
        filename = filename.strip().strip('.')
        if not filename: filename = 'output'
        return filename[:200]

    @staticmethod
    def format_file_size(size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"

    def cancel(self):
        self.cancelled = True


class ModernCSVTools(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV工具箱 Pro")
        self.resize(1000, 700)
        self.csv_files = []
        self.current_worker = None
        self.extract_converted_temp_file = None  # 添加临时文件路径属性
        self.setup_styles()
        self.setup_ui()
        self.center_window()
        self.setup_icon()

    def setup_icon(self):
        """设置应用程序图标和任务栏图标"""
        try:
            # 尝试多种路径寻找图标
            icon_paths = [
                # 1. 当前目录下的CSV.ico
                'CSV.ico',
                # 2. 与脚本同目录的CSV.ico
                os.path.join(os.path.dirname(__file__), 'CSV.ico'),
                # 3. 可执行文件目录下的CSV.ico（打包后）
                os.path.join(os.path.dirname(sys.executable), 'CSV.ico'),
                # 4. 资源目录下的图标
                'resources/CSV.ico',
            ]

            icon_path = None
            for path in icon_paths:
                if os.path.exists(path):
                    icon_path = path
                    break

            if icon_path:
                icon = QIcon(icon_path)
                # 设置窗口图标
                self.setWindowIcon(icon)
                # 设置应用程序图标（Windows任务栏）
                QApplication.setWindowIcon(icon)
                print(f"✓ 图标设置成功: {icon_path}")
            else:
                print("⚠ 未找到CSV.ico图标，使用默认图标")
                # 使用内置的Qt图标作为备用
                self.setWindowIcon(self.style().standardIcon(QStyle.SP_ComputerIcon))

        except Exception as e:
            print(f"⚠ 图标加载失败: {str(e)}")
            # 即使图标加载失败，也要确保窗口有图标
            self.setWindowIcon(self.style().standardIcon(QStyle.SP_ComputerIcon))

    def setup_styles(self):
        style = """
        QSpinBox, QDoubleSpinBox {
            background-color: #ffffff; color: #333333; border: 1px solid #d0d0d0;
            border-radius: 4px; padding: 4px; font-size: 11px;
        }
        QSpinBox:hover, QDoubleSpinBox:hover { border-color: #2196F3; }
        QSpinBox::up-button, QDoubleSpinBox::up-button {
            border: 1px solid #d0d0d0; border-top-right-radius: 3px;
            background-color: #f0f0f0; width: 16px; height: 10px;
        }
        QSpinBox::down-button, QDoubleSpinBox::down-button {
            border: 1px solid #d0d0d0; border-bottom-right-radius: 3px;
            background-color: #f0f0f0; width: 16px; height: 10px;
        }
        QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover,
        QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover {
            background-color: #e0e0e0;
        }
        QSpinBox::up-arrow, QDoubleSpinBox::up-arrow {
            image: none; border-left: 4px solid transparent; border-right: 4px solid transparent;
            border-bottom: 5px solid #666666; margin-top: 2px;
        }
        QSpinBox::down-arrow, QDoubleSpinBox::down-arrow {
            image: none; border-left: 4px solid transparent; border-right: 4px solid transparent;
            border-top: 5px solid #666666; margin-top: 2px;
        }
        QMainWindow { background-color: #f5f5f5; }
        QTabWidget::pane { border: 1px solid #d0d0d0; background-color: #ffffff; margin: 0px; border-radius: 6px; }
        QTabBar::tab {
            background-color: #e0e0e0; color: #333333; padding: 8px 20px; font-size: 12px; font-weight: bold;
            border: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 2px;
        }
        QTabBar::tab:selected { background-color: #2196F3; color: white; }
        QTabBar::tab:hover:!selected { background-color: #d0d0d0; color: #333333; }
        QGroupBox {
            border: 1px solid #d0d0d0; border-radius: 6px; margin-top: 10px; padding-top: 15px;
            font-weight: bold; background-color: #ffffff;
        }
        QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; color: #2196F3; }
        QComboBox {
            background-color: #ffffff; color: #333333; border: 1px solid #d0d0d0; border-radius: 4px;
            padding: 4px 6px; font-size: 11px; min-height: 22px; padding-right: 6px;
        }
        QComboBox:hover { border-color: #2196F3; }
        QComboBox::drop-down { border: none; width: 0px; subcontrol-position: right center; }
        QComboBox::down-arrow { image: none; border: none; }
        QComboBox QAbstractItemView { background-color: #ffffff; border: 1px solid #d0d0d0; selection-background-color: #2196F3; selection-color: white; }
        QFrame[class="card"] { background-color: #ffffff; border-radius: 6px; border: 1px solid #e0e0e0; margin: 2px; padding: 8px; }
        QLabel[class="card-title"] { color: #2196F3; font-size: 12px; font-weight: bold; margin-bottom: 4px; }
        QPushButton {
            background-color: #d0d0d0; color: #333333; border: none; border-radius: 4px; padding: 6px 12px;
            font-size: 11px; font-weight: bold; min-width: 70px; min-height: 24px;
        }
        QPushButton:hover { background-color: #d0d0d0; color: #000000; }
        QPushButton:pressed { background-color: #c0c0c0; }
        QPushButton:disabled { background-color: #f5f5f5; color: #999999; }
        QPushButton[class="primary"] { background-color: #2196F3; color: white; }
        QPushButton[class="primary"]:hover { background-color: #42A5F5; }
        QPushButton[class="success"] { background-color: #4CAF50; color: white; }
        QPushButton[class="success"]:hover { background-color: #66BB6A; }
        QPushButton[class="danger"] { background-color: #F44336; color: white; }
        QPushButton[class="danger"]:hover { background-color: #EF5350; }
        QPushButton[class="secondary"] { background-color: #FF9800; color: white; }
        QPushButton[class="secondary"]:hover { background-color: #FFB74D; }
        QPushButton[class="merge-primary"] { background-color: #00BCD4; color: white; }
        QPushButton[class="merge-primary"]:hover { background-color: #26C6DA; }
        QPushButton[class="split-primary"] { background-color: #9C27B0; color: white; }
        QPushButton[class="split-primary"]:hover { background-color: #AB47BC; }
        QPushButton[class="extract-primary"] { background-color: #1565C0; color: white; }
        QPushButton[class="extract-primary"]:hover { background-color: #1976D2; }
        QPushButton[class="browse"] { background-color: #757575; color: white; }
        QPushButton[class="browse"]:hover { background-color: #9E9E9E; }
        QLineEdit {
            background-color: #ffffff; color: #333333; border: 1px solid #d0d0d0; border-radius: 4px;
            padding: 6px 8px; font-size: 11px; selection-background-color: #2196F3; selection-color: white;
        }
        QLineEdit:focus { border: 1px solid #2196F3; outline: none; }
        QLineEdit:disabled { background-color: #f5f5f5; color: #999999; }
        QCheckBox { color: #333333; font-size: 11px; spacing: 6px; }
        QCheckBox::indicator { width: 16px; height: 16px; border-radius: 3px; background-color: #ffffff; border: 1px solid #d0d0d0; }
        QCheckBox::indicator:hover { border-color: #2196F3; }
        QCheckBox::indicator:checked { background-color: #2196F3; border: 1px solid #2196F3; image: url(none); }
        QCheckBox::indicator:checked:hover { background-color: #42A5F5; }
        QTreeWidget {
            background-color: #ffffff; color: #333333; border: 1px solid #d0d0d0; border-radius: 4px;
            font-size: 10px; alternate-background-color: #fafafa;
        }
        QTreeWidget::item { height: 24px; padding: 2px 4px; }
        QTreeWidget::item:hover { background-color: #e3f2fd; }
        QTreeWidget::item:selected { background-color: #2196F3; color: white; }
        QTreeWidget::item:selected:!active { background-color: #f5f5f5; color: #333333; }
        QHeaderView::section { background-color: #2196F3; color: white; padding: 8px; border: none; font-weight: bold; font-size: 10px; }
        QTextEdit {
            background-color: #1e1e1e; color: #e0e0e0; border: 1px solid #333333; border-radius: 4px;
            font-family: Consolas, monospace; font-size: 10px; padding: 6px; background-clip: padding;
            selection-background-color: #2196F3; selection-color: white;
        }
        QTextEdit::selection { background-color: #2196F3; color: white; }
        QLabel { color: #333333; font-size: 11px; }
        QLabel[class="title"] { color: #2196F3; font-size: 18px; font-weight: bold; }
        QLabel[class="subtitle"] { color: #2196F3; font-size: 12px; font-weight: bold; }
        QLabel[class="info"] { color: #2196F3; font-size: 11px; font-weight: bold; }
        QLabel[class="success"] { color: #4CAF50; font-size: 11px; font-weight: bold; }
        QLabel[class="error"] { color: #F44336; font-size: 11px; font-weight: bold; }
        QProgressBar { border: 1px solid #d0d0d0; border-radius: 4px; background-color: #ffffff; text-align: center; }
        QProgressBar::chunk { background-color: #2196F3; border-radius: 3px; transition: width 0.3s ease; }
        QFrame[class="split-config"] { background-color: #f8f9fa; border: 1px solid #d0d0d0; border-radius: 6px; margin: 2px; padding: 12px; }
        QLabel[class="config-label"] { color: #444444; font-size: 11px; font-weight: bold; }
        /* 邮件链接按钮样式 */
        QPushButton[class="mail-link"] {
            background-color: transparent; color: #33CCFF; border: none; 
            font-size: 11px; padding: 0px; margin: 0px; text-decoration: underline;
            min-width: 0px; min-height: 0px;
        }
        QPushButton[class="mail-link"]:hover { color: #66D9FF; }
        QPushButton[class="mail-link"]:pressed { color: #0099CC; }
        /* GitHub链接按钮样式 */
        QPushButton[class="github-link"] {
            background-color: transparent; color: #33CCFF; border: none; 
            font-size: 11px; padding: 0px; margin: 0px; text-decoration: underline;
            min-width: 0px; min-height: 0px;
        }
        QPushButton[class="github-link"]:hover { color: #66D9FF; }
        QPushButton[class="github-link"]:pressed { color: #0099CC; }
        """
        self.setStyleSheet(style)

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(8)

        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)
        logo_label = QLabel("📊")
        logo_label.setFont(QFont("Segoe UI", 20))
        logo_label.setStyleSheet("color: #2196F3;")
        header_layout.addWidget(logo_label)
        title_label = QLabel("CSV工具箱 Pro")
        title_label.setProperty("class", "title")
        header_layout.addWidget(title_label)
        header_layout.addStretch()

        # 添加GitHub仓库链接按钮
        github_btn = QPushButton("v3.2")
        github_btn.setProperty("class", "github-link")
        github_btn.setToolTip("点击访问GitHub仓库可查看源码和使用说明")
        github_btn.setCursor(Qt.PointingHandCursor)
        github_btn.clicked.connect(self.open_github_repo)
        header_layout.addWidget(github_btn)

        # 添加分隔符
        separator = QLabel("|")
        separator.setStyleSheet("color: #33CCFF; font-size: 11px;")
        header_layout.addWidget(separator)

        # 添加邮件链接按钮
        mail_btn = QPushButton("@叾屾")
        mail_btn.setProperty("class", "mail-link")
        mail_btn.setToolTip("点击发送邮件给开发者: zhangfuyi_52@outlook.com")
        mail_btn.setCursor(Qt.PointingHandCursor)
        mail_btn.clicked.connect(self.contact_developer)
        header_layout.addWidget(mail_btn)

        main_layout.addLayout(header_layout)

        self.tab_widget = QTabWidget()
        self.tab_widget.setDocumentMode(True)
        main_layout.addWidget(self.tab_widget)

        self.create_merge_tab()
        self.create_split_tab()
        self.create_extract_tab()

    def open_github_repo(self):
        """打开GitHub仓库页面"""
        github_url = "https://github.com/0039fy/CSV-Tools-Pro/"

        # 使用QDesktopServices打开GitHub仓库页面
        if QDesktopServices.openUrl(QUrl(github_url)):
            self.log_message(f"正在打开GitHub仓库页面...", self.merge_log, "info")
        else:
            # 如果打开失败，显示提示信息
            QMessageBox.information(self, "访问GitHub仓库",
                                    f"无法打开浏览器。\n\n"
                                    f"请手动访问：\n"
                                    f"{github_url}")

            # 复制GitHub仓库地址到剪贴板
            clipboard = QApplication.clipboard()
            clipboard.setText(github_url)
            self.log_message(f"已复制GitHub仓库地址到剪贴板: {github_url}", self.merge_log, "info")

    def contact_developer(self):
        """联系开发者 - 打开默认邮件客户端"""
        email = "zhangfuyi_52@outlook.com"
        subject = "CSV工具箱 Pro 反馈/问题"
        body = """尊敬的用户：

感谢您使用CSV工具箱 Pro！

请在此处描述您遇到的问题或建议：

1. 问题描述：
2. 重现步骤：
3. 期望结果：
4. 实际结果：
5. 其他说明：

请附上相关文件或截图（如有需要）。

感谢您的反馈！

---
(以上内容由工具自动生成，可删除)
"""

        # 创建mailto URL
        mailto_url = f"mailto:{email}?subject={subject}&body={body}"

        # 使用QDesktopServices打开邮件客户端
        if QDesktopServices.openUrl(QUrl(mailto_url)):
            self.log_message(f"正在打开邮件客户端...", self.merge_log, "info")
        else:
            # 如果打开失败，显示提示信息
            QMessageBox.information(self, "联系开发者",
                                    f"无法打开邮件客户端。\n\n"
                                    f"请手动发送邮件到：\n"
                                    f"{email}\n\n"
                                    f"主题：{subject}\n\n"
                                    f"或者复制以下信息：\n{body}")

            # 复制邮箱地址到剪贴板
            clipboard = QApplication.clipboard()
            clipboard.setText(email)
            self.log_message(f"已复制邮箱地址到剪贴板: {email}", self.merge_log, "info")

    def create_merge_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_row = QHBoxLayout()
        title_row.setSpacing(6)
        title_label = QLabel("📁 文件列表")
        title_label.setProperty("class", "card-title")
        title_row.addWidget(title_label)
        title_row.addStretch()

        self.add_files_btn = QPushButton("+ 添加")
        self.add_files_btn.setProperty("class", "success")
        self.add_files_btn.clicked.connect(self.add_csv_files)
        title_row.addWidget(self.add_files_btn)

        self.remove_file_btn = QPushButton("- 移除")
        self.remove_file_btn.setProperty("class", "danger")
        self.remove_file_btn.clicked.connect(self.remove_selected_file)
        title_row.addWidget(self.remove_file_btn)

        self.clear_files_btn = QPushButton("清空")
        self.clear_files_btn.setProperty("class", "danger")
        self.clear_files_btn.clicked.connect(self.clear_files_list)
        title_row.addWidget(self.clear_files_btn)

        card_layout.addLayout(title_row)

        self.files_tree = QTreeWidget()
        self.files_tree.setHeaderLabels(["序号", "文件名", "大小", "修改时间"])
        self.files_tree.setColumnWidth(0, 50)
        self.files_tree.setColumnWidth(1, 250)
        self.files_tree.setColumnWidth(2, 80)
        self.files_tree.setColumnWidth(3, 120)
        self.files_tree.setAlternatingRowColors(True)
        self.files_tree.setMinimumHeight(150)
        card_layout.addWidget(self.files_tree)
        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)
        title_label = QLabel("⚙️ 合并选项")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        path_row = QHBoxLayout()
        path_row.setSpacing(6)
        path_row.addWidget(QLabel("输出路径:"))
        self.output_path = QLineEdit()
        self.output_path.setPlaceholderText("选择或输入输出文件路径...")
        path_row.addWidget(self.output_path, 1)
        browse_btn = QPushButton("浏览")
        browse_btn.setProperty("class", "browse")
        browse_btn.clicked.connect(self.browse_output_path)
        path_row.addWidget(browse_btn)
        card_layout.addLayout(path_row)

        self.header_check = QCheckBox("保留表头（保留第一个文件表头，不勾合并后文件无表头）")
        self.header_check.setChecked(True)
        card_layout.addWidget(self.header_check)
        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)
        title_label = QLabel("⚡ 操作")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        self.merge_progress = QProgressBar()
        self.merge_progress.setVisible(False)
        card_layout.addWidget(self.merge_progress)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(8)
        self.merge_btn = QPushButton("🚀 开始合并")
        self.merge_btn.setProperty("class", "merge-primary")
        self.merge_btn.clicked.connect(self.start_merge)
        btn_layout.addWidget(self.merge_btn)

        self.cancel_merge_btn = QPushButton("取消")
        self.cancel_merge_btn.setProperty("class", "secondary")
        self.cancel_merge_btn.setEnabled(False)
        self.cancel_merge_btn.clicked.connect(self.cancel_merge)
        btn_layout.addWidget(self.cancel_merge_btn)

        self.open_output_btn = QPushButton("打开输出目录")
        self.open_output_btn.setProperty("class", "secondary")
        self.open_output_btn.clicked.connect(self.open_output_directory)
        btn_layout.addWidget(self.open_output_btn)

        card_layout.addLayout(btn_layout)
        layout.addWidget(card)

        log_group = QGroupBox("📝 日志")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(0, 15, 0, 0)
        log_layout.setSpacing(0)

        self.merge_log = QTextEdit()
        self.merge_log.setReadOnly(True)
        self.merge_log.setMinimumHeight(120)
        log_layout.addWidget(self.merge_log)
        layout.addWidget(log_group)

        self.tab_widget.addTab(tab, "📁 文件合并")

    def create_split_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_label = QLabel("📁 选择文件")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        file_row = QHBoxLayout()
        file_row.setSpacing(6)
        file_row.addWidget(QLabel("CSV文件:"))
        self.split_file_path = QLineEdit()
        self.split_file_path.setPlaceholderText("选择要分割的CSV文件...")
        file_row.addWidget(self.split_file_path, 1)

        browse_btn = QPushButton("浏览")
        browse_btn.setProperty("class", "browse")
        browse_btn.clicked.connect(self.browse_split_file)
        file_row.addWidget(browse_btn)
        card_layout.addLayout(file_row)

        self.file_info_label = QLabel("请选择文件...")
        self.file_info_label.setProperty("class", "info")
        card_layout.addWidget(self.file_info_label)

        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "split-config")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(8)

        title_label = QLabel("⚙️ 分割选项")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        row1 = QHBoxLayout()
        row1.setSpacing(12)
        row1.addWidget(QLabel("分割方式:"))
        self.split_method = QComboBox()
        self.split_method.addItems(["按行数分割", "按文件数分割", "按大小分割"])
        self.split_method.currentTextChanged.connect(self.on_split_method_changed)
        row1.addWidget(self.split_method)
        self.split_method.setFixedWidth(90)

        self.split_param_label = QLabel("每份行数:")
        row1.addWidget(self.split_param_label)

        self.split_param_spin = QSpinBox()
        self.split_param_spin.setRange(1, 10000000)
        self.split_param_spin.setValue(1000000)
        self.split_param_spin.setSuffix(" 行")
        row1.addWidget(self.split_param_spin)
        row1.addStretch()
        card_layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(12)
        row2.addWidget(QLabel("文件名前缀:"))
        self.file_prefix = QLineEdit()
        self.file_prefix.setText("split_part")
        self.file_prefix.setFixedWidth(150)
        row2.addWidget(self.file_prefix)

        row2.addWidget(QLabel("输出目录:"))
        self.split_output_dir = QLineEdit()
        row2.addWidget(self.split_output_dir, 1)

        browse_btn = QPushButton("浏览")
        browse_btn.setProperty("class", "browse")
        browse_btn.clicked.connect(self.browse_split_output_dir)
        row2.addWidget(browse_btn)

        card_layout.addLayout(row2)
        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_label = QLabel("⚡ 操作")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        self.split_progress = QProgressBar()
        self.split_progress.setVisible(False)
        card_layout.addWidget(self.split_progress)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(8)

        self.split_btn = QPushButton("✂️ 开始分割")
        self.split_btn.setProperty("class", "split-primary")
        self.split_btn.clicked.connect(self.start_split)
        btn_layout.addWidget(self.split_btn)

        self.cancel_split_btn = QPushButton("取消")
        self.cancel_split_btn.setProperty("class", "secondary")
        self.cancel_split_btn.setEnabled(False)
        self.cancel_split_btn.clicked.connect(self.cancel_split)
        btn_layout.addWidget(self.cancel_split_btn)

        self.open_dir_btn = QPushButton("📂 打开输出目录")
        self.open_dir_btn.setProperty("class", "secondary")
        self.open_dir_btn.clicked.connect(self.open_output_directory)
        btn_layout.addWidget(self.open_dir_btn)

        card_layout.addLayout(btn_layout)
        layout.addWidget(card)

        log_group = QGroupBox("📝 日志")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(0, 15, 0, 0)
        log_layout.setSpacing(0)

        self.split_log = QTextEdit()
        self.split_log.setReadOnly(True)
        self.split_log.setMinimumHeight(120)
        log_layout.addWidget(self.split_log)
        layout.addWidget(log_group)

        self.tab_widget.addTab(tab, "✂️ 文件分割")

    def create_extract_tab(self):
        if not DUCKDB_AVAILABLE:
            tab = QWidget()
            layout = QVBoxLayout(tab)
            layout.setContentsMargins(20, 20, 20, 20)
            warning_label = QLabel("数据提取功能需要安装 duckdb")
            warning_label.setStyleSheet("color: #F44336; font-size: 14px; font-weight: bold;")
            warning_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(warning_label)
            info_label = QLabel("请执行以下命令安装:")
            info_label.setStyleSheet("font-size: 12px;")
            info_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(info_label)
            cmd_label = QLabel("pip install duckdb")
            cmd_label.setStyleSheet(
                "font-family: Consolas; background-color: #f5f5f5; padding: 10px; border-radius: 4px;")
            cmd_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(cmd_label)
            self.tab_widget.addTab(tab, "🔍 数据提取")
            return

        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_label = QLabel("📁 选择CSV文件")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        file_row = QHBoxLayout()
        file_row.setSpacing(6)
        file_row.addWidget(QLabel("CSV文件:"))
        self.extract_file_path = QLineEdit()
        self.extract_file_path.setPlaceholderText("选择要提取数据的CSV文件...")
        file_row.addWidget(self.extract_file_path, 1)

        browse_btn = QPushButton("浏览")
        browse_btn.setProperty("class", "browse")
        browse_btn.clicked.connect(self.browse_extract_file)
        file_row.addWidget(browse_btn)
        card_layout.addLayout(file_row)

        self.extract_file_info = QLabel("请选择文件...")
        self.extract_file_info.setProperty("class", "info")
        card_layout.addWidget(self.extract_file_info)

        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "split-config")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(8)

        title_label = QLabel("⚙️ 提取条件")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        col_row = QHBoxLayout()
        col_row.setSpacing(12)
        col_row.addWidget(QLabel("列 名:"))
        self.extract_column_combo = QComboBox()
        self.extract_column_combo.setFixedWidth(200)
        col_row.addWidget(self.extract_column_combo)

        self.extract_col_type_label = QLabel("")
        self.extract_col_type_label.setStyleSheet("color: #2196F3; font-size: 11px;")
        col_row.addWidget(self.extract_col_type_label)
        col_row.addStretch()
        card_layout.addLayout(col_row)

        val_row = QHBoxLayout()
        val_row.setSpacing(12)
        val_row.addWidget(QLabel("数 值:"))
        self.extract_value_input = QLineEdit()
        self.extract_value_input.setPlaceholderText("手动输入值或点击获取示例值")
        self.extract_value_input.setFixedWidth(200)
        val_row.addWidget(self.extract_value_input)

        select_btn = QPushButton("获取示例值")
        select_btn.setProperty("class", "secondary")
        select_btn.clicked.connect(self.select_sample_value)
        select_btn.setToolTip("从当前列的示例值中选择")
        val_row.addWidget(select_btn)

        # 修改清空按钮的提示信息
        clear_btn = QPushButton("清空")
        clear_btn.setProperty("class", "browse")
        clear_btn.clicked.connect(self.clear_value_input)
        clear_btn.setToolTip("清空当前输入的值和示例值列表")
        val_row.addWidget(clear_btn)

        val_row.addStretch()
        card_layout.addLayout(val_row)

        tip_label = QLabel("\n提示：可直接输入该列已知内容提取，或点击'获取示例值'从列表中选择\n\n"
                           "对于GB级大文件为节约时间采用随机分块获取方式，若列表中没有预期结果，可清空再次获取")
        tip_label.setStyleSheet("color: #FF3300; font-size: 10px; font-style: italic;")
        tip_label.setWordWrap(True)
        card_layout.addWidget(tip_label)

        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_label = QLabel("📤 输出设置")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        path_row = QHBoxLayout()
        path_row.setSpacing(6)
        path_row.addWidget(QLabel("输出路径:"))
        self.extract_output_path = QLineEdit()
        self.extract_output_path.setPlaceholderText("选择或输入输出文件路径...")
        path_row.addWidget(self.extract_output_path, 1)

        browse_btn = QPushButton("浏览")
        browse_btn.setProperty("class", "browse")
        browse_btn.clicked.connect(self.browse_extract_output)
        path_row.addWidget(browse_btn)
        card_layout.addLayout(path_row)

        layout.addWidget(card)

        card = QFrame()
        card.setProperty("class", "card")
        card.setFrameShape(QFrame.StyledPanel)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(8, 6, 8, 8)
        card_layout.setSpacing(6)

        title_label = QLabel("⚡ 操作")
        title_label.setProperty("class", "card-title")
        card_layout.addWidget(title_label)

        self.extract_progress = QProgressBar()
        self.extract_progress.setVisible(False)
        card_layout.addWidget(self.extract_progress)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(8)

        self.extract_btn = QPushButton("🔍 开始提取")
        self.extract_btn.setProperty("class", "extract-primary")
        self.extract_btn.clicked.connect(self.start_extract)
        btn_layout.addWidget(self.extract_btn)

        self.cancel_extract_btn = QPushButton("取消")
        self.cancel_extract_btn.setProperty("class", "secondary")
        self.cancel_extract_btn.setEnabled(False)
        self.cancel_extract_btn.clicked.connect(self.cancel_extract)
        btn_layout.addWidget(self.cancel_extract_btn)

        self.open_extract_output_btn = QPushButton("📂 打开输出目录")
        self.open_extract_output_btn.setProperty("class", "secondary")
        self.open_extract_output_btn.clicked.connect(self.open_extract_output_directory)
        btn_layout.addWidget(self.open_extract_output_btn)

        card_layout.addLayout(btn_layout)
        layout.addWidget(card)

        log_group = QGroupBox("📝 日志")
        log_layout = QVBoxLayout(log_group)
        log_layout.setContentsMargins(0, 15, 0, 0)
        log_layout.setSpacing(0)

        self.extract_log = QTextEdit()
        self.extract_log.setReadOnly(True)
        self.extract_log.setMinimumHeight(120)
        log_layout.addWidget(self.extract_log)
        layout.addWidget(log_group)

        self.tab_widget.addTab(tab, "🔍 数据提取")

        self.extract_csv_info = None
        self.extract_column_mapping = {}
        self.extract_column_types = {}
        self.current_sample_values = []

    def on_split_method_changed(self, method):
        if method == "按行数分割":
            self.split_param_label.setText("每份行数:")
            self.split_param_spin.setSuffix(" 行")
            self.split_param_spin.setRange(1, 10000000)
            self.split_param_spin.setValue(1000000)
        elif method == "按文件数分割":
            self.split_param_label.setText("文件数量:")
            self.split_param_spin.setSuffix(" 个")
            self.split_param_spin.setRange(2, 1000)
            self.split_param_spin.setValue(10)
        elif method == "按大小分割":
            self.split_param_label.setText("每份大小:")
            self.split_param_spin.setSuffix(" MB")
            self.split_param_spin.setRange(1, 1024)
            self.split_param_spin.setValue(100)

    def center_window(self):
        qr = self.frameGeometry()
        cp = self.screen().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def log_message(self, message, widget=None, msg_type="info"):
        if widget is None:
            widget = self.merge_log if self.tab_widget.currentIndex() == 0 else self.split_log

        timestamp = time.strftime('%H:%M:%S')
        color_map = {"info": "#e0e0e0", "success": "#4CAF50", "warning": "#FF9800", "error": "#F44336"}
        color = color_map.get(msg_type, "#e0e0e0")
        prefix_map = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}
        prefix = prefix_map.get(msg_type, "ℹ️")

        if self.tab_widget.currentIndex() in [0, 2]:
            if "开始分割" in message or "分割完成" in message or "错误" in message:
                widget.append('<hr style="border: none; border-top: 1px solid #333333; margin: 4px 0;">')

        html = f'<span style="color: #888888;">[{timestamp}]</span> <span style="color: {color};">{prefix} {message}</span><br>'
        widget.append(html)
        scrollbar = widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        widget.update()

    def log_progress_detail(self, progress_info):
        widget = self.split_log
        timestamp = time.strftime('%H:%M:%S')
        html = f'<span style="color: #888888;">[{timestamp}]</span> '
        html += f'<span style="color: #2196F3;">⚙️ {progress_info["progress"]} 生成: {progress_info["filename"]}</span><br>'
        widget.append(html)
        scrollbar = widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        widget.update()

    def update_file_info(self, file_path):
        if not os.path.exists(file_path): return
        file_size = os.path.getsize(file_path)
        size_str = WorkerThread.format_file_size(file_size)
        try:
            encoding = WorkerThread.detect_encoding(file_path)
            info_text = f"大小: {size_str} | 编码: {encoding}"
            self.file_info_label.setText(info_text)
        except Exception as e:
            self.file_info_label.setText(f"大小: {size_str} | 无法检测编码")

    def add_csv_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "选择CSV文件", "", "CSV文件 (*.csv *.txt);;所有文件 (*.*)")
        if files:
            for file_path in files:
                if file_path not in self.csv_files:
                    self.csv_files.append(file_path)
            self.update_files_treeview()
            self.log_message(f"已添加 {len(files)} 个文件", self.merge_log, "success")
            if self.csv_files and not self.output_path.text():
                first_file = self.csv_files[0]
                first_dir = os.path.dirname(first_file)
                base_name = os.path.basename(first_file)
                root, ext = os.path.splitext(base_name)
                self.output_path.setText(os.path.join(first_dir, f"{root}_merged{ext}"))

    def update_files_treeview(self):
        self.files_tree.clear()
        for i, file_path in enumerate(self.csv_files, 1):
            if os.path.exists(file_path):
                filename = os.path.basename(file_path)
                file_size = WorkerThread.format_file_size(os.path.getsize(file_path))
                mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(file_path)))
                QTreeWidgetItem(self.files_tree, [str(i), filename, file_size, mtime])

    def remove_selected_file(self):
        selected_items = self.files_tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "提示", "请先选择要移除的文件")
            return
        for item in selected_items:
            index = int(item.text(0)) - 1
            if 0 <= index < len(self.csv_files):
                removed_file = self.csv_files.pop(index)
                self.log_message(f"已移除: {os.path.basename(removed_file)}", self.merge_log, "warning")
        self.update_files_treeview()

    def clear_files_list(self):
        if not self.csv_files: return
        reply = QMessageBox.question(self, "确认", f"确定要清空文件列表吗？\n共 {len(self.csv_files)} 个文件",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.csv_files.clear()
            self.update_files_treeview()
            self.output_path.clear()
            self.log_message("文件列表已清空", self.merge_log, "warning")

    def browse_output_path(self):
        current_path = self.output_path.text()
        directory = os.path.dirname(current_path) if current_path else ""
        default_name = os.path.basename(current_path) if current_path else "merged_result.csv"
        file_path, _ = QFileDialog.getSaveFileName(self, "选择输出文件", os.path.join(directory, default_name),
                                                   "CSV文件 (*.csv);;文本文件 (*.txt);;所有文件 (*.*)")
        if file_path: self.output_path.setText(file_path)

    def browse_split_file(self):
        file_path, _ = QFileDialog.getOpenFileNames(self, "选择要分割的CSV文件", "",
                                                    "CSV文件 (*.csv *.txt);;所有文件 (*.*)")
        if file_path:
            self.split_file_path.setText(file_path[0])
            if not self.split_output_dir.text():
                self.split_output_dir.setText(os.path.dirname(file_path[0]))
            self.update_file_info(file_path[0])

    def browse_split_output_dir(self):
        directory = QFileDialog.getExistingDirectory(self, "选择输出目录")
        if directory: self.split_output_dir.setText(directory)

    def browse_extract_file(self):
        # 清理之前的临时文件
        self.clear_extract_temp_file()

        file_path, _ = QFileDialog.getOpenFileName(self, "选择要提取数据的CSV文件", "",
                                                   "CSV文件 (*.csv *.txt);;所有文件 (*.*)")
        if file_path:
            self.extract_file_path.setText(file_path)
            self.load_extract_file_info(file_path)

    def browse_extract_output(self):
        current_path = self.extract_output_path.text()
        directory = os.path.dirname(current_path) if current_path else ""
        default_name = os.path.basename(current_path) if current_path else "提取结果.csv"
        file_path, _ = QFileDialog.getSaveFileName(self, "选择输出文件", os.path.join(directory, default_name),
                                                   "CSV文件 (*.csv)")
        if file_path: self.extract_output_path.setText(file_path)

    def open_output_directory(self):
        if self.tab_widget.currentIndex() == 0:
            path = self.output_path.text()
            if not path:
                QMessageBox.warning(self, "提示", "请先选择输出文件路径")
                return
            directory = os.path.dirname(path)
        else:
            path = self.split_output_dir.text()
            if not path:
                QMessageBox.warning(self, "提示", "请先选择输出目录")
                return
            directory = path

        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        try:
            if sys.platform == "win32":
                os.startfile(directory)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", directory])
            else:
                subprocess.Popen(["xdg-open", directory])
        except Exception as e:
            QMessageBox.warning(self, "提示", f"无法打开目录: {str(e)}")

    def open_extract_output_directory(self):
        path = self.extract_output_path.text()
        if not path:
            QMessageBox.warning(self, "提示", "请先选择输出文件路径")
            return
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        try:
            if sys.platform == "win32":
                os.startfile(directory)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", directory])
            else:
                subprocess.Popen(["xdg-open", directory])
        except Exception as e:
            QMessageBox.warning(self, "提示", f"无法打开目录: {str(e)}")

    def start_merge(self):
        if self.current_worker and self.current_worker.isRunning():
            QMessageBox.warning(self, "警告", "操作正在进行中，请稍候...")
            return
        if not self.csv_files:
            QMessageBox.warning(self, "警告", "请先添加要合并的CSV文件！")
            return
        output_path = self.output_path.text()
        if not output_path:
            QMessageBox.warning(self, "警告", "请选择输出路径！")
            return
        if os.path.exists(output_path):
            reply = QMessageBox.question(self, "确认", "输出文件已存在，是否覆盖？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No: return

        self.merge_btn.setEnabled(False)
        self.cancel_merge_btn.setEnabled(True)
        self.merge_progress.setVisible(True)
        self.merge_progress.setValue(0)
        self.merge_log.clear()
        self.log_message("开始合并文件...", self.merge_log)

        self.current_worker = WorkerThread('merge', self.csv_files, output_path, self.header_check.isChecked())
        self.current_worker.progress.connect(lambda msg: self.log_message(msg, self.merge_log))
        self.current_worker.progress_percentage.connect(self.merge_progress.setValue)
        self.current_worker.completed.connect(self.merge_completed)
        self.current_worker.error.connect(lambda msg: self.merge_error(msg))
        self.current_worker.start()

    def merge_completed(self, result):
        elapsed_time, output_size, output_path = result
        self.merge_btn.setEnabled(True)
        self.cancel_merge_btn.setEnabled(False)
        self.merge_progress.setVisible(False)
        self.log_message(f"✅ 合并完成！耗时: {elapsed_time:.2f}秒", self.merge_log, "success")

        total_input_size = sum(os.path.getsize(f) for f in self.csv_files)
        speed = total_input_size / elapsed_time / 1024 / 1024 if elapsed_time > 0 else 0
        QMessageBox.information(self, "完成",
                                f"✅ 文件合并完成！\n\n📊 耗时: {elapsed_time:.2f}秒\n📁 输出文件: {os.path.basename(output_path)}\n💾 文件大小: {WorkerThread.format_file_size(output_size)}\n⚡ 处理速度: {speed:.1f} MB/秒")
        self.current_worker = None

    def merge_error(self, error_msg):
        self.merge_btn.setEnabled(True)
        self.cancel_merge_btn.setEnabled(False)
        self.merge_progress.setVisible(False)
        self.log_message(f"❌ 错误: {error_msg}", self.merge_log, "error")
        QMessageBox.critical(self, "错误", f"合并过程中出错:\n{error_msg}")
        self.current_worker = None

    def cancel_merge(self):
        if self.current_worker and self.current_worker.isRunning():
            reply = QMessageBox.question(self, "确认", "确定要取消当前操作吗？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                self.current_worker.cancel()
                self.log_message("操作已被用户取消", self.merge_log, "warning")
                self.merge_btn.setEnabled(True)
                self.cancel_merge_btn.setEnabled(False)
                self.merge_progress.setVisible(False)
                self.current_worker = None

    def start_split(self):
        if self.current_worker and self.current_worker.isRunning():
            QMessageBox.warning(self, "警告", "操作正在进行中，请稍候...")
            return
        file_path = self.split_file_path.text()
        output_dir = self.split_output_dir.text()
        if not file_path or not os.path.exists(file_path):
            QMessageBox.critical(self, "错误", "请选择有效的文件")
            return
        if not output_dir:
            output_dir = os.path.dirname(file_path)
            self.split_output_dir.setText(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        try:
            split_method = self.split_method.currentText()
            param_value = self.split_param_spin.value()
            self.split_btn.setEnabled(False)
            self.cancel_split_btn.setEnabled(True)
            self.split_progress.setVisible(True)
            self.split_progress.setValue(0)
            self.split_log.clear()
            self.log_message(f"开始分割文件...", self.split_log)

            self.current_worker = WorkerThread('split', file_path, output_dir, split_method, param_value,
                                               self.file_prefix.text())
            self.current_worker.progress.connect(lambda msg: self.log_message(msg, self.split_log))
            self.current_worker.progress_detail.connect(self.log_progress_detail)
            self.current_worker.progress_percentage.connect(self.split_progress.setValue)
            self.current_worker.completed.connect(self.split_completed)
            self.current_worker.error.connect(lambda msg: self.split_error(msg))
            self.current_worker.start()
        except Exception as e:
            self.split_btn.setEnabled(True)
            self.cancel_split_btn.setEnabled(False)
            self.split_progress.setVisible(False)
            self.log_message(f"❌ 错误: {str(e)}", self.split_log, "error")
            QMessageBox.critical(self, "错误", f"分割失败:\n{str(e)}")

    def split_completed(self, result):
        elapsed, num_files, total_lines, output_files = result
        self.split_btn.setEnabled(True)
        self.cancel_split_btn.setEnabled(False)
        self.split_progress.setVisible(False)

        file_size = os.path.getsize(self.split_file_path.text())
        speed = file_size / elapsed / 1024 / 1024 if elapsed > 0 else 0

        self.log_message("=" * 60, self.split_log)
        self.log_message(f"✅ 分割完成！", self.split_log, "success")
        self.log_message("", self.split_log)
        self.log_message(f"📊 基本信息:", self.split_log)
        self.log_message(f"   • 耗时: {elapsed:.2f} 秒", self.split_log)
        self.log_message(f"   • 处理速度: {speed:.1f} MB/秒", self.split_log)
        self.log_message(f"   • 文件总行数: {total_lines:,} 行", self.split_log)
        self.log_message(f"   • 生成文件数: {num_files} 个", self.split_log)
        self.log_message("", self.split_log)
        self.log_message(f"📁 输出文件详情:", self.split_log)

        total_output_size = 0
        for i, file in enumerate(output_files, 1):
            file_size = os.path.getsize(file)
            total_output_size += file_size
            size_str = WorkerThread.format_file_size(file_size)
            self.log_message(f"   {i:3d}. {os.path.basename(file)} ({size_str})", self.split_log)

        self.log_message("", self.split_log)
        self.log_message(f"📦 输出总大小: {WorkerThread.format_file_size(total_output_size)}", self.split_log)
        self.log_message("=" * 60, self.split_log)

        message = f"分割完成！\n\n耗时: {elapsed:.2f} 秒\n生成文件: {num_files} 个\n总行数: {total_lines:,} 行"
        if speed > 0:
            message += f"\n处理速度: {speed:.1f} MB/秒"
        QMessageBox.information(self, "分割完成", message)
        self.current_worker = None

    def split_error(self, error_msg):
        self.split_btn.setEnabled(True)
        self.cancel_split_btn.setEnabled(False)
        self.split_progress.setVisible(False)
        self.log_message(f"❌ 错误: {error_msg}", self.split_log, "error")
        QMessageBox.critical(self, "错误", f"分割失败:\n{error_msg}")
        self.current_worker = None

    def cancel_split(self):
        if self.current_worker and self.current_worker.isRunning():
            reply = QMessageBox.question(self, "确认", "确定要取消当前操作吗？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                self.current_worker.cancel()
                self.log_message("操作已被用户取消", self.split_log, "warning")
                self.split_btn.setEnabled(True)
                self.cancel_split_btn.setEnabled(False)
                self.split_progress.setVisible(False)
                self.current_worker = None

    def load_extract_file_info(self, file_path):
        if not os.path.exists(file_path):
            return
        file_size = os.path.getsize(file_path)
        size_str = WorkerThread.format_file_size(file_size)
        self.extract_file_info.setText(f"大小: {size_str} | 检测编码并转换...")
        self.log_message(f"正在读取文件: {os.path.basename(file_path)}", self.extract_log)

        # 检查编码并转换
        encoding = WorkerThread.detect_encoding(file_path)
        self.log_message(f"检测到编码: {encoding}", self.extract_log)

        # 如果不是UTF-8或UTF-8-SIG，转换为UTF-8
        converted_file = file_path
        if encoding.lower() not in ['utf-8', 'utf-8-sig']:
            try:
                self.log_message(f"编码 {encoding} 不是UTF-8，正在转换为UTF-8...", self.extract_log)

                # 创建临时文件
                import tempfile
                temp_fd, temp_path = tempfile.mkstemp(suffix='_utf8.csv')
                os.close(temp_fd)

                # 转换编码
                chunk_size = 1024 * 1024 * 10  # 10MB块大小

                # 使用mmap快速读取和转换
                with open(file_path, 'rb') as f_in:
                    mm = mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ)
                    try:
                        total_size = len(mm)
                        processed = 0

                        # 使用二进制模式读取，然后按检测到的编码解码，再编码为UTF-8
                        with open(temp_path, 'wb', buffering=1024 * 1024) as f_out:
                            # 读取并转换整个文件
                            try:
                                # 尝试使用检测到的编码解码，然后编码为UTF-8
                                # 为了处理大文件，我们分块处理
                                pos = 0
                                while pos < total_size:
                                    # 读取一块数据
                                    chunk_size_bytes = min(1024 * 1024 * 10, total_size - pos)  # 10MB
                                    chunk = mm[pos:pos + chunk_size_bytes]

                                    # 找到最后一个完整的行结束位置
                                    last_newline = chunk.rfind(b'\n')
                                    if last_newline == -1 and pos + chunk_size_bytes < total_size:
                                        # 如果没有找到换行符并且不是最后一块，继续读取直到找到换行符
                                        while True:
                                            next_byte_pos = pos + chunk_size_bytes
                                            if next_byte_pos >= total_size:
                                                break
                                            next_byte = mm[next_byte_pos:next_byte_pos + 1]
                                            chunk += next_byte
                                            chunk_size_bytes += 1
                                            if next_byte == b'\n':
                                                break
                                        last_newline = len(chunk) - 1

                                    if last_newline != -1:
                                        chunk = chunk[:last_newline + 1]
                                        pos += len(chunk)
                                    else:
                                        pos += chunk_size_bytes

                                    # 转换编码
                                    try:
                                        # 先尝试用检测到的编码解码
                                        decoded = chunk.decode(encoding, errors='replace')
                                        # 再编码为UTF-8
                                        encoded = decoded.encode('utf-8')
                                        f_out.write(encoded)
                                    except:
                                        # 如果转换失败，直接写入原始数据
                                        f_out.write(chunk)

                                    processed += len(chunk)

                                    # 更新进度
                                    progress = int((processed / total_size) * 100) if total_size > 0 else 0
                                    self.extract_file_info.setText(f"大小: {size_str} | 转换编码中... {progress}%")

                                    # 每处理10%更新一次日志
                                    if processed % (total_size // 10 + 1) < chunk_size_bytes:
                                        self.log_message(f"编码转换进度: {progress}%", self.extract_log)

                            except Exception as e:
                                self.log_message(f"编码转换出错: {str(e)}", self.extract_log, "warning")
                                # 如果转换失败，使用原始文件
                                temp_path = file_path

                    finally:
                        mm.close()

                self.log_message(f"✓ 编码转换完成，保存为: {os.path.basename(temp_path)}", self.extract_log, "success")
                converted_file = temp_path
                self.extract_converted_temp_file = temp_path  # 保存临时文件路径以便后续清理

            except Exception as e:
                self.log_message(f"编码转换失败，使用原始文件: {str(e)}", self.extract_log, "warning")
                converted_file = file_path
        else:
            self.log_message(f"✓ 文件已经是UTF-8编码", self.extract_log, "success")

        self.extract_file_info.setText(f"大小: {size_str} | 正在读取列信息...")

        # 使用转换后的文件继续加载
        self.extract_btn.setEnabled(False)
        self.current_worker = WorkerThread('extract_info', converted_file)
        self.current_worker.progress.connect(lambda msg: self.log_message(msg, self.extract_log))
        self.current_worker.completed.connect(self.on_extract_file_info_loaded)
        self.current_worker.error.connect(lambda msg: self.on_extract_file_error(msg))
        self.current_worker.start()

    def clear_extract_temp_file(self):
        """清理临时转换文件"""
        if self.extract_converted_temp_file:
            try:
                if os.path.exists(self.extract_converted_temp_file):
                    os.remove(self.extract_converted_temp_file)
                    self.log_message(f"清理临时文件: {os.path.basename(self.extract_converted_temp_file)}",
                                     self.extract_log, "info")
            except:
                pass
            finally:
                self.extract_converted_temp_file = None

    def on_extract_file_info_loaded(self, result):
        self.extract_btn.setEnabled(True)
        self.current_worker = None
        if not result:
            self.log_message("❌ 无法读取文件信息", self.extract_log, "error")
            return

        # 保存原始文件路径和转换后的文件路径
        original_path = self.extract_file_path.text()
        self.extract_csv_info = result
        self.extract_csv_info['original_path'] = original_path  # 保存原始路径
        self.extract_csv_info['converted_path'] = result['path']  # 保存转换后的路径

        self.extract_column_mapping = result.get('column_mapping', {})
        self.extract_column_types = result.get('column_types', {})
        columns = result.get('columns', [])
        self.extract_column_combo.clear()
        self.extract_column_combo.addItems(columns)
        total_rows = result.get('total_rows', 0)
        file_size_mb = result.get('file_size', 0)
        self.extract_file_info.setText(f"大小: {file_size_mb:.1f} MB | 行数: {total_rows:,} | 列数: {len(columns)}")
        self.log_message(f"✓ 文件加载完成: {total_rows:,} 行, {len(columns)} 列", self.extract_log, "success")

        # 如果使用的是临时文件，记录一下
        if self.extract_converted_temp_file and self.extract_converted_temp_file != original_path:
            self.log_message(f"⚠ 注意: 当前使用的是转换后的UTF-8临时文件", self.extract_log, "warning")

        self.extract_column_combo.currentTextChanged.connect(self.on_extract_column_selected)

    def on_extract_file_error(self, error_msg):
        self.extract_btn.setEnabled(True)
        self.current_worker = None
        self.log_message(f"❌ 读取文件失败: {error_msg}", self.extract_log, "error")

    def on_extract_column_selected(self, column):
        if not column:
            self.extract_col_type_label.setText("")
            self.extract_value_input.clear()
            self.current_sample_values.clear()
            return
        col_type = self.extract_column_types.get(column, "未知")
        self.extract_col_type_label.setText(f"类型: {col_type}")
        self.extract_value_input.clear()
        self.current_sample_values.clear()
        self.log_message(f"已选择列: {column}", self.extract_log, "info")

    def select_sample_value(self):
        column = self.extract_column_combo.currentText()
        if not column:
            QMessageBox.warning(self, "提示", "请先选择列")
            return
        if not self.extract_csv_info:
            QMessageBox.warning(self, "提示", "请先加载文件")
            return
        if not self.current_sample_values:
            self.log_message(f"正在获取 '{column}' 的示例值...", self.extract_log)
            self.extract_btn.setEnabled(False)
            self.current_worker = WorkerThread('get_column_values', self.extract_csv_info['converted_path'], column, 50,
                                               self.extract_column_mapping)
            self.current_worker.progress.connect(lambda msg: self.log_message(msg, self.extract_log))
            self.current_worker.completed.connect(self.on_sample_values_loaded_for_selection)
            self.current_worker.error.connect(lambda msg: self.on_sample_values_error_for_selection(msg))
            self.current_worker.start()
        else:
            self.show_sample_selection_dialog()

    def on_sample_values_loaded_for_selection(self, values):
        self.extract_btn.setEnabled(True)
        self.current_worker = None
        if values:
            self.current_sample_values = values
            self.log_message(f"✓ 获取到 {len(values)} 个示例值", self.extract_log, "success")
            self.show_sample_selection_dialog()
        else:
            self.log_message("⚠ 未找到有效示例值", self.extract_log, "warning")
            self.current_sample_values.clear()
            QMessageBox.information(self, "提示", "该列没有找到示例值，请手动输入")

    def on_sample_values_error_for_selection(self, error_msg):
        self.extract_btn.setEnabled(True)
        self.current_worker = None
        self.log_message(f"❌ 获取示例值失败: {error_msg}", self.extract_log, "error")

    def show_sample_selection_dialog(self):
        if not self.current_sample_values:
            QMessageBox.warning(self, "提示", "没有可用的示例值")
            return
        from PySide6.QtWidgets import QInputDialog, QComboBox, QVBoxLayout, QLabel, QDialog, QPushButton
        dialog = QDialog(self)
        dialog.setWindowTitle("选择示例值")
        dialog.setModal(True)
        dialog.resize(400, 150)
        layout = QVBoxLayout(dialog)
        column = self.extract_column_combo.currentText()
        col_type = self.extract_column_types.get(column, "未知")
        info_label = QLabel(f"列: {column} (类型: {col_type})")
        info_label.setStyleSheet("font-weight: bold; color: #2196F3; margin-bottom: 10px;")
        layout.addWidget(info_label)
        combo = QComboBox()
        combo.addItems(self.current_sample_values)
        combo.setEditable(False)
        combo.setMaxVisibleItems(20)
        combo.setMinimumWidth(350)
        layout.addWidget(combo)
        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("确定")
        ok_btn.setProperty("class", "primary")
        ok_btn.clicked.connect(dialog.accept)
        btn_layout.addWidget(ok_btn)
        cancel_btn = QPushButton("取消")
        cancel_btn.setProperty("class", "secondary")
        cancel_btn.clicked.connect(dialog.reject)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
        if dialog.exec_() == QDialog.Accepted:
            selected_value = combo.currentText()
            self.extract_value_input.setText(selected_value)
            self.log_message(f"已选择值: '{selected_value}'", self.extract_log, "success")

    def clear_value_input(self):
        """清空输入值和示例值列表"""
        self.extract_value_input.clear()
        # 清空示例值列表，以便下次可以重新获取
        self.current_sample_values.clear()
        self.log_message("已清空输入值和示例值列表", self.extract_log, "info")

    def start_extract(self):
        if self.current_worker and self.current_worker.isRunning():
            QMessageBox.warning(self, "警告", "操作正在进行中，请稍候...")
            return
        if not self.extract_csv_info:
            QMessageBox.warning(self, "警告", "请先选择CSV文件！")
            return

        # 使用转换后的文件路径
        csv_path = self.extract_csv_info.get('converted_path', self.extract_csv_info.get('path'))

        column = self.extract_column_combo.currentText()
        if not column:
            QMessageBox.warning(self, "警告", "请选择要提取的列！")
            return
        value = self.extract_value_input.text().strip()
        if not value:
            col_type = self.extract_column_types.get(column, '未知')
            if 'DOUBLE' in col_type or 'FLOAT' in col_type or 'INT' in col_type or 'BIGINT' in col_type:
                if not QMessageBox.question(self, "确认", "值为空，是否提取该列为空/0值的数据？",
                                            QMessageBox.Yes | QMessageBox.No):
                    return
            else:
                if not QMessageBox.question(self, "确认", "值为空，是否提取该列为空的数据？",
                                            QMessageBox.Yes | QMessageBox.No):
                    return

        output_path = self.extract_output_path.text()

        # 如果没有指定输出路径，则使用与输入文件同目录的默认路径
        if not output_path:
            # 获取输入文件的目录和基本名
            original_path = self.extract_csv_info.get('original_path', csv_path)
            input_dir = os.path.dirname(original_path)
            input_name = os.path.splitext(os.path.basename(original_path))[0]

            # 构建默认输出文件名
            column_name = column.replace(' ', '_')[:50]  # 清理列名
            value_str = value.replace(' ', '_')[:50] if value else "empty"

            # 构建默认输出路径
            default_filename = f"{input_name}_{column_name}_{value_str}_提取.csv"
            output_path = os.path.join(input_dir, default_filename)
            self.extract_output_path.setText(output_path)

            # 记录日志
            self.log_message(f"已设置默认输出路径: {output_path}", self.extract_log, "info")

        if os.path.exists(output_path):
            reply = QMessageBox.question(self, "确认", "输出文件已存在，是否覆盖？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.No:
                return

        self.extract_btn.setEnabled(False)
        self.cancel_extract_btn.setEnabled(True)
        self.extract_progress.setVisible(True)
        self.extract_progress.setValue(0)
        self.extract_progress.setStyleSheet("""
            QProgressBar { border: 1px solid #d0d0d0; border-radius: 4px; background-color: #ffffff; text-align: center; }
            QProgressBar::chunk { background-color: #2196F3; border-radius: 3px; transition: width 0.3s ease; }
        """)
        self.extract_log.clear()
        self.log_message(f"开始提取数据: {column} = '{value}'", self.extract_log)

        # 使用转换后的文件路径
        self.current_worker = WorkerThread('extract', csv_path, column, value, output_path,
                                           self.extract_column_mapping, self.extract_column_types)
        self.current_worker.progress.connect(lambda msg: self.log_message(msg, self.extract_log))
        self.current_worker.progress_percentage.connect(self.extract_progress.setValue)
        self.current_worker.completed.connect(self.extract_completed)
        self.current_worker.error.connect(lambda msg: self.extract_error(msg))
        self.current_worker.start()

    def extract_completed(self, result):
        total_rows, query_time, output_path, query, clean_value = result
        self.extract_btn.setEnabled(True)
        self.cancel_extract_btn.setEnabled(False)
        self.extract_progress.setVisible(False)
        column = self.extract_column_combo.currentText()
        col_type = self.extract_column_types.get(column, "未知")
        self.log_message("=" * 60, self.extract_log)
        self.log_message(f"✅ 提取完成！", self.extract_log, "success")
        self.log_message("", self.extract_log)
        self.log_message(f"📊 基本信息:", self.extract_log)
        self.log_message(f"   • 列名: {column} (类型: {col_type})", self.extract_log)
        self.log_message(f"   • 值: '{clean_value}'", self.extract_log)
        self.log_message(f"   • 匹配行数: {total_rows:,} 行", self.extract_log)
        self.log_message(f"   • 耗时: {query_time:.2f} 秒", self.extract_log)
        if output_path:
            file_size = os.path.getsize(output_path) / (1024 * 1024)
            self.log_message(f"   • 输出文件: {os.path.basename(output_path)}", self.extract_log)
            self.log_message(f"   • 文件大小: {file_size:.1f} MB", self.extract_log)
        self.log_message("", self.extract_log)
        self.log_message(f"📋 执行查询:", self.extract_log)
        self.log_message(f"   {query}", self.extract_log)
        self.log_message("", self.extract_log)
        self.log_message("=" * 60, self.extract_log)
        if total_rows == 0:
            QMessageBox.information(self, "提取完成", "没有找到匹配的数据")
        else:
            message = f"提取完成！\n\n匹配行数: {total_rows:,} 行\n耗时: {query_time:.2f} 秒"
            if output_path:
                message += f"\n输出文件: {os.path.basename(output_path)}"
            QMessageBox.information(self, "提取完成", message)
        self.current_worker = None

    def extract_error(self, error_msg):
        self.extract_btn.setEnabled(True)
        self.cancel_extract_btn.setEnabled(False)
        self.extract_progress.setVisible(False)
        self.log_message(f"❌ 错误: {error_msg}", self.extract_log, "error")
        QMessageBox.critical(self, "错误", f"提取失败:\n{error_msg}")
        self.current_worker = None

    def cancel_extract(self):
        if self.current_worker and self.current_worker.isRunning():
            reply = QMessageBox.question(self, "确认", "确定要取消当前操作吗？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                self.current_worker.cancel()
                self.log_message("操作已被用户取消", self.extract_log, "warning")
                self.extract_btn.setEnabled(True)
                self.cancel_extract_btn.setEnabled(False)
                self.extract_progress.setVisible(False)
                self.current_worker = None

    def closeEvent(self, event):
        # 清理临时转换文件
        self.clear_extract_temp_file()

        if self.current_worker and self.current_worker.isRunning():
            reply = QMessageBox.question(self, "确认", "有任务正在运行，确定要退出吗？", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                self.current_worker.cancel()
                self.current_worker.wait(2000)
            else:
                event.ignore()
                return
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName("CSV工具箱 Pro")
    # 设置应用程序ID（Windows任务栏唯一标识）
    if sys.platform == "win32":
        # 在Windows上设置唯一的应用程序ID

        import ctypes
        try:
            # 尝试设置应用程序ID（需要Windows 7+）
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("fy.csvtools.pro.v32")
        except:
            pass

    window = ModernCSVTools()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()