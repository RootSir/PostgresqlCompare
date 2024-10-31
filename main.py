import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import psycopg2
from typing import Dict, List, Tuple
import json, configparser, os, sys, threading
from queue import Queue, Empty
import base64
import time

class SchemaComparator:
    def __init__(self, db1_params: Dict, db2_params: Dict):
        """初始化数据库连接"""
        self.db1_conn = psycopg2.connect(**db1_params)
        self.db2_conn = psycopg2.connect(**db2_params)
        
    def get_tables_structure(self, conn, schema_name: str, queue: Queue = None, db_label: str = "") -> Dict:
        """获取指定schema下所有表的结构"""
        try:
            cursor = conn.cursor()
            
            if queue:
                queue.put(("status", f"正在获取{db_label}的表结构..."))
            
            # 一次性查询所有表的列信息
            cursor.execute("""
                SELECT 
                    table_name,
                    column_name,
                    is_nullable,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    datetime_precision,
                    udt_name,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = %s
                ORDER BY table_name, column_name
            """, (schema_name,))
            
            columns = cursor.fetchall()
            total_tables = len(set(col[0] for col in columns))
            current_table = None
            table_count = 0
            
            # 组织数据结构
            tables_structure = {}
            for row in columns:
                table_name = row[0]
                if table_name != current_table:
                    current_table = table_name
                    table_count += 1
                    if queue:
                        queue.put(("progress", f"{db_label}进度: {table_count}/{total_tables} ({table_name})"))
                
                if table_name not in tables_structure:
                    tables_structure[table_name] = {
                        'columns': [],
                        'primary_keys': [],
                        'indexes': [],
                        'foreign_keys': []
                    }
                tables_structure[table_name]['columns'].append(row[1:])

            # 获取主键信息
            cursor.execute("""
                SELECT 
                    tc.table_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                ORDER BY tc.table_name, kcu.ordinal_position;
            """, (schema_name,))
            
            for table_name, column_name in cursor.fetchall():
                if table_name in tables_structure:
                    tables_structure[table_name]['primary_keys'].append(column_name)

            # 获取索引信息
            cursor.execute("""
                SELECT
                    t.relname as table_name,
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique
                FROM
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a,
                    pg_namespace n
                WHERE
                    t.oid = ix.indrelid
                    and i.oid = ix.indexrelid
                    and a.attrelid = t.oid
                    and a.attnum = ANY(ix.indkey)
                    and t.relnamespace = n.oid
                    and n.nspname = %s
                ORDER BY
                    t.relname,
                    i.relname;
            """, (schema_name,))
            
            for table_name, index_name, column_name, is_unique in cursor.fetchall():
                if table_name in tables_structure:
                    tables_structure[table_name]['indexes'].append((index_name, column_name, is_unique))

            # 获取外键信息
            cursor.execute("""
                SELECT
                    kcu.table_name,
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = %s
                ORDER BY kcu.table_name, tc.constraint_name;
            """, (schema_name,))
            
            for table_name, constraint_name, column_name, foreign_table, foreign_column in cursor.fetchall():
                if table_name in tables_structure:
                    tables_structure[table_name]['foreign_keys'].append(
                        (constraint_name, column_name, foreign_table, foreign_column)
                    )

            cursor.close()
            return tables_structure
                
        except Exception as e:
            if queue:
                queue.put(("error", f"{db_label}查询出错: {str(e)}"))
            raise
    def compare_table_structure(self, table1: Dict, table2: Dict, table_name: str) -> Dict:
        """比较单个表的结构差异"""
        differences = {}
        
        # 比较列
        columns1 = {col[0]: col for col in table1['columns']}
        columns2 = {col[0]: col for col in table2['columns']}
        
        # 检查缺失的列
        missing_in_db1 = [col for col in columns2.keys() if col not in columns1]
        missing_in_db2 = [col for col in columns1.keys() if col not in columns2]
        
        # 检查列定义差异
        column_diffs = {}
        for col_name in set(columns1.keys()) & set(columns2.keys()):
            if columns1[col_name] != columns2[col_name]:
                column_diffs[col_name] = {
                    'db1': dict(zip(
                        ['name', 'nullable', 'type', 'max_length', 'numeric_precision', 
                         'numeric_scale', 'datetime_precision', 'udt_name', 'position'],
                        columns1[col_name]
                    )),
                    'db2': dict(zip(
                        ['name', 'nullable', 'type', 'max_length', 'numeric_precision', 
                         'numeric_scale', 'datetime_precision', 'udt_name', 'position'],
                        columns2[col_name]
                    ))
                }
        
        if missing_in_db1 or missing_in_db2 or column_diffs:
            differences['columns'] = {
                'missing_in_db1': missing_in_db1,
                'missing_in_db2': missing_in_db2,
                'differences': column_diffs
            }
            
        # 比较主键
        if table1['primary_keys'] != table2['primary_keys']:
            differences['primary_keys'] = {
                'db1': table1['primary_keys'],
                'db2': table2['primary_keys']
            }
            
        # 比较索引
        indexes1 = {idx[0]: idx for idx in table1['indexes']}
        indexes2 = {idx[0]: idx for idx in table2['indexes']}
        
        missing_indexes_db1 = [idx for idx in indexes2.keys() if idx not in indexes1]
        missing_indexes_db2 = [idx for idx in indexes1.keys() if idx not in indexes2]
        
        if missing_indexes_db1 or missing_indexes_db2:
            differences['indexes'] = {
                'missing_in_db1': missing_indexes_db1,
                'missing_in_db2': missing_indexes_db2
            }
            
        # 比较外键
        fk1 = {fk[0]: fk for fk in table1['foreign_keys']}
        fk2 = {fk[0]: fk for fk in table2['foreign_keys']}
        
        missing_fk_db1 = [fk for fk in fk2.keys() if fk not in fk1]
        missing_fk_db2 = [fk for fk in fk1.keys() if fk not in fk2]
        
        if missing_fk_db1 or missing_fk_db2:
            differences['foreign_keys'] = {
                'missing_in_db1': missing_fk_db1,
                'missing_in_db2': missing_fk_db2
            }
            
        return differences if differences else None

    def compare_schemas(self, schema_name: str, queue: Queue = None, db1_label: str = "", db2_label: str = "") -> Dict:
        try:
            # 创建两个队列用于接收各自的进度
            db1_queue = Queue()
            db2_queue = Queue()
            
            # 创建两个线程分别查询两个数据库
            def get_db1_structure():
                try:
                    result = self.get_tables_structure(self.db1_conn, schema_name, db1_queue, db1_label)
                    db1_queue.put(("complete", result))
                except Exception as e:
                    db1_queue.put(("error", str(e)))
                    
            def get_db2_structure():
                try:
                    result = self.get_tables_structure(self.db2_conn, schema_name, db2_queue, db2_label)
                    db2_queue.put(("complete", result))
                except Exception as e:
                    db2_queue.put(("error", str(e)))
            
            thread1 = threading.Thread(target=get_db1_structure)
            thread2 = threading.Thread(target=get_db2_structure)
            
            thread1.start()
            thread2.start()
            
            # 等待两个线程完成
            db1_result = None
            db2_result = None
            
            while True:
                # 检查是否有错误
                if not db1_queue.empty():
                    status, data = db1_queue.get()
                    if status == "error":
                        raise Exception(f"数据库1错误: {data}")
                    elif status in ["status", "progress"]:
                        if queue:
                            queue.put((status, data))
                    elif status == "complete":
                        db1_result = data
                        
                if not db2_queue.empty():
                    status, data = db2_queue.get()
                    if status == "error":
                        raise Exception(f"数据库2错误: {data}")
                    elif status in ["status", "progress"]:
                        if queue:
                            queue.put((status, data))
                    elif status == "complete":
                        db2_result = data
                
                if db1_result is not None and db2_result is not None:
                    break
                    
                time.sleep(0.1)
            
            if queue:
                queue.put(("status", "正在比较差异..."))
            
            differences = {
                'missing_in_db1': [],
                'missing_in_db2': [],
                'structure_diff': {}
            }
            
            # 比较表是否存在
            db1_table_names = set(db1_result.keys())
            db2_table_names = set(db2_result.keys())
            
            differences['missing_in_db1'] = list(db2_table_names - db1_table_names)
            differences['missing_in_db2'] = list(db1_table_names - db2_table_names)
            
            # 比较共同表的结构
            common_tables = db1_table_names & db2_table_names
            total_tables = len(common_tables)
            for i, table_name in enumerate(sorted(common_tables), 1):
                if queue:
                    queue.put(("progress", f"比较进度: {i}/{total_tables} ({table_name})"))
                table_diff = self.compare_table_structure(
                    db1_result[table_name],
                    db2_result[table_name],
                    table_name
                )
                if table_diff:
                    differences['structure_diff'][table_name] = table_diff
                    
            return differences
            
        finally:
            self.close()
            
    def close(self):
        """关闭数据库连接"""
        self.db1_conn.close()
        self.db2_conn.close()
class DBCompareGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("数据库结构比较工具")
        self.root.geometry("1000x800")
        
        try:
            if getattr(sys, 'frozen', False):
                current_dir = os.path.dirname(sys.executable)
            else:
                current_dir = os.path.dirname(os.path.abspath(__file__))
            config_dir = os.path.join(current_dir, 'config')
            if not os.path.exists(config_dir):
                os.makedirs(config_dir)
            self.config_file = os.path.join(config_dir, "db_compare_config.ini")
        except Exception as e:
            print(f"创建配置目录失败：{str(e)}")
            self.config_file = os.path.join(os.path.expanduser("~"), "db_compare_config.ini")
        
        self.result_queue = Queue()
        self.load_config()
        self.create_gui()

    def load_config(self):
        self.config = configparser.ConfigParser()
        default_config = {
            'DB1': {
                'host': 'localhost',
                'port': '5432',
                'dbname': '',
                'user': '',
                'password': '',
                'schema': 'public',
                'label': '数据库1'
            },
            'DB2': {
                'host': 'localhost',
                'port': '5432',
                'dbname': '',
                'user': '',
                'password': '',
                'schema': 'public',
                'label': '数据库2'
            }
        }
        
        try:
            if os.path.exists(self.config_file):
                self.config.read(self.config_file, encoding='utf-8')
                print(f"已加载配置文件: {os.path.abspath(self.config_file)}")
                
                # 解码密码
                if self.config.has_section('DB1') and 'password' in self.config['DB1']:
                    try:
                        encoded_pwd = self.config['DB1']['password']
                        padding = len(encoded_pwd) % 4
                        if padding:
                            encoded_pwd += '=' * (4 - padding)
                        pwd = base64.b64decode(encoded_pwd.encode()).decode()
                        self.config['DB1']['password'] = pwd
                    except:
                        self.config['DB1']['password'] = ''
                        
                if self.config.has_section('DB2') and 'password' in self.config['DB2']:
                    try:
                        encoded_pwd = self.config['DB2']['password']
                        padding = len(encoded_pwd) % 4
                        if padding:
                            encoded_pwd += '=' * (4 - padding)
                        pwd = base64.b64decode(encoded_pwd.encode()).decode()
                        self.config['DB2']['password'] = pwd
                    except:
                        self.config['DB2']['password'] = ''
            else:
                for section, values in default_config.items():
                    if not self.config.has_section(section):
                        self.config.add_section(section)
                    for key, value in values.items():
                        self.config[section][key] = value
                print("使用默认配置")
        except Exception as e:
            print(f"加载配置文件失败：{str(e)}")
            for section, values in default_config.items():
                if not self.config.has_section(section):
                    self.config.add_section(section)
                for key, value in values.items():
                    self.config[section][key] = value

    def create_gui(self):
        config_frame = ttk.LabelFrame(self.root, text="数据库连接配置", padding="5")
        config_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 创建第一个数据库配置框架
        db1_frame = ttk.LabelFrame(config_frame, text="数据库1配置", padding="5")
        db1_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(db1_frame, text="标识:").grid(row=0, column=0, sticky=tk.W)
        self.db1_label_entry = ttk.Entry(db1_frame)
        self.db1_label_entry.grid(row=0, column=1)
        self.db1_label_entry.insert(0, self.config.get('DB1', 'label', fallback='数据库1'))
        
        ttk.Label(db1_frame, text="主机:").grid(row=1, column=0, sticky=tk.W)
        self.db1_host_entry = ttk.Entry(db1_frame)
        self.db1_host_entry.grid(row=1, column=1)
        self.db1_host_entry.insert(0, self.config.get('DB1', 'host', fallback='localhost'))
        
        ttk.Label(db1_frame, text="端口:").grid(row=1, column=2, sticky=tk.W)
        self.db1_port_entry = ttk.Entry(db1_frame, width=10)
        self.db1_port_entry.grid(row=1, column=3)
        self.db1_port_entry.insert(0, self.config.get('DB1', 'port', fallback='5432'))
        
        ttk.Label(db1_frame, text="数据库名:").grid(row=2, column=0, sticky=tk.W)
        self.db1_name_entry = ttk.Entry(db1_frame)
        self.db1_name_entry.grid(row=2, column=1)
        self.db1_name_entry.insert(0, self.config.get('DB1', 'dbname', fallback=''))
        
        ttk.Label(db1_frame, text="用户名:").grid(row=2, column=2, sticky=tk.W)
        self.db1_user_entry = ttk.Entry(db1_frame)
        self.db1_user_entry.grid(row=2, column=3)
        self.db1_user_entry.insert(0, self.config.get('DB1', 'user', fallback=''))
        
        ttk.Label(db1_frame, text="密码:").grid(row=3, column=0, sticky=tk.W)
        self.db1_pwd_entry = ttk.Entry(db1_frame, show="*")
        self.db1_pwd_entry.grid(row=3, column=1)
        self.db1_pwd_entry.insert(0, self.config.get('DB1', 'password', fallback=''))
        
        # 创建第二个数据库配置框架
        db2_frame = ttk.LabelFrame(config_frame, text="数据库2配置", padding="5")
        db2_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(db2_frame, text="标识:").grid(row=0, column=0, sticky=tk.W)
        self.db2_label_entry = ttk.Entry(db2_frame)
        self.db2_label_entry.grid(row=0, column=1)
        self.db2_label_entry.insert(0, self.config.get('DB2', 'label', fallback='数据库2'))
        
        ttk.Label(db2_frame, text="主机:").grid(row=1, column=0, sticky=tk.W)
        self.db2_host_entry = ttk.Entry(db2_frame)
        self.db2_host_entry.grid(row=1, column=1)
        self.db2_host_entry.insert(0, self.config.get('DB2', 'host', fallback='localhost'))
        
        ttk.Label(db2_frame, text="端口:").grid(row=1, column=2, sticky=tk.W)
        self.db2_port_entry = ttk.Entry(db2_frame, width=10)
        self.db2_port_entry.grid(row=1, column=3)
        self.db2_port_entry.insert(0, self.config.get('DB2', 'port', fallback='5432'))
        
        ttk.Label(db2_frame, text="数据库名:").grid(row=2, column=0, sticky=tk.W)
        self.db2_name_entry = ttk.Entry(db2_frame)
        self.db2_name_entry.grid(row=2, column=1)
        self.db2_name_entry.insert(0, self.config.get('DB2', 'dbname', fallback=''))
        
        ttk.Label(db2_frame, text="用户名:").grid(row=2, column=2, sticky=tk.W)
        self.db2_user_entry = ttk.Entry(db2_frame)
        self.db2_user_entry.grid(row=2, column=3)
        self.db2_user_entry.insert(0, self.config.get('DB2', 'user', fallback=''))
        
        ttk.Label(db2_frame, text="密码:").grid(row=3, column=0, sticky=tk.W)
        self.db2_pwd_entry = ttk.Entry(db2_frame, show="*")
        self.db2_pwd_entry.grid(row=3, column=1)
        self.db2_pwd_entry.insert(0, self.config.get('DB2', 'password', fallback=''))
        
        # Schema配置
        schema_frame = ttk.Frame(config_frame)
        schema_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(schema_frame, text="Schema:").pack(side=tk.LEFT)
        self.schema_entry = ttk.Entry(schema_frame, width=30)
        self.schema_entry.pack(side=tk.LEFT, padx=5)
        self.schema_entry.insert(0, self.config.get('DB1', 'schema', fallback='public'))
        
        # 按钮区域
        button_frame = ttk.Frame(self.root)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(button_frame, text="测试连接", command=self.test_connection).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="开始比较", command=self.compare_schemas).pack(side=tk.LEFT, padx=5)
        
        # 进度显示区域
        progress_frame = ttk.Frame(self.root)
        progress_frame.pack(fill=tk.X, padx=5)
        
        self.db1_progress_var = tk.StringVar(value="")
        self.db1_progress_label = ttk.Label(progress_frame, textvariable=self.db1_progress_var, width=40)
        self.db1_progress_label.pack(side=tk.LEFT, padx=5)
        
        self.db2_progress_var = tk.StringVar(value="")
        self.db2_progress_label = ttk.Label(progress_frame, textvariable=self.db2_progress_var, width=40)
        self.db2_progress_label.pack(side=tk.LEFT, padx=5)
        
        self.progress_var = tk.StringVar(value="就绪")
        self.progress_label = ttk.Label(progress_frame, textvariable=self.progress_var)
        self.progress_label.pack(side=tk.LEFT, padx=5)
        
        # 结果显示区域
        result_frame = ttk.LabelFrame(self.root, text="比较结果", padding="5")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.result_text = scrolledtext.ScrolledText(result_frame, wrap=tk.WORD)
        self.result_text.pack(fill=tk.BOTH, expand=True)

    def save_config(self):
        try:
            if not self.config.has_section('DB1'):
                self.config.add_section('DB1')
            if not self.config.has_section('DB2'):
                self.config.add_section('DB2')

            # 对密码进行 Base64 编码
            db1_pwd = base64.b64encode(self.db1_pwd_entry.get().encode()).decode()
            db2_pwd = base64.b64encode(self.db2_pwd_entry.get().encode()).decode()

            self.config['DB1'].update({
                'host': self.db1_host_entry.get(),
                'port': self.db1_port_entry.get(),
                'dbname': self.db1_name_entry.get(),
                'user': self.db1_user_entry.get(),
                'password': db1_pwd,
                'schema': self.schema_entry.get(),
                'label': self.db1_label_entry.get()
            })
            
            self.config['DB2'].update({
                'host': self.db2_host_entry.get(),
                'port': self.db2_port_entry.get(),
                'dbname': self.db2_name_entry.get(),
                'user': self.db2_user_entry.get(),
                'password': db2_pwd,
                'schema': self.schema_entry.get(),
                'label': self.db2_label_entry.get()
            })
            
            config_dir = os.path.dirname(self.config_file)
            if not os.path.exists(config_dir):
                os.makedirs(config_dir)
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                self.config.write(f)
                
            print(f"配置已成功保存到: {os.path.abspath(self.config_file)}")
            
        except Exception as e:
            error_msg = f"保存配置文件失败：{str(e)}"
            print(error_msg)
            messagebox.showerror("错误", error_msg)

    def get_db_params(self, db_num: int) -> Dict:
        if db_num == 1:
            return {
                'host': self.db1_host_entry.get(),
                'port': self.db1_port_entry.get(),
                'dbname': self.db1_name_entry.get(),
                'user': self.db1_user_entry.get(),
                'password': self.db1_pwd_entry.get()
            }
        else:
            return {
                'host': self.db2_host_entry.get(),
                'port': self.db2_port_entry.get(),
                'dbname': self.db2_name_entry.get(),
                'user': self.db2_user_entry.get(),
                'password': self.db2_pwd_entry.get()
            }

    def test_connection(self):
        try:
            conn1 = psycopg2.connect(**self.get_db_params(1))
            conn1.close()
            conn2 = psycopg2.connect(**self.get_db_params(2))
            conn2.close()
            self.save_config()
            messagebox.showinfo("成功", "两个数据库连接测试成功！")
        except Exception as e:
            messagebox.showerror("错误", f"连接测试失败：{str(e)}")

    def compare_schemas(self):
        try:
            self.save_config()
            self.disable_buttons()
            self.progress_var.set("正在比较...")
            self.db1_progress_var.set("")
            self.db2_progress_var.set("")
            thread = threading.Thread(target=self._compare_schemas_thread)
            thread.daemon = True
            thread.start()
            self.root.after(100, self._check_comparison_result)
        except Exception as e:
            self.enable_buttons()
            self.progress_var.set("就绪")
            messagebox.showerror("错误", f"比较失败：{str(e)}")

    def _compare_schemas_thread(self):
        try:
            db1_label = self.db1_label_entry.get() or "数据库1"
            db2_label = self.db2_label_entry.get() or "数据库2"
            
            comparator = SchemaComparator(
                self.get_db_params(1),
                self.get_db_params(2)
            )
            schema_name = self.schema_entry.get()
            
            progress_queue = Queue()
            
            def process_queue():
                try:
                    while not progress_queue.empty():
                        status, data = progress_queue.get_nowait()
                        if status == "status":
                            self.progress_var.set(data)
                        elif status == "progress":
                            if data.startswith(db1_label):
                                self.db1_progress_var.set(data)
                            elif data.startswith(db2_label):
                                self.db2_progress_var.set(data)
                        elif status == "error":
                            raise Exception(data)
                except Empty:
                    pass
                self.root.after(100, process_queue)
            
            self.root.after(100, process_queue)
            
            differences = comparator.compare_schemas(
                schema_name, 
                progress_queue,
                db1_label,
                db2_label
            )
            
            self.result_queue.put(("success", differences))
            
        except Exception as e:
            self.result_queue.put(("error", str(e)))

    def _check_comparison_result(self):
        try:
            if not self.result_queue.empty():
                status, result = self.result_queue.get_nowait()
                if status == "success":
                    self.display_results(result)
                    self.progress_var.set("比较完成")
                else:
                    messagebox.showerror("错误", f"比较失败：{result}")
                    self.progress_var.set("就绪")
                self.enable_buttons()
            else:
                self.root.after(100, self._check_comparison_result)
        except Exception as e:
            self.enable_buttons()
            self.progress_var.set("就绪")
            messagebox.showerror("错误", f"处理结果失败：{str(e)}")

    def disable_buttons(self):
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Frame):
                for button in child.winfo_children():
                    if isinstance(button, ttk.Button):
                        button.configure(state='disabled')
                    
    def enable_buttons(self):
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Frame):
                for button in child.winfo_children():
                    if isinstance(button, ttk.Button):
                        button.configure(state='normal')

    def display_results(self, differences: Dict):
        self.result_text.delete(1.0, tk.END)
        db1_label = self.db1_label_entry.get() or "数据库1"
        db2_label = self.db2_label_entry.get() or "数据库2"
        
        if differences['missing_in_db1']:
            self.result_text.insert(tk.END, f"{db1_label}中缺失的表:\n")
            for table in sorted(differences['missing_in_db1']):
                self.result_text.insert(tk.END, f"  - {table}\n")
            self.result_text.insert(tk.END, "\n")
            
        if differences['missing_in_db2']:
            self.result_text.insert(tk.END, f"{db2_label}中缺失的表:\n")
            for table in sorted(differences['missing_in_db2']):
                self.result_text.insert(tk.END, f"  - {table}\n")
            self.result_text.insert(tk.END, "\n")
            
        if differences['structure_diff']:
            self.result_text.insert(tk.END, "表结构差异:\n")
            for table_name in sorted(differences['structure_diff'].keys()):
                diff = differences['structure_diff'][table_name]
                self.result_text.insert(tk.END, f"\n表 {table_name}:\n")
                
                if 'columns' in diff:
                    if diff['columns']['missing_in_db1']:
                        self.result_text.insert(tk.END, f"  {db1_label}中缺失的列:\n")
                        for col in sorted(diff['columns']['missing_in_db1']):
                            self.result_text.insert(tk.END, f"    - {col}\n")
                            
                    if diff['columns']['missing_in_db2']:
                        self.result_text.insert(tk.END, f"  {db2_label}中缺失的列:\n")
                        for col in sorted(diff['columns']['missing_in_db2']):
                            self.result_text.insert(tk.END, f"    - {col}\n")
                            
                    if diff['columns']['differences']:
                        self.result_text.insert(tk.END, "  列定义差异:\n")
                        for col_name in sorted(diff['columns']['differences'].keys()):
                            col_diff = diff['columns']['differences'][col_name]
                            db1_info = col_diff['db1']
                            db2_info = col_diff['db2']
                            
                            self.result_text.insert(tk.END, f"    {col_name}:\n")
                            differences_found = []
                            
                            if db1_info['type'] != db2_info['type']:
                                differences_found.append(f"数据类型: {db1_info['type']} -> {db2_info['type']}")
                            if db1_info['max_length'] != db2_info['max_length']:
                                differences_found.append(f"最大长度: {db1_info['max_length']} -> {db2_info['max_length']}")
                            if db1_info['numeric_precision'] != db2_info['numeric_precision']:
                                differences_found.append(f"精度: {db1_info['numeric_precision']} -> {db2_info['numeric_precision']}")
                            if db1_info['numeric_scale'] != db2_info['numeric_scale']:
                                differences_found.append(f"小数位: {db1_info['numeric_scale']} -> {db2_info['numeric_scale']}")
                            if db1_info['datetime_precision'] != db2_info['datetime_precision']:
                                differences_found.append(f"时间精度: {db1_info['datetime_precision']} -> {db2_info['datetime_precision']}")
                            if db1_info['nullable'] != db2_info['nullable']:
                                differences_found.append(f"可空性: {db1_info['nullable']} -> {db2_info['nullable']}")
                            if db1_info['position'] != db2_info['position']:
                                differences_found.append(f"位置: {db1_info['position']} -> {db2_info['position']}")
                                
                            for diff_desc in differences_found:
                                self.result_text.insert(tk.END, f"      - {diff_desc}\n")
                
                if 'primary_keys' in diff:
                    self.result_text.insert(tk.END, "  主键差异:\n")
                    self.result_text.insert(tk.END, f"    {db1_label}: {', '.join(diff['primary_keys']['db1']) or '无'}\n")
                    self.result_text.insert(tk.END, f"    {db2_label}: {', '.join(diff['primary_keys']['db2']) or '无'}\n")
                    
                if 'indexes' in diff:
                    if diff['indexes']['missing_in_db1']:
                        self.result_text.insert(tk.END, f"  {db1_label}中缺失的索引:\n")
                        for idx in sorted(diff['indexes']['missing_in_db1']):
                            self.result_text.insert(tk.END, f"    - {idx}\n")
                            
                    if diff['indexes']['missing_in_db2']:
                        self.result_text.insert(tk.END, f"  {db2_label}中缺失的索引:\n")
                        for idx in sorted(diff['indexes']['missing_in_db2']):
                            self.result_text.insert(tk.END, f"    - {idx}\n")
                            
                if 'foreign_keys' in diff:
                    if diff['foreign_keys']['missing_in_db1']:
                        self.result_text.insert(tk.END, f"  {db1_label}中缺失的外键:\n")
                        for fk in sorted(diff['foreign_keys']['missing_in_db1']):
                            self.result_text.insert(tk.END, f"    - {fk}\n")
                            
                    if diff['foreign_keys']['missing_in_db2']:
                        self.result_text.insert(tk.END, f"  {db2_label}中缺失的外键:\n")
                        for fk in sorted(diff['foreign_keys']['missing_in_db2']):
                            self.result_text.insert(tk.END, f"    - {fk}\n")
        
        if not any(differences.values()):
            self.result_text.insert(tk.END, "未发现差异！")

if __name__ == "__main__":
    root = tk.Tk()
    app = DBCompareGUI(root)
    root.mainloop()
