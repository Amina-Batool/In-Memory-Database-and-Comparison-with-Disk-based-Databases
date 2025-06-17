import pickle
import time
import zlib
import sqlite3
import ast
import matplotlib.pyplot as plt
from openpyxl import Workbook
import os
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog
import json
from pathlib import Path
import ast

# --- Configuration ---
DATA_DIR = "data" 
DISK_DB_FILE = "disk_database.db"
MEMORY_DB = {}
INDEX_STORE = {}  
PRIMARY_KEYS = { 
    "teachers": "TeacherID",
    "students": "StudentID",
    "departments": "DeptID",
    "courses": "CourseID"
}

# --- Ensure Data Directory Exists ---
Path(DATA_DIR).mkdir(exist_ok=True)

def initialize_data_files():
    default_data = {
        "teachers.json": [
            {"TeacherID": 1, "TeacherName": "Dr. Smith", "DeptID": 1},
            {"TeacherID": 2, "TeacherName": "Dr. Brown", "DeptID": 2}
        ],
        "students.json": [
            {"StudentID": 101, "StudentName": "Alice", "TeacherID": 1},
            {"StudentID": 102, "StudentName": "Bob", "TeacherID": 1},
            {"StudentID": 103, "StudentName": "Charlie", "TeacherID": 2}
        ],
        "departments.json": [
            {"DeptID": 1, "DeptName": "Computer Science"},
            {"DeptID": 2, "DeptName": "Physics"}
        ],
        "courses.json": [
            {"CourseID": 501, "CourseName": "Data Structures", "DeptID": 1},
            {"CourseID": 502, "CourseName": "Quantum Mechanics", "DeptID": 2}
        ]
    }
    
    for filename, data in default_data.items():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)

def load_data_to_memory():
    try:
        print(f"🔍 Looking for data files in {DATA_DIR}...")
        files = os.listdir(DATA_DIR)
        json_files = [f for f in files if f.endswith('.json')]
        
        if not json_files:
            print("⚠ No JSON files found in data directory")
            return
            
        print(f"Found {len(json_files)} JSON files")
        
        for filename in json_files:
            table_name = filename[:-5] 
            filepath = os.path.join(DATA_DIR, filename)
            
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    MEMORY_DB[table_name] = {i: record for i, record in enumerate(data)}
                print(f"✔ Loaded {len(data)} records into '{table_name}'")
            except Exception as e:
                print(f"❌ Failed to load {filename}: {str(e)}")
                
    except Exception as e:
        print(f"❌ Error loading data to memory: {str(e)}")

def compress_data(data):
    return zlib.compress(pickle.dumps(data))

def decompress_data(compressed_data):
    if isinstance(compressed_data, dict):
        return compressed_data
    return pickle.loads(zlib.decompress(compressed_data))

# --- Modified Memory DB Storage ---
MEMORY_DB = {}  
MEMORY_DB_RAW_SIZE = 0  
MEMORY_DB_COMPRESSED_SIZE = 0 

# --- Store Data in Memory with Secondary Indexes ---
def store_data_memory(table_name, data_dict):
    global MEMORY_DB_RAW_SIZE, MEMORY_DB_COMPRESSED_SIZE

    if not data_dict:
        return
    
    primary_key = PRIMARY_KEYS.get(table_name)
    if not primary_key:
        raise Exception(f"No primary key defined for {table_name}")
    
    raw_size = len(pickle.dumps(data_dict))
    compressed_data = compress_data(data_dict)
    compressed_size = len(compressed_data)

    MEMORY_DB[table_name] = compressed_data
    MEMORY_DB_RAW_SIZE += raw_size
    MEMORY_DB_COMPRESSED_SIZE += compressed_size

    # Primary key index
    primary_index = {record[primary_key]: key for key, record in data_dict.items()}

    # Secondary indexes (for all fields)
    secondary_indexes = {}
    for key, record in data_dict.items():
        for field, value in record.items():
            if field == primary_key:
                continue  
            if field not in secondary_indexes:
                secondary_indexes[field] = {}
            if value not in secondary_indexes[field]:
                secondary_indexes[field][value] = []
            secondary_indexes[field][value].append(key)

    INDEX_STORE[table_name] = {
        "primary": primary_index,
        "secondary": secondary_indexes
    }

    save_memory_to_file(table_name, data_dict)
    store_data_disk(table_name, data_dict)


# --- Modified save_memory_to_file to accept data ---
def save_memory_to_file(table_name, data=None):
    if data is None:
        data = retrieve_data_memory(table_name)
        
    if table_name in MEMORY_DB:
        filepath = os.path.join(DATA_DIR, f"{table_name}.json")
        data_list = list(data.values()) if isinstance(data, dict) else data
        with open(filepath, 'w') as f:
            json.dump(data_list, f, indent=2)

# --- Store Data in Memory with Index ---
def store_data_memory(table_name, data_dict):
    if not data_dict:
        return
    
    primary_key = PRIMARY_KEYS.get(table_name)
    if not primary_key:
        raise Exception(f"No primary key defined for {table_name}")
    
    compressed_data = compress_data(data_dict)
    MEMORY_DB[table_name] = compressed_data
    
    # Build index
    INDEX_STORE[table_name] = {record[primary_key]: key for key, record in data_dict.items()}
    
    save_memory_to_file(table_name, data_dict)
    store_data_disk(table_name, data_dict)

# --- Retrieve Full Table ---
def retrieve_data_memory(table_name):
    if table_name not in MEMORY_DB:
        return {}
    
    compressed_data = MEMORY_DB[table_name]
    return decompress_data(compressed_data)

# --- Fast Retrieve by Primary Key ---
def get_record_by_primary_key(table_name, primary_key_value):
    if table_name not in MEMORY_DB:
        return None
    
    index = INDEX_STORE.get(table_name, {}).get("primary", {})
    if primary_key_value not in index:
        return None
    
    data = retrieve_data_memory(table_name)
    return data.get(index[primary_key_value])
# --- Disk Operations (SQLite) ---
def store_data_disk(table_name, data_dict):
    conn = None
    try:
        conn = sqlite3.connect(DISK_DB_FILE)
        cursor = conn.cursor()
        
        if not data_dict:
            return
            
        first_record = next(iter(data_dict.values()))
        fields = list(first_record.keys())
        
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not cursor.fetchone():
            columns = ', '.join([f"{field} TEXT" for field in fields])
            cursor.execute(f"CREATE TABLE {table_name} ({columns});")
        
        cursor.execute(f"DELETE FROM {table_name};")
        for row in data_dict.values():
            values = [str(row.get(field, '')) for field in fields]
            placeholders = ', '.join(['?'] * len(fields))
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error: {str(e)}")
    finally:
        if conn:
            conn.close()

def retrieve_data_disk(table_name):
    conn = None
    try:
        conn = sqlite3.connect(DISK_DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description] if cursor.description else []
        data = {}
        for i, row in enumerate(rows):
            data[i] = {columns[j]: row[j] for j in range(len(columns))} if columns else {}
        return data
    except sqlite3.Error as e:
        print(f"❌ SQLite error: {str(e)}")
        return {}
    finally:
        if conn:
            conn.close()

# --- Indexing Functions ---
def create_index(table, column, index_type="hash"):
    try:
        data = retrieve_data_memory(table)
        if not data:
            messagebox.showerror("Error", f"Table '{table}' has no data")
            return False
            
        index = {}
        for k, record in data.items():
            key = record.get(column)
            if key is not None:
                if index_type == "hash":
                    index[key] = k
                else:
                    index.setdefault(key, []).append(k)
                    
        INDEX_STORE[f"{table}_{column}"] = index
        print(f"✅ Index created on '{column}' with {index_type} indexing.")
        return True
    except Exception as e:
        messagebox.showerror("Error", f"Failed to create index: {str(e)}")
        return False
def log_to_file(query_name, execution_time, data, storage_type="Memory"):
    with open("runtime_report.txt", "a") as f:
        f.write(f"{query_name} ({storage_type}) - {execution_time:.6f} sec\nData: {data}\n\n")

def get_valid_table_gui(prompt):
    root = tk.Tk()
    root.withdraw()
    table = simpledialog.askstring("Select Table", prompt, parent=root)
    root.destroy()
    return table

def get_valid_field(table_name, prompt):
    data = retrieve_data_memory(table_name)
    if not data:
        print("⚠ Table is empty.")
        print("═" * 60)
        return input(prompt)
    fields = list(next(iter(data.values())).keys())
    while True:
        print(f"Available fields in '{table_name}': {fields}")
        print("═" * 60)
        field = input(prompt).strip()
        if field in fields:
            return field
        print("❌ Invalid field name. Try again.")
        print("═" * 60)

def get_valid_index_type():
    while True:
        index_type = input("Index type (hash/manual): ").strip().lower()
        if index_type in ["hash", "manual"]:
            return index_type
        print("❌ Invalid index type. Try again.")
        print("═" * 60)

def get_valid_join_type():
    while True:
        join_type = input("Join type (inner/left/right): ").strip().lower()
        if join_type in ["inner", "left", "right"]:
            return join_type
        print("❌ Invalid join type. Try again.")
        print("═" * 60)

def initialize_system():
    try:
        print("⏳ Initializing database system...")
        
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            print(f"📁 Created data directory: {DATA_DIR}")
        
        initialize_data_files()
        print("📝 Initialized data files")
        
        load_data_to_memory()
        print("💾 Loaded data into memory")
        
        for table_name, data in MEMORY_DB.items():
            store_data_disk(table_name, data)
        print("💽 Initialized disk database")
        
        if not MEMORY_DB:
            print("⚠ Warning: No tables loaded into memory")
            return False
            
        print("✅ Database system initialized successfully")
        print(f"Loaded tables: {list(MEMORY_DB.keys())}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to initialize system: {str(e)}")
        return False
    
#--VIEW RECORDS
def view_records_gui(root, compare_disk=True):
    if not MEMORY_DB:
        messagebox.showerror("Error", "No tables available in memory.")
        return

    select_window = tk.Toplevel(root)
    select_window.title("Select Table")
    select_window.geometry("300x150")

    tk.Label(select_window, text="Select table:", font=("Helvetica", 12)).pack(pady=10)

    selected_table = tk.StringVar()
    dropdown = ttk.Combobox(select_window, textvariable=selected_table, values=list(MEMORY_DB.keys()), state="readonly", width=25)
    dropdown.pack(pady=5)
    dropdown.current(0)

    def display_records():
        table = selected_table.get()
        select_window.destroy()

        result_window = tk.Toplevel(root)
        result_window.title(f"Records - {table}")
        result_window.geometry("800x600")

        text_area = scrolledtext.ScrolledText(result_window, font=("Courier New", 10))
        text_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # In-Memory Retrieval
        start_mem = time.time()
        data_mem = retrieve_data_memory(table)
        end_mem = time.time()

        text_area.insert(tk.END, f"{'═' * 80}\n📦 In-Memory Data from '{table}':\n{'═' * 80}\n")
        for k, v in data_mem.items():
            text_area.insert(tk.END, f"(Memory) {k}: {v}\n")
        text_area.insert(tk.END, f"{'═' * 80}\n⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
        log_to_file(f"View {table}", end_mem - start_mem, data_mem, "Memory")

        # Disk-Based Retrieval
        if compare_disk:
            start_disk = time.time()
            data_disk = retrieve_data_disk(table)
            end_disk = time.time()

            text_area.insert(tk.END, f"\n💽 Disk-Based Data from '{table}':\n{'═' * 80}\n")
            for k, v in data_disk.items():
                text_area.insert(tk.END, f"(Disk) {k}: {v}\n")
            text_area.insert(tk.END, f"{'═' * 80}\n⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
            log_to_file(f"View {table}", end_disk - start_disk, data_disk, "Disk")

        text_area.config(state=tk.DISABLED)

    tk.Button(select_window, text="View Records", command=display_records, width=20).pack(pady=10)

#-- VIEW JOINS
def view_joins_gui(root, compare_disk=True):
    if not MEMORY_DB:
        messagebox.showerror("Error", "No tables available in memory.")
        return

    join_window = tk.Toplevel(root)
    join_window.title("View Joins")
    join_window.geometry("400x400")

    def update_fields(event=None):
        left_key.set('')
        right_key.set('')
        
        left_table = selected_left.get()
        right_table = selected_right.get()
        
        if not left_table or not right_table:
            return
            
        try:
            left_data = retrieve_data_memory(left_table)
            if left_data:
                first_record = next(iter(left_data.values()))
                left_fields = list(first_record.keys())
                left_key_cb['values'] = left_fields
                if left_fields:
                    left_key_cb.current(0)
            
            right_data = retrieve_data_memory(right_table)
            if right_data:
                first_record = next(iter(right_data.values()))
                right_fields = list(first_record.keys())
                right_key_cb['values'] = right_fields
                if right_fields:
                    right_key_cb.current(0)
                    
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update fields: {str(e)}")

    tk.Label(join_window, text="Left Table").pack()
    selected_left = tk.StringVar()
    left_cb = ttk.Combobox(join_window, textvariable=selected_left, values=list(MEMORY_DB.keys()), state="readonly")
    left_cb.pack(pady=5)
    left_cb.bind("<<ComboboxSelected>>", update_fields)

    tk.Label(join_window, text="Right Table").pack()
    selected_right = tk.StringVar()
    right_cb = ttk.Combobox(join_window, textvariable=selected_right, values=list(MEMORY_DB.keys()), state="readonly")
    right_cb.pack(pady=5)
    right_cb.bind("<<ComboboxSelected>>", update_fields)

    tk.Label(join_window, text="Left Join Key").pack()
    left_key = tk.StringVar()
    left_key_cb = ttk.Combobox(join_window, textvariable=left_key, state="readonly")
    left_key_cb.pack(pady=5)

    tk.Label(join_window, text="Right Join Key").pack()
    right_key = tk.StringVar()
    right_key_cb = ttk.Combobox(join_window, textvariable=right_key, state="readonly")
    right_key_cb.pack(pady=5)

    tk.Label(join_window, text="Join Type").pack()
    join_type = tk.StringVar(value="inner")
    join_type_cb = ttk.Combobox(join_window, textvariable=join_type, values=["inner", "left", "right"], state="readonly")
    join_type_cb.pack(pady=10)
    join_type_cb.current(0)

    def run_join():
        left = selected_left.get()
        right = selected_right.get()
        l_key = left_key.get()
        r_key = right_key.get()
        j_type = join_type.get()

        if not all([left, right, l_key, r_key]):
            messagebox.showerror("Error", "Please select both tables and join keys")
            return

        left_data_mem = retrieve_data_memory(left)
        right_data_mem = retrieve_data_memory(right)
        result_mem = {}

        start_mem = time.time()
        if j_type == "inner":
            for l_id, l_val in left_data_mem.items():
                for r_id, r_val in right_data_mem.items():
                    if l_val.get(l_key) == r_val.get(r_key):
                        result_mem[f"{l_id}-{r_id}"] = {**l_val, **r_val}
        elif j_type == "left":
            for l_id, l_val in left_data_mem.items():
                match = False
                for r_id, r_val in right_data_mem.items():
                    if l_val.get(l_key) == r_val.get(r_key):
                        result_mem[f"{l_id}-{r_id}"] = {**l_val, **r_val}
                        match = True
                if not match:
                    result_mem[f"{l_id}-NULL"] = l_val
        elif j_type == "right":
            for r_id, r_val in right_data_mem.items():
                match = False
                for l_id, l_val in left_data_mem.items():
                    if l_val.get(l_key) == r_val.get(r_key):
                        result_mem[f"{l_id}-{r_id}"] = {**l_val, **r_val}
                        match = True
                if not match:
                    result_mem[f"NULL-{r_id}"] = r_val
        end_mem = time.time()

        result_window = tk.Toplevel(root)
        result_window.title("Join Result")
        result_window.geometry("900x600")
        text_area = scrolledtext.ScrolledText(result_window, font=("Courier", 10))
        text_area.pack(fill=tk.BOTH, expand=True)

        text_area.insert(tk.END, f"--- In-Memory Join Result ({j_type} join) ---\n{'═'*80}\n")
        for k, v in result_mem.items():
            text_area.insert(tk.END, f"(Memory) {k}: {v}\n")
        text_area.insert(tk.END, f"{'═'*80}\n⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
        log_to_file(f"{j_type.capitalize()} Join", end_mem - start_mem, result_mem, "Memory")

        if compare_disk:
            result_disk = {}
            left_data_disk = retrieve_data_disk(left)
            right_data_disk = retrieve_data_disk(right)
            start_disk = time.time()
            if j_type == "inner":
                for l_id, l_val in left_data_disk.items():
                    for r_id, r_val in right_data_disk.items():
                        if l_val.get(l_key) == r_val.get(r_key):
                            result_disk[f"{l_id}-{r_id}"] = {**l_val, **r_val}
            elif j_type == "left":
                for l_id, l_val in left_data_disk.items():
                    match = False
                    for r_id, r_val in right_data_disk.items():
                        if l_val.get(l_key) == r_val.get(r_key):
                            result_disk[f"{l_id}-{r_id}"] = {**l_val, **r_val}
                            match = True
                    if not match:
                        result_disk[f"{l_id}-NULL"] = l_val
            elif j_type == "right":
                for r_id, r_val in right_data_disk.items():
                    match = False
                    for l_id, l_val in left_data_disk.items():
                        if l_val.get(l_key) == r_val.get(r_key):
                            result_disk[f"{l_id}-{r_id}"] = {**l_val, **r_val}
                            match = True
                    if not match:
                        result_disk[f"NULL-{r_id}"] = r_val
            end_disk = time.time()

            text_area.insert(tk.END, f"\n--- Disk-Based Join Result ---\n{'═'*80}\n")
            for k, v in result_disk.items():
                text_area.insert(tk.END, f"(Disk) {k}: {v}\n")
            text_area.insert(tk.END, f"{'═'*80}\n⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
            log_to_file(f"{j_type.capitalize()} Join", end_disk - start_disk, result_disk, "Disk")

        text_area.config(state=tk.DISABLED)

    tk.Button(join_window, text="Run Join", command=run_join, width=20).pack(pady=20)

#--CREATING INDEX  
def create_index_gui(root):
    if not MEMORY_DB:
        messagebox.showerror("Error", "No tables in memory.")
        return

    win = tk.Toplevel(root)
    win.title("Create Index")
    win.geometry("350x250")

    tk.Label(win, text="Select Table").pack()
    table_var = tk.StringVar()
    table_cb = ttk.Combobox(win, textvariable=table_var, values=list(MEMORY_DB.keys()), state="readonly")
    table_cb.pack(pady=5)

    tk.Label(win, text="Select Column").pack()
    column_var = tk.StringVar()
    column_cb = ttk.Combobox(win, textvariable=column_var, state="readonly")
    column_cb.pack(pady=5)

    def update_columns(event=None):
        table = table_var.get()
        if table:
            sample = retrieve_data_memory(table)
            if sample:
                fields = list(next(iter(sample.values())).keys())
                column_cb['values'] = fields
                if fields:
                    column_cb.current(0)

    table_cb.bind("<<ComboboxSelected>>", update_columns)

    tk.Label(win, text="Index Type").pack()
    index_var = tk.StringVar(value="hash")
    index_cb = ttk.Combobox(win, textvariable=index_var, values=["hash", "btree"], state="readonly")
    index_cb.pack(pady=5)

    def run_indexing():
        table = table_var.get()
        column = column_var.get()
        index_type = index_var.get()
        if table and column:
            create_index(table, column, index_type)
            messagebox.showinfo("Success", f"Index created on '{column}' using {index_type}")
            win.destroy()
        else:
            messagebox.showwarning("Input Needed", "Please select table and column.")

    tk.Button(win, text="Create Index", command=run_indexing).pack(pady=15)

#--SQL QUERY
def run_sql_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run SQL Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter SQL Query:").pack(pady=5)
    sql_text = scrolledtext.ScrolledText(win, height=8, font=("Courier", 10))
    sql_text.pack(fill=tk.X, padx=10)

    output_text = scrolledtext.ScrolledText(win, height=20, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def parse_sql_query(query):
        query = query.strip().replace('\n', ' ')
        query_lower = query.lower()
        
        if query_lower.startswith('insert into'):
            try:
                table_start = query_lower.index('into') + 5
                table_end = query.find('(', table_start)
                table_name = query[table_start:table_end].strip()
                
                columns_start = table_end + 1
                columns_end = query.find(')', columns_start)
                columns = [col.strip() for col in query[columns_start:columns_end].split(',')]
                
                values_start = query_lower.find('values') + 7
                values_end = query.find(')', values_start)
                if values_end == -1:
                    values_end = len(query)
                values_str = query[values_start:values_end].strip(' ()')
                
                values = []
                current = ''
                in_quotes = False
                for char in values_str:
                    if char == "'":
                        in_quotes = not in_quotes
                    elif char == ',' and not in_quotes:
                        values.append(current.strip().strip("'"))
                        current = ''
                        continue
                    current += char
                if current:
                    values.append(current.strip().strip("'"))
                
                return {
                    'type': 'INSERT',
                    'table': table_name,
                    'columns': columns,
                    'values': values
                }
            except Exception as e:
                print(f"Error parsing INSERT query: {str(e)}")
                return None
                
        elif query_lower.startswith('update'):
            try:
                table_start = query_lower.index('update') + 7
                table_end = query_lower.index('set', table_start)
                table_name = query[table_start:table_end].strip()
                
                set_start = table_end + 4
                set_end = query_lower.find('where', set_start) if 'where' in query_lower else len(query)
                set_clause = query[set_start:set_end].strip()
                
                where_clause = None
                if 'where' in query_lower:
                    where_start = query_lower.index('where') + 6
                    where_clause = query[where_start:].strip()
                
                return {
                    'type': 'UPDATE',
                    'table': table_name,
                    'set': set_clause,
                    'where': where_clause
                }
            except Exception as e:
                print(f"Error parsing UPDATE query: {str(e)}")
                return None
                
        elif query_lower.startswith('delete from'):
            try:
                table_start = query_lower.index('from') + 5
                table_end = query_lower.index('where', table_start) if 'where' in query_lower else len(query)
                table_name = query[table_start:table_end].strip()
                
                where_clause = None
                if 'where' in query_lower:
                    where_start = query_lower.index('where') + 6
                    where_clause = query[where_start:].strip()
                
                return {
                    'type': 'DELETE',
                    'table': table_name,
                    'where': where_clause
                }
            except Exception as e:
                print(f"Error parsing DELETE query: {str(e)}")
                return None
                
        return {'type': 'SELECT'}  

    def evaluate_where(record, where_condition):
        if not where_condition:
            return True
            
        try:
            if '=' in where_condition:
                col, val = where_condition.split('=', 1)
                col = col.strip()
                val = val.strip().strip("'")
                return str(record.get(col, None)) == val
            return False
        except:
            return False

    def execute_query():
        query = sql_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Input Needed", "Please enter a SQL query.")
            return

        output_text.delete("1.0", tk.END)
        parsed = parse_sql_query(query)
        
        if parsed and parsed['type'] in ['INSERT', 'UPDATE', 'DELETE']:
            try:
                table_name = parsed['table']
                current_data = retrieve_data_memory(table_name)
                
                if parsed['type'] == 'INSERT':
                    new_id = max(current_data.keys()) + 1 if current_data else 1
                    new_record = dict(zip(parsed['columns'], parsed['values']))
                    
                    for col in parsed['columns']:
                        if col.lower().endswith('id'):
                            try:
                                new_record[col] = int(new_record[col])
                            except (ValueError, KeyError):
                                pass
                    
                    current_data[new_id] = new_record
                    output_text.insert(tk.END, f"✅ Inserted record with ID {new_id}\n")
                    
                elif parsed['type'] == 'UPDATE':
                    set_parts = [part.strip() for part in parsed['set'].split(',')]
                    updates = {}
                    for part in set_parts:
                        col, val = part.split('=', 1)
                        col = col.strip()
                        val = val.strip().strip("'")
                        updates[col] = val
                    
                    updated_count = 0
                    for record_id, record in current_data.items():
                        if evaluate_where(record, parsed['where']):
                            for col, val in updates.items():
                                record[col] = val
                            updated_count += 1
                    output_text.insert(tk.END, f"✅ Updated {updated_count} records\n")
                    
                elif parsed['type'] == 'DELETE':
                    deleted_ids = []
                    for record_id, record in list(current_data.items()):
                        if evaluate_where(record, parsed['where']):
                            del current_data[record_id]
                            deleted_ids.append(record_id)
                    output_text.insert(tk.END, f"✅ Deleted {len(deleted_ids)} records\n")
                
                MEMORY_DB[table_name] = current_data
                save_memory_to_file(table_name)
                store_data_disk(table_name, current_data)
                
                return
                
            except Exception as e:
                output_text.insert(tk.END, f"❌ Error executing {parsed['type']}: {str(e)}\n")
                return
        
        # Handle SELECT queries (original functionality)
        conn_mem = sqlite3.connect(DISK_DB_FILE)
        cursor_mem = conn_mem.cursor()
        
        # Create in-memory tables if they don't exist
        for table in MEMORY_DB:
            data = retrieve_data_memory(table)
            if data:
                fields = data[next(iter(data))].keys()
                try:
                    cursor_mem.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(fields)});")
                    cursor_mem.execute(f"DELETE FROM {table};")
                    for row in data.values():
                        values = [row.get(f) for f in fields]
                        cursor_mem.execute(f"INSERT INTO {table} VALUES ({','.join(['?'] * len(values))})", values)
                except sqlite3.OperationalError as e:
                    if "already exists" not in str(e):
                        output_text.insert(tk.END, f"Error creating memory table {table}: {e}\n")

        try:
            # Memory Execution
            start_mem = time.time()
            cursor_mem.execute(query)
            result_mem = cursor_mem.fetchall()
            end_mem = time.time()
            
            output_text.insert(tk.END, "--- In-Memory SQL Result ---\n" + "═"*70 + "\n")
            for row in result_mem:
                output_text.insert(tk.END, f"(Memory) {row}\n")
            output_text.insert(tk.END, f"{'═'*70}\n⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
            log_to_file("SQL Query", end_mem - start_mem, result_mem, "Memory")

            # Disk Execution
            if compare_disk:
                conn_disk = sqlite3.connect(DISK_DB_FILE)
                cursor_disk = conn_disk.cursor()
                try:
                    start_disk = time.time()
                    cursor_disk.execute(query)
                    result_disk = cursor_disk.fetchall()
                    end_disk = time.time()
                    
                    output_text.insert(tk.END, "\n--- Disk SQL Result ---\n" + "═"*70 + "\n")
                    for row in result_disk:
                        output_text.insert(tk.END, f"(Disk) {row}\n")
                    output_text.insert(tk.END, f"{'═'*70}\n⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                    log_to_file("SQL Query", end_disk - start_disk, result_disk, "Disk")
                except Exception as e:
                    output_text.insert(tk.END, f"❌ Disk SQL Error: {e}\n")
                finally:
                    conn_disk.close()
                    
        except Exception as e:
            output_text.insert(tk.END, f"❌ SQL Error: {e}\n")
        finally:
            conn_mem.close()

    tk.Button(win, text="Execute", command=execute_query, width=20).pack(pady=10)

# ----MONGODB QUERIES (modified for comparison)
def run_mongo_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run MongoDB-like Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter MongoDB-like Query:").pack(pady=5)
    mongo_query_text = scrolledtext.ScrolledText(win, height=8, font=("Courier", 10))
    mongo_query_text.pack(fill=tk.X, padx=10)

    output_text = scrolledtext.ScrolledText(win, height=20, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def execute_mongo_query():
        query = mongo_query_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Input Needed", "Please enter a MongoDB-like query.")
            return

        output_text.delete("1.0", tk.END)

        table = simpledialog.askstring("Select Table", "Enter table name:")
        if not table or table not in MEMORY_DB:
            messagebox.showerror("Error", "Invalid table name")
            return

        data_mem = retrieve_data_memory(table)
        data_disk = retrieve_data_disk(table)

        output_text.insert(tk.END, "Supported MongoDB-like commands:\n")
        output_text.insert(tk.END, "═" * 60 + "\n")
        output_text.insert(tk.END, "1. db.collection.find({})\n")
        output_text.insert(tk.END, "2. db.collection.find({'field': value})\n")
        output_text.insert(tk.END, "═" * 60 + "\n")

        try:
            if ".find(" in query:
                cond_str = query.split(".find(")[1].rstrip(")")
                cond = ast.literal_eval(cond_str) if cond_str.strip() else {}

                # --- In-Memory Find (Optimized) ---
                start_mem = time.time()
                result_mem = {}

                if cond == {}:
                    result_mem = data_mem
                elif len(cond) == 1:
                    field, value = next(iter(cond.items()))
                    sec_index = INDEX_STORE.get(table, {}).get("secondary", {}).get(field, {})
                    if sec_index and value in sec_index:
                        record_keys = sec_index[value]
                        result_mem = {k: data_mem[k] for k in record_keys if k in data_mem}
                    else:
                        result_mem = {}
                else:
                    # fallback to normal loop if complex condition
                    result_mem = {k: record for k, record in data_mem.items()
                                 if all(record.get(field) == val for field, val in cond.items())}

                end_mem = time.time()

                output_text.insert(tk.END, "\n--- In-Memory Find Result ---\n")
                output_text.insert(tk.END, "═" * 60 + "\n")
                for k, v in result_mem.items():
                    output_text.insert(tk.END, f"(Memory) {k}: {v}\n")
                output_text.insert(tk.END, "═" * 60 + "\n")
                output_text.insert(tk.END, f"⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
                log_to_file("MongoDB Query", end_mem - start_mem, result_mem, "Memory")
                # --- Disk-Based Find (normal) ---
                if compare_disk:
                    start_disk = time.time()
                    result_disk = {k: record for k, record in data_disk.items()
                                  if all(record.get(field) == val for field, val in cond.items())}
                    end_disk = time.time()

                    output_text.insert(tk.END, "\n--- Disk-Based Find Result ---\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    for k, v in result_disk.items():
                        output_text.insert(tk.END, f"(Disk) {k}: {v}\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                    log_to_file("MongoDB Query", end_disk - start_disk, result_disk, "Disk")

        except Exception as e:
            output_text.insert(tk.END, f"❌ Error executing Mongo query: {str(e)}\n")

    tk.Button(win, text="Execute", command=execute_mongo_query, width=20).pack(pady=10)

#-- LMDB QUERY
def run_lmdb_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run LMDB-like Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter LMDB-like Query:").pack(pady=5)
    query_entry = tk.Entry(win, width=50)
    query_entry.pack(pady=5)

    output_text = scrolledtext.ScrolledText(win, width=100, height=30, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def execute_lmdb_query():
        query = query_entry.get().strip()
        output_text.delete(1.0, tk.END)

        table = simpledialog.askstring("Select Table", "Enter table name:")
        if not table or table not in MEMORY_DB:
            messagebox.showerror("Error", "Invalid table name")
            return

        # Get compressed data directly from MEMORY_DB
        compressed_data = MEMORY_DB[table]
        
        try:
            output_text.insert(tk.END, "Supported LMDB-like commands:\n")
            output_text.insert(tk.END, "═" * 60 + "\n")
            output_text.insert(tk.END, "1. get <key>\n")
            output_text.insert(tk.END, "2. all\n")
            output_text.insert(tk.END, "═" * 60 + "\n")

            if query.startswith("get "):
                try:
                    key = int(query.split()[1])
                    
                    # --- Optimized In-Memory Get ---
                    start_mem = time.time()
                    
                    # Directly access the index if available
                    if table in INDEX_STORE and "primary" in INDEX_STORE[table]:
                        primary_index = INDEX_STORE[table]["primary"]
                        if key in primary_index:
                            all_data = decompress_data(compressed_data)
                            record_key = primary_index[key]
                            result_mem = all_data[record_key]
                        else:
                            result_mem = None
                    else:
                        # Fallback to full decompression if no index
                        data_mem = decompress_data(compressed_data)
                        result_mem = data_mem.get(key, None)
                    
                    end_mem = time.time()

                    output_text.insert(tk.END, "\n--- In-Memory Get Result ---\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"(Memory) {key}: {result_mem}\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
                    log_to_file("LMDB Query", end_mem - start_mem, {key: result_mem}, "Memory")

                    # Disk comparison remains the same
                    if compare_disk:
                        start_disk = time.time()
                        data_disk = retrieve_data_disk(table)
                        result_disk = data_disk.get(key, None)
                        end_disk = time.time()

                        output_text.insert(tk.END, "\n--- Disk-Based Get Result ---\n")
                        output_text.insert(tk.END, "═" * 60 + "\n")
                        output_text.insert(tk.END, f"(Disk) {key}: {result_disk}\n")
                        output_text.insert(tk.END, "═" * 60 + "\n")
                        output_text.insert(tk.END, f"⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                        log_to_file("LMDB Query", end_disk - start_disk, {key: result_disk}, "Disk")

                except (ValueError, IndexError):
                    output_text.insert(tk.END, "❌ Invalid key format. Use 'get <number>' or 'all'\n")

            elif query == "all":
                # --- Optimized In-Memory All Records ---
                start_mem = time.time()
                
                # Decompress data once and use generator for output
                data_mem = decompress_data(compressed_data)
                result_count = len(data_mem)
                end_mem = time.time()

                output_text.insert(tk.END, "\n--- In-Memory All Records ---\n")
                output_text.insert(tk.END, "═" * 60 + "\n")
                output_text.insert(tk.END, f"Retrieved {result_count} records in {end_mem - start_mem:.6f} sec\n")
                
                for i, (k, v) in enumerate(data_mem.items()):
                    if i < 10:
                        output_text.insert(tk.END, f"(Memory) {k}: {v}\n")
                    elif i == 10:
                        output_text.insert(tk.END, "... (showing first 10 of {result_count} records)\n")
                        break
                
                output_text.insert(tk.END, "═" * 60 + "\n")
                log_to_file("LMDB Query", end_mem - start_mem, {"count": result_count}, "Memory")

                # Disk comparison remains the same
                if compare_disk:
                    start_disk = time.time()
                    data_disk = retrieve_data_disk(table)
                    result_count_disk = len(data_disk)
                    end_disk = time.time()

                    output_text.insert(tk.END, "\n--- Disk-Based All Records ---\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"Retrieved {result_count_disk} records in {end_disk - start_disk:.6f} sec\n")
                    
                    for i, (k, v) in enumerate(data_disk.items()):
                        if i < 10:
                            output_text.insert(tk.END, f"(Disk) {k}: {v}\n")
                        elif i == 10:
                            output_text.insert(tk.END, "... (showing first 10 of {result_count_disk} records)\n")
                            break
                    
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    log_to_file("LMDB Query", end_disk - start_disk, {"count": result_count_disk}, "Disk")

            else:
                output_text.insert(tk.END, "❌ Invalid query. Use 'get <key>' or 'all'\n")

        except Exception as e:
            output_text.insert(tk.END, f"❌ Error executing LMDB query: {str(e)}\n")

    tk.Button(win, text="Execute", command=execute_lmdb_query, width=20).pack(pady=10)
    
# ---POSTGRESQL QUERIES (modified for comparison)
def run_postgresql_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run Postgre-like Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter PostgreSQL-like SQL Query:").pack(pady=5)
    sql_text = scrolledtext.ScrolledText(win, height=8, font=("Courier", 10))
    sql_text.pack(fill=tk.X, padx=10)

    output_text = scrolledtext.ScrolledText(win, height=20, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def execute_pg_query():
        query = sql_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Input Needed", "Please enter a SQL query.")
            return

        output_text.delete("1.0", tk.END)

        # In-Memory
        conn_mem = sqlite3.connect(":memory:")
        cursor_mem = conn_mem.cursor()
        for table in MEMORY_DB:
            data = retrieve_data_memory(table)
            if data:
                fields = data[next(iter(data))].keys()
                try:
                    cursor_mem.execute(f"CREATE TABLE {table} ({', '.join(fields)});")
                    for row in data.values():
                        values = [row.get(f) for f in fields]
                        cursor_mem.execute(f"INSERT INTO {table} VALUES ({','.join(['?'] * len(values))})", values)
                except sqlite3.OperationalError as e:
                    if "already exists" not in str(e):
                        output_text.insert(tk.END, f"❌ Error creating memory table {table}: {e}\n")

        conn_disk = sqlite3.connect(DISK_DB_FILE)
        cursor_disk = conn_disk.cursor()

        try:
            # Memory Execution
            start_mem = time.time()
            cursor_mem.execute(query)
            result_mem = cursor_mem.fetchall()
            end_mem = time.time()
            conn_mem.commit()

            output_text.insert(tk.END, "--- In-Memory PostgreSQL Result ---\n" + "═" * 70 + "\n")
            for row in result_mem:
                output_text.insert(tk.END, f"(Memory) {row}\n")
            output_text.insert(tk.END, f"{'═'*70}\n⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
            log_to_file("PostgreSQL Query", end_mem - start_mem, result_mem, "Memory")

            # Disk Execution
            if compare_disk:
                start_disk = time.time()
                cursor_disk.execute(query)
                result_disk = cursor_disk.fetchall()
                end_disk = time.time()
                conn_disk.commit()

                output_text.insert(tk.END, "\n--- Disk-Based PostgreSQL Result ---\n" + "═" * 70 + "\n")
                for row in result_disk:
                    output_text.insert(tk.END, f"(Disk) {row}\n")
                output_text.insert(tk.END, f"{'═'*70}\n⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                log_to_file("PostgreSQL Query", end_disk - start_disk, result_disk, "Disk")

        except Exception as e:
            output_text.insert(tk.END, f"❌ PostgreSQL Query Error: {e}\n")
        finally:
            conn_mem.close()
            conn_disk.close()

    tk.Button(win, text="Execute", command=execute_pg_query, width=20).pack(pady=10)

#-- tinyDB QUERIES (modified for comparison - basic read comparison)
def run_tinydb_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run TinyDB-like Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter TinyDB-like Query:").pack(pady=5)
    query_text = scrolledtext.ScrolledText(win, height=4, font=("Courier", 10))
    query_text.pack(fill=tk.X, padx=10)

    output_text = scrolledtext.ScrolledText(win, height=22, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def execute_tiny_query():
        query = query_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Input Needed", "Please enter a TinyDB-like query.")
            return

        output_text.delete("1.0", tk.END)

        try:
            table = simpledialog.askstring("Select Table", "Enter table name:")
            if not table:
                messagebox.showerror("Error", "No table name provided")
                return
            if table not in MEMORY_DB:
                messagebox.showerror("Error", f"Table '{table}' not found in database")
                return

            data_mem = retrieve_data_memory(table)
            if compare_disk:
                data_disk = retrieve_data_disk(table)

            output_text.insert(tk.END, "🔍 Supported TinyDB-like queries:\n")
            output_text.insert(tk.END, "═" * 60 + "\n")
            output_text.insert(tk.END, "1. Query all: all\n")
            output_text.insert(tk.END, "2. Query by field: field == value\n")
            output_text.insert(tk.END, "3. Query with AND: (field1 == value1) & (field2 == value2)\n")
            output_text.insert(tk.END, "═" * 60 + "\n\n")

            result_mem = {}
            result_disk = {}

            # --- In-Memory Find (with Index Optimization) ---
            start_mem = time.time()
            if query.lower() == "all":
                result_mem = data_mem  # No optimization for 'all'
            elif "==" in query and "&" not in query:
                try:
                    field_part, value_part = [part.strip() for part in query.split("==", 1)]
                    field = field_part
                    value = ast.literal_eval(value_part)

                    # Use secondary index for faster lookup
                    sec_index = INDEX_STORE.get(table, {}).get("secondary", {}).get(field, {})
                    if sec_index and value in sec_index:
                        record_keys = sec_index[value]
                        result_mem = {k: data_mem[k] for k in record_keys if k in data_mem}
                    else:
                        result_mem = {k: v for k, v in data_mem.items() if v.get(field) == value}
                except (ValueError, SyntaxError) as e:
                    raise ValueError(f"Invalid query format. Example: 'field == value' - Error: {str(e)}")
            else:
                raise ValueError("Only simple 'field == value' queries are supported currently.")

            end_mem = time.time()

            output_text.insert(tk.END, "--- In-Memory TinyDB Result ---\n")
            output_text.insert(tk.END, "═" * 70 + "\n")
            if result_mem:
                for k, v in result_mem.items():
                    output_text.insert(tk.END, f"(Memory) {k}: {v}\n")
            else:
                output_text.insert(tk.END, "No results found in memory\n")
            output_text.insert(tk.END, f"\n⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
            output_text.insert(tk.END, "═" * 70 + "\n")

            log_to_file("TinyDB Query", end_mem - start_mem, result_mem, "Memory")

            # --- Disk-Based Find (normal) ---
            if compare_disk:
                start_disk = time.time()

                if query.lower() == "all":
                    result_disk = data_disk
                elif "==" in query and "&" not in query:
                    try:
                        field_part, value_part = [part.strip() for part in query.split("==", 1)]
                        field = field_part
                        value = ast.literal_eval(value_part)

                        result_disk = {k: v for k, v in data_disk.items() if v.get(field) == value}
                    except (ValueError, SyntaxError) as e:
                        raise ValueError(f"Invalid query format for disk execution. Error: {str(e)}")
                else:
                    raise ValueError("Only simple 'field == value' queries are supported currently.")

                end_disk = time.time()

                output_text.insert(tk.END, "\n--- Disk-Based TinyDB Result ---\n")
                output_text.insert(tk.END, "═" * 70 + "\n")
                if result_disk:
                    for k, v in result_disk.items():
                        output_text.insert(tk.END, f"(Disk) {k}: {v}\n")
                else:
                    output_text.insert(tk.END, "No results found on disk\n")
                output_text.insert(tk.END, f"\n⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                output_text.insert(tk.END, "═" * 70 + "\n")

                log_to_file("TinyDB Query", end_disk - start_disk, result_disk, "Disk")

        except Exception as e:
            output_text.insert(tk.END, f"❌ Error executing TinyDB query:\n")
            output_text.insert(tk.END, f"Error type: {type(e)._name_}\n")
            output_text.insert(tk.END, f"Error details: {str(e)}\n")
            output_text.insert(tk.END, f"Query attempted: {query}\n")
            output_text.insert(tk.END, "═" * 60 + "\n")
            import traceback
            output_text.insert(tk.END, f"Traceback:\n{traceback.format_exc()}\n")

    tk.Button(win, text="Execute", command=execute_tiny_query, width=20).pack(pady=10)


#-- LMDB QUERIES (modified for basic key-value comparison)
def run_lmdb_query_gui(root, compare_disk=True):
    win = tk.Toplevel(root)
    win.title("Run LMDB-like Query")
    win.geometry("800x600")

    tk.Label(win, text="Enter LMDB-like Query:").pack(pady=5)
    query_entry = tk.Entry(win, width=50)
    query_entry.pack(pady=5)

    output_text = scrolledtext.ScrolledText(win, width=100, height=30, font=("Courier", 10))
    output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def execute_lmdb_query():
        query = query_entry.get().strip()
        output_text.delete(1.0, tk.END)

        table = simpledialog.askstring("Select Table", "Enter table name:")
        if not table or table not in MEMORY_DB:
            messagebox.showerror("Error", "Invalid table name")
            return

        data_mem = retrieve_data_memory(table)
        data_disk = retrieve_data_disk(table)

        output_text.insert(tk.END, "Supported LMDB-like commands:\n")
        output_text.insert(tk.END, "═" * 60 + "\n")
        output_text.insert(tk.END, "1. get <key>\n")
        output_text.insert(tk.END, "2. all\n")
        output_text.insert(tk.END, "═" * 60 + "\n")

        try:
            if query.startswith("get "):
                try:
                    key = int(query.split()[1])

                    # --- Optimized In-Memory Get using direct indexing ---
                    start_mem = time.time()
                    result_mem = data_mem.get(key, None)
                    end_mem = time.time()

                    output_text.insert(tk.END, "\n--- In-Memory Get Result ---\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"(Memory) {key}: {result_mem}\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, f"⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
                    log_to_file("LMDB Query", end_mem - start_mem, {key: result_mem}, "Memory")

                    # --- Disk-Based Get ---
                    if compare_disk:
                        start_disk = time.time()
                        result_disk = data_disk.get(key, None)
                        end_disk = time.time()

                        output_text.insert(tk.END, "\n--- Disk-Based Get Result ---\n")
                        output_text.insert(tk.END, "═" * 60 + "\n")
                        output_text.insert(tk.END, f"(Disk) {key}: {result_disk}\n")
                        output_text.insert(tk.END, "═" * 60 + "\n")
                        output_text.insert(tk.END, f"⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                        log_to_file("LMDB Query", end_disk - start_disk, {key: result_disk}, "Disk")

                except (ValueError, IndexError):
                    output_text.insert(tk.END, "❌ Invalid key format. Use 'get <number>' or 'all'\n")

            elif query == "all":
                # --- In-Memory All Records ---
                start_mem = time.time()

                # Directly join results into a single string, avoiding repeated string concatenation
                result_mem = "\n".join([f"(Memory) {k}: {v}" for k, v in data_mem.items()])
                
                end_mem = time.time()

                output_text.insert(tk.END, "\n--- In-Memory All Records ---\n")
                output_text.insert(tk.END, "═" * 60 + "\n")
                output_text.insert(tk.END, result_mem)
                output_text.insert(tk.END, "\n" + "═" * 60 + "\n")
                output_text.insert(tk.END, f"⏱ Memory Execution Time: {end_mem - start_mem:.6f} sec\n")
                log_to_file("LMDB Query", end_mem - start_mem, data_mem, "Memory")

                # --- Disk-Based All Records ---
                if compare_disk:
                    start_disk = time.time()
                    result_disk = "\n".join([f"(Disk) {k}: {v}" for k, v in data_disk.items()])
                    end_disk = time.time()

                    output_text.insert(tk.END, "\n--- Disk-Based All Records ---\n")
                    output_text.insert(tk.END, "═" * 60 + "\n")
                    output_text.insert(tk.END, result_disk)
                    output_text.insert(tk.END, "\n" + "═" * 60 + "\n")
                    output_text.insert(tk.END, f"⏱ Disk Execution Time: {end_disk - start_disk:.6f} sec\n")
                    log_to_file("LMDB Query", end_disk - start_disk, data_disk, "Disk")

            else:
                output_text.insert(tk.END, "❌ Invalid query. Use 'get <key>' or 'all'\n")

        except Exception as e:
            output_text.insert(tk.END, f"❌ Error executing LMDB query: {str(e)}\n")

    tk.Button(win, text="Execute", command=execute_lmdb_query, width=20).pack(pady=10)

#--REPORT GENERATION
def report_generation_gui():
    messagebox.showinfo("Action", "Generating Report (Memory vs. Disk)")
    runtime_txt_file = "runtime_report.txt"
    runtime_excel_file = "runtimes.xlsx"

    memory_queries = {}
    disk_queries = {}

    try:
        with open(runtime_txt_file, "r") as file:
            for line in file:
                if " - " in line and "sec" in line:
                    parts = line.split(" - ")
                    query_info = parts[0].strip()
                    time_str = parts[1].split(" ")[0].strip()
                    storage_type = "Memory"
                    query_name = query_info
                    if " (Disk)" in query_info:
                        storage_type = "Disk"
                        query_name = query_info.replace(" (Disk)", "").strip()
                    elif " (Memory)" in query_info:
                        storage_type = "Memory"
                        query_name = query_info.replace(" (Memory)", "").strip()

                    if storage_type == "Memory":
                        memory_queries.setdefault(query_name, []).append(float(time_str))
                    elif storage_type == "Disk":
                        disk_queries.setdefault(query_name, []).append(float(time_str))

        if not memory_queries and not disk_queries:
            messagebox.showerror("Error", "No query runtime data found.")
            return

        query_labels = list(set(memory_queries.keys()) | set(disk_queries.keys()))
        x = range(len(query_labels))
        width = 0.35

        fig, ax = plt.subplots(figsize=(12, 7))
        mem_times = [sum(memory_queries.get(q, [0.0])) / len(memory_queries.get(q, [1])) for q in query_labels]
        disk_times = [sum(disk_queries.get(q, [0.0])) / len(disk_queries.get(q, [1])) for q in query_labels]

        ax.bar([i - width/2 for i in x], mem_times, width, label="Memory")
        ax.bar([i + width/2 for i in x], disk_times, width, label="Disk")

        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title('Memory vs. Disk Query Runtime Comparison')
        ax.set_xticks(x)
        ax.set_xticklabels(query_labels, rotation=45, ha="right")
        ax.legend()
        fig.tight_layout()
        plt.savefig("runtime_comparison_graph.png")
        plt.show()

        wb = Workbook()
        ws = wb.active
        ws.title = "Query Runtimes Comparison"
        ws.append(["Query", "Memory Time (sec)", "Disk Time (sec)"])
        all_queries = sorted(list(set(memory_queries.keys()) | set(disk_queries.keys())))
        for query in all_queries:
            mem_avg = sum(memory_queries.get(query, [0.0])) / len(memory_queries.get(query, [1])) if memory_queries.get(query) else 0.0
            disk_avg = sum(disk_queries.get(query, [0.0])) / len(disk_queries.get(query, [1])) if disk_queries.get(query) else 0.0
            ws.append([query, mem_avg, disk_avg])
        wb.save(runtime_excel_file)

        messagebox.showinfo("Success", "Report generated: runtimes.xlsx and runtime_comparison_graph.png")

    except FileNotFoundError:
        messagebox.showerror("Error", f"{runtime_txt_file} not found. Run some queries first.")
    except Exception as e:
        messagebox.showerror("Error", f"Error generating report:\n{str(e)}")

# --- Main Menu ---
def exit_program(root):
    if messagebox.askyesno("Exit", "Are you sure you want to exit?"):
        if os.path.exists("disk_database.db"):
            os.remove("disk_database.db")
        root.destroy()
        return

def main_gui():
    if not initialize_system():
        messagebox.showerror("Error", "Failed to initialize database system")
        return

    root = tk.Tk()
    root.title("In-Memory Database System with Disk Comparison")
    root.geometry("600x550")
    root.configure(bg="#f0f0f0")

    tk.Label(root, text="💾 In-Memory Database System With Disk Comparison 💽", 
             font=("Helvetica", 14, "bold"), pady=20, bg="#f0f0f0").pack()
    menu_options = [
        ("View Records (Compare with Disk)", lambda: view_records_gui(root)),
        ("View Joins (Compare with Disk)", lambda: view_joins_gui(root)),
        ("Create Index", lambda: create_index_gui(root)),
        ("Run SQL Query (Compare)", lambda: run_sql_query_gui(root)),
        ("Run MongoDB-like Query (Compare)", lambda: run_mongo_query_gui(root)),
        ("Run PostgreSQL-like Query (Compare)", lambda: run_postgresql_query_gui(root)),
        ("Run tinyDB-like Query (Compare)", lambda: run_tinydb_query_gui(root)),
        ("Run LMDB-like Query (Compare)", lambda: run_lmdb_query_gui(root)),
        ("Generate Report (Memory vs. Disk)", lambda: report_generation_gui()),
        ("Exit", lambda: exit_program(root))
    ]

    for text, command in menu_options:
        tk.Button(root, text=text, command=command, width=60, height=2, 
                 bg="#e0e0e0", font=("Helvetica", 10)).pack(pady=5)

    root.mainloop()
if __name__ == "__main__":
    main_gui()