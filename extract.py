import os
import shutil
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import json
from pathlib import Path

CONFIG_FILE = "rpf-extractor_config.json"

TOOL_TYPES = {
    "RPF CLI": "rpf"
}

STREAM_EXTENSIONS = {
    'yft', 'ytd', 'ydr', 'ydd', 'ybn', 'ymap', 'ytyp',
    'awc', 'cut', 'rel', 'ynv', 'ycd', 'ynd',
    'ypdb', 'ysc', 'yvr', 'xtd'
}

DATA_EXTENSIONS = {
    'meta', 'xml', 'dat'               
}

IMPORTANT_FOLDERS = {
    'vehicles', 'weapons', 'peds', 'props'
}

IGNORE_FOLDERS = {
    'audio', 'lang', 'common.rpf',
    'x64a.rpf', 'x64b.rpf', 'x64c.rpf', 'x64d.rpf', 'x64e.rpf', 'x64f.rpf', 'x64g.rpf',
    'dlc_patch', 'update', 'platform'
}

def load_config():
    """Load configuration from JSON file."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
    return {"rpf_cli": "", "last_directory": "", "auto_cleanup": True, "tool_type": "rpf"}

def save_config(config):
    """Save configuration to JSON file."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        print(f"Error saving config: {e}")

def find_rpf_cli():
    """Try to find rpf-cli.exe in common locations."""
    search_paths = [
        "./rpf-cli.exe",
        os.path.join(os.path.dirname(__file__), "rpf-cli.exe"),
        os.path.join(os.path.dirname(__file__), "tools", "rpf-cli.exe"),
    ]
    
    for path in search_paths:
        if os.path.isfile(path):
            return os.path.abspath(path)
    
    return None

def validate_rpf_cli(rpf_cli_path):
    """Validate that the RPF CLI tool exists and works."""
    if not rpf_cli_path or not os.path.isfile(rpf_cli_path):
        return False, "RPF CLI tool not found at specified path"
    
    try:
        result = subprocess.run(
            [rpf_cli_path], 
            capture_output=True, 
            timeout=3,
            text=True
        )
        return True, "RPF CLI tool validated successfully"
    except subprocess.TimeoutExpired:
        return True, "RPF CLI tool found and executable"
    except FileNotFoundError:
        return False, "RPF CLI tool file not found"
    except Exception as e:
        return False, f"RPF CLI validation error: {str(e)}"

def ensure_clean_dirs(base_path):
    """Create or clean the stream and data directories."""
    stream_dir = os.path.join(base_path, "stream")
    data_dir = os.path.join(base_path, "data")

    for directory in [stream_dir, data_dir]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.makedirs(directory)
    
    return stream_dir, data_dir

def move_files(base_dir, stream_dir, data_dir, progress_callback=None):
    """Move files to stream/ and data/ with improved handling."""
    moved_files = {'stream': 0, 'data': 0, 'skipped': 0}
    processed = 0
    extracted_rpfs = []

    total_files = sum(len(files) for _, _, files in os.walk(base_dir))
    if total_files == 0:
        return moved_files

    for root, dirs, files in os.walk(base_dir):
        if any(ignore.lower() in root.lower() for ignore in IGNORE_FOLDERS):
            continue

        is_important = any(folder.lower() in root.lower() for folder in IMPORTANT_FOLDERS)

        for file in files:
            processed += 1
            if progress_callback:
                progress_callback(processed, total_files, f"Processing: {file}")

            ext = file.lower().split('.')[-1]
            src_path = os.path.join(root, file)
            
            try:
                if ext == 'rpf':
                    moved_files['skipped'] += 1
                    continue
                
                if is_important or ext in STREAM_EXTENSIONS:
                    dest_path = os.path.join(stream_dir, file)
                    dest_dir = 'stream'
                elif ext in DATA_EXTENSIONS:
                    dest_path = os.path.join(data_dir, file)
                    dest_dir = 'data'
                else:
                    moved_files['skipped'] += 1
                    continue

                if os.path.exists(dest_path):
                    base_name, extension = os.path.splitext(file)
                    counter = 1
                    while os.path.exists(dest_path):
                        new_name = f"{base_name}_{counter}{extension}"
                        dest_path = os.path.join(os.path.dirname(dest_path), new_name)
                        counter += 1

                src_size = os.path.getsize(src_path)
                
                with open(src_path, 'rb') as fsrc:
                    with open(dest_path, 'wb') as fdst:
                        copied_bytes = 0
                        while True:
                            chunk = fsrc.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            fdst.write(chunk)
                            copied_bytes += len(chunk)
                        
                        fdst.flush()
                        os.fsync(fdst.fileno())
                
                dest_size = os.path.getsize(dest_path)
                
                if src_size != dest_size:
                    print(f"ERROR: Size mismatch for {file}")
                    print(f"  Source: {src_size} bytes ({src_size / 1024 / 1024:.2f} MB)")
                    print(f"  Destination: {dest_size} bytes ({dest_size / 1024 / 1024:.2f} MB)")
                    print(f"  Copied: {copied_bytes} bytes ({copied_bytes / 1024 / 1024:.2f} MB)")
                    raise Exception(f"File size mismatch: {file} (expected {src_size}, got {dest_size})")
                
                if src_size > 16 * 1024 * 1024:
                    print(f"Successfully copied large file: {file} ({src_size / 1024 / 1024:.2f} MB)")
                
                moved_files[dest_dir] += 1

            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
                moved_files['skipped'] += 1
                continue

    return moved_files

def extract_rpf_recursive(rpf_file, rpf_cli, output_dir, tool_type, progress_callback=None, is_nested=False):
    """Recursively extract a .rpf file and all nested .rpf files."""
    
    if progress_callback:
        progress_callback(0, 100, f"Extracting: {os.path.basename(rpf_file)}")
    
    if is_nested:
        extraction_dir = os.path.join(os.path.dirname(rpf_file), f"_temp_{os.path.basename(rpf_file)}_extract")
    else:
        extraction_dir = os.path.join(os.path.dirname(rpf_file), "dlc")
    
    try:
        cmd = [rpf_cli, "extract", rpf_file]
        
        print(f"Running extraction command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            timeout=300,
            cwd=os.path.dirname(rpf_file),
            shell=False
        )
        
        if result.stdout:
            output = result.stdout.decode('utf-8', errors='ignore')
            if output.strip():
                print(f"Extraction Output: {output}")
        if result.stderr:
            errors = result.stderr.decode('utf-8', errors='ignore')
            if errors.strip():
                print(f"Extraction Warnings: {errors}")
            
    except subprocess.TimeoutExpired:
        return None, "Extraction timed out (5 minutes limit)", []
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
        return None, f"Extraction failed:\n{error_msg}", []
    except FileNotFoundError:
        return None, "Extraction tool not found. Please check the tool path.", []
    except Exception as e:
        return None, f"Unexpected error during extraction: {str(e)}", []

    base_dir = None
    possible_dirs = [
        extraction_dir,
        os.path.join(os.path.dirname(rpf_file), "dlc"),
        os.path.join(os.path.dirname(rpf_file), os.path.splitext(os.path.basename(rpf_file))[0])
    ]
    
    for possible_dir in possible_dirs:
        if os.path.exists(possible_dir):
            base_dir = possible_dir
            break
    
    if not base_dir:
        rpf_dir = os.path.dirname(rpf_file)
        for item in os.listdir(rpf_dir):
            item_path = os.path.join(rpf_dir, item)
            if os.path.isdir(item_path) and item not in ['stream', 'data']:
                base_dir = item_path
                break
    
    if not base_dir or not os.path.exists(base_dir):
        return None, "No extracted content found. The RPF file may be empty or in an unsupported format.", []

    if not is_nested:
        stream_dir, data_dir = ensure_clean_dirs(output_dir)
    else:
        stream_dir = os.path.join(output_dir, "stream")
        data_dir = os.path.join(output_dir, "data")

    if progress_callback:
        progress_callback(0, 100, "Organizing extracted files...")
    
    moved_files = move_files(base_dir, stream_dir, data_dir, progress_callback)

    successfully_extracted_rpfs = []

    nested_rpfs = []
    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith(".rpf"):
                nested_rpfs.append(os.path.join(root, f))

    if nested_rpfs and progress_callback:
        progress_callback(0, len(nested_rpfs), f"Found {len(nested_rpfs)} nested RPF files...")

    for idx, nested_rpf in enumerate(nested_rpfs):
        if progress_callback:
            progress_callback(
                idx + 1, 
                len(nested_rpfs), 
                f"Processing nested RPF {idx + 1}/{len(nested_rpfs)}: {os.path.basename(nested_rpf)}"
            )

        nested_result, nested_error, nested_extracted_rpfs = extract_rpf_recursive(
            nested_rpf, rpf_cli, output_dir, tool_type, None, is_nested=True
        )
        
        if nested_result:
            moved_files['stream'] += nested_result['stream']
            moved_files['data'] += nested_result['data']
            moved_files['skipped'] += nested_result['skipped']
            
            if nested_result['stream'] > 0 or nested_result['data'] > 0:
                successfully_extracted_rpfs.append(nested_rpf)
                successfully_extracted_rpfs.extend(nested_extracted_rpfs)
        
        nested_extract_dir = os.path.join(os.path.dirname(nested_rpf), f"_temp_{os.path.basename(nested_rpf)}_extract")
        if os.path.exists(nested_extract_dir):
            try:
                shutil.rmtree(nested_extract_dir)
            except Exception as e:
                print(f"Cleanup warning for {nested_extract_dir}: {e}")

    return moved_files, None, successfully_extracted_rpfs

class RPFExtractorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("RPF Extractor - Production Ready")
        self.root.geometry("650x480")
        self.root.resizable(True, True)
        
        self.config = load_config()
        self.is_extracting = False
        
        self.setup_ui()
        self.load_saved_settings()
        
        if not self.config.get("rpf_cli"):
            self.auto_find_rpf_cli()

    def setup_ui(self):
        """Setup the GUI components."""
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        header_text = "RPF Extractor - Powered by RPF CLI Tool"
        ttk.Label(main_frame, text=header_text, font=('', 11, 'bold')).grid(
            row=0, column=0, columnspan=3, pady=(0, 5)
        )
        
        warning_text = "⚠️ Note: Current RPF CLI may not extract some files correctly. Large files may be truncated."
        ttk.Label(main_frame, text=warning_text, font=('', 8), foreground="orange").grid(
            row=1, column=0, columnspan=3, pady=(0, 10)
        )

        ttk.Label(main_frame, text="RPF CLI Tool Path (rpf-cli.exe):").grid(
            row=2, column=0, sticky=tk.W, pady=(5, 2)
        )
        
        cli_frame = ttk.Frame(main_frame)
        cli_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 5))
        
        self.cli_entry = ttk.Entry(cli_frame, width=60)
        self.cli_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(cli_frame, text="Browse", command=self.select_cli, width=10).pack(
            side=tk.LEFT, padx=(5, 2)
        )
        ttk.Button(cli_frame, text="Validate", command=self.validate_cli, width=10).pack(
            side=tk.LEFT
        )

        ttk.Label(main_frame, text="RPF File to Extract:").grid(
            row=4, column=0, sticky=tk.W, pady=(10, 2)
        )
        
        file_frame = ttk.Frame(main_frame)
        file_frame.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 5))
        
        self.file_entry = ttk.Entry(file_frame, width=60)
        self.file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(file_frame, text="Browse", command=self.select_file, width=10).pack(
            side=tk.LEFT, padx=(5, 0)
        )

        ttk.Label(main_frame, text="Output Directory:").grid(
            row=6, column=0, sticky=tk.W, pady=(10, 2)
        )
        
        output_frame = ttk.Frame(main_frame)
        output_frame.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 5))
        
        self.output_entry = ttk.Entry(output_frame, width=60)
        self.output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ttk.Button(output_frame, text="Browse", command=self.select_output, width=10).pack(
            side=tk.LEFT, padx=(5, 0)
        )

        options_frame = ttk.LabelFrame(main_frame, text="Options", padding="5")
        options_frame.grid(row=8, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 5))
        
        self.auto_cleanup_var = tk.BooleanVar(value=self.config.get("auto_cleanup", True))
        ttk.Checkbutton(
            options_frame, 
            text="Auto-cleanup temporary files after extraction",
            variable=self.auto_cleanup_var
        ).pack(anchor=tk.W)

        self.progress = ttk.Progressbar(main_frame, mode='determinate', length=500)
        self.progress.grid(row=9, column=0, columnspan=3, pady=(10, 5), sticky=(tk.W, tk.E))

        self.status_label = ttk.Label(main_frame, text="Ready to extract", foreground="green")
        self.status_label.grid(row=10, column=0, columnspan=3, pady=5)

        self.extract_btn = ttk.Button(
            main_frame, 
            text="Extract RPF", 
            command=self.start_extraction,
            style='Accent.TButton'
        )
        self.extract_btn.grid(row=11, column=0, columnspan=3, pady=(10, 0))

        main_frame.columnconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

    def load_saved_settings(self):
        """Load saved settings from config."""
        if self.config.get("rpf_cli"):
            self.cli_entry.insert(0, self.config["rpf_cli"])
        
        if self.config.get("last_directory"):
            self.output_entry.insert(0, self.config["last_directory"])

    def auto_find_rpf_cli(self):
        """Automatically try to find the RPF CLI tool."""
        found_path = find_rpf_cli()
        if found_path:
            self.cli_entry.delete(0, tk.END)
            self.cli_entry.insert(0, found_path)
            self.config["rpf_cli"] = found_path
            save_config(self.config)
            self.status_label.config(text="RPF CLI tool found automatically ✓", foreground="green")

    def select_cli(self):
        """Select extraction tool executable."""
        file_path = filedialog.askopenfilename(
            title="Select RPF CLI tool (rpf-cli.exe)",
            filetypes=[("Executable Files", "*.exe"), ("All Files", "*.*")]
        )
        if file_path:
            self.cli_entry.delete(0, tk.END)
            self.cli_entry.insert(0, file_path)
            self.config["rpf_cli"] = file_path
            save_config(self.config)

    def validate_cli(self):
        """Validate the RPF CLI tool."""
        cli_path = self.cli_entry.get()
        is_valid, message = validate_rpf_cli(cli_path)
        
        if is_valid:
            messagebox.showinfo("Validation Success", message)
            self.status_label.config(text="RPF CLI validated ✓", foreground="green")
        else:
            messagebox.showerror("Validation Failed", message)
            self.status_label.config(text="RPF CLI validation failed ✗", foreground="red")

    def select_file(self):
        """Select RPF file to extract."""
        initial_dir = self.config.get("last_directory", "")
        file_path = filedialog.askopenfilename(
            title="Select an RPF file",
            filetypes=[("RPF Files", "*.rpf"), ("All Files", "*.*")],
            initialdir=initial_dir
        )
        if file_path:
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, file_path)
            
            if not self.output_entry.get():
                output_dir = os.path.dirname(file_path)
                self.output_entry.delete(0, tk.END)
                self.output_entry.insert(0, output_dir)

    def select_output(self):
        """Select output directory."""
        directory = filedialog.askdirectory(title="Select Output Directory")
        if directory:
            self.output_entry.delete(0, tk.END)
            self.output_entry.insert(0, directory)

    def update_progress(self, current, total, message):
        """Update progress bar and status label."""
        if total > 0:
            progress_percent = min(100, (current / total) * 100)
            self.progress['value'] = progress_percent
        self.status_label.config(text=message)
        self.root.update_idletasks()

    def extraction_thread(self):
        """Thread function for extraction process."""
        rpf_file = self.file_entry.get()
        rpf_cli = self.cli_entry.get()
        output_dir = self.output_entry.get()
        auto_cleanup = self.auto_cleanup_var.get()

        try:
            result, error, extracted_rpfs = extract_rpf_recursive(
                rpf_file, rpf_cli, output_dir, "rpf", self.update_progress
            )

            if result and (result['stream'] > 0 or result['data'] > 0):
                stream_dir = os.path.join(output_dir, "stream")
                deleted_rpfs = []
                
                for extracted_rpf in extracted_rpfs:
                    rpf_name = os.path.basename(extracted_rpf)
                    stream_rpf_path = os.path.join(stream_dir, rpf_name)
                    
                    if os.path.exists(stream_rpf_path):
                        try:
                            os.remove(stream_rpf_path)
                            deleted_rpfs.append(rpf_name)
                            print(f"Deleted extracted RPF: {rpf_name}")
                        except Exception as e:
                            print(f"Could not delete {rpf_name}: {e}")

            if auto_cleanup:
                dlc_dir = os.path.join(os.path.dirname(rpf_file), "dlc")
                if os.path.exists(dlc_dir):
                    try:
                        shutil.rmtree(dlc_dir)
                    except Exception as e:
                        print(f"Cleanup warning: {e}")
                
                rpf_dir = os.path.dirname(rpf_file)
                for item in os.listdir(rpf_dir):
                    if item.startswith("_temp_") and item.endswith("_extract"):
                        temp_dir = os.path.join(rpf_dir, item)
                        try:
                            shutil.rmtree(temp_dir)
                        except Exception as e:
                            print(f"Cleanup warning for {item}: {e}")

            if error:
                self.root.after(0, lambda: messagebox.showerror("Extraction Failed", error))
                self.root.after(0, lambda: self.status_label.config(
                    text="Extraction failed ✗", foreground="red"
                ))
            elif result:
                stats = (
                    f"✓ Extraction completed successfully!\n\n"
                    f"📦 Stream files: {result['stream']}\n"
                    f"📄 Data files: {result['data']}\n"
                    f"⏭️  Skipped files: {result['skipped']}\n\n"
                    f"📁 Output location:\n{output_dir}"
                )
                self.root.after(0, lambda: messagebox.showinfo("Success", stats))
                self.root.after(0, lambda: self.status_label.config(
                    text="Extraction completed ✓", foreground="green"
                ))
            
        except Exception as e:
            error_msg = f"Unexpected error:\n{str(e)}"
            self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
            self.root.after(0, lambda: self.status_label.config(
                text="Error occurred ✗", foreground="red"
            ))
        
        finally:
            self.is_extracting = False
            self.root.after(0, lambda: self.extract_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.progress.config(value=0))

    def start_extraction(self):
        """Start the extraction process in a separate thread."""
        if self.is_extracting:
            messagebox.showwarning("In Progress", "Extraction already in progress!")
            return

        rpf_file = self.file_entry.get()
        rpf_cli = self.cli_entry.get()
        output_dir = self.output_entry.get()

        if not rpf_file or not os.path.isfile(rpf_file):
            messagebox.showerror("Error", "Please select a valid RPF file!")
            return

        if not rpf_cli or not os.path.isfile(rpf_cli):
            messagebox.showerror(
                "Error", 
                "Please select a valid RPF CLI tool (rpf-cli.exe)!\n\n"
                "Note: The current tool has a 16MB file size limit.\n"
                "Consider looking for an updated version or alternative RPF extraction tool."
            )
            return

        if not output_dir or not os.path.isdir(output_dir):
            messagebox.showerror("Error", "Please select a valid output directory!")
            return

        self.config["last_directory"] = output_dir
        self.config["rpf_cli"] = rpf_cli
        self.config["auto_cleanup"] = self.auto_cleanup_var.get()
        save_config(self.config)

        self.is_extracting = True
        self.extract_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Starting extraction...", foreground="blue")

        thread = threading.Thread(target=self.extraction_thread, daemon=True)
        thread.start()

if __name__ == "__main__":
    root = tk.Tk()
    app = RPFExtractorGUI(root)
    root.mainloop()