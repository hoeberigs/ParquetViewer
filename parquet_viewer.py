import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import threading
import os
import sys

ROWS_PER_PAGE = 500
MAX_COL_WIDTH = 300
MIN_COL_WIDTH = 60


def resolve_parquet_path(path):
    """If path is a part file inside a parquet directory, return the parent dir.
    If path is a directory containing parquet files, return it as-is.
    Otherwise return the file path as-is."""
    if os.path.isdir(path):
        return path
    if os.path.isfile(path):
        basename = os.path.basename(path)
        parent = os.path.dirname(path)
        if basename.startswith("part-"):
            return parent
        siblings = [f for f in os.listdir(parent) if f.startswith("part-") and "parquet" in f.lower()]
        if siblings:
            return parent
    return path


def get_path_size(path):
    """Get total size in bytes for a file or directory."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total


class ParquetViewer(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Parquet Viewer")
        self.geometry("1200x750")
        self.configure(bg="#1e1e2e")
        self.df = None
        self.filtered_df = None
        self.current_page = 0
        self.sort_col = None
        self.sort_asc = True
        self._build_ui()

    def _build_ui(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Treeview",
                        background="#2a2a3e",
                        foreground="#cdd6f4",
                        fieldbackground="#2a2a3e",
                        rowheight=24,
                        font=("Consolas", 10))
        style.configure("Treeview.Heading",
                        background="#313244",
                        foreground="#cba6f7",
                        font=("Segoe UI", 10, "bold"),
                        relief="flat")
        style.map("Treeview.Heading", background=[("active", "#45475a")])
        style.map("Treeview", background=[("selected", "#585b70")])
        style.configure("TScrollbar", background="#313244", troughcolor="#1e1e2e", arrowcolor="#cdd6f4")
        style.configure("TEntry", fieldbackground="#313244", foreground="#cdd6f4", insertcolor="#cdd6f4")

        top_bar = tk.Frame(self, bg="#181825", pady=6, padx=10)
        top_bar.pack(fill="x")

        tk.Button(top_bar, text="Open File", command=self._open_file,
                  bg="#cba6f7", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=12, pady=4, cursor="hand2").pack(side="left", padx=(0, 4))

        tk.Button(top_bar, text="Open Folder", command=self._open_folder,
                  bg="#89b4fa", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=12, pady=4, cursor="hand2").pack(side="left", padx=(0, 4))

        tk.Button(top_bar, text="Export CSV", command=self._export_csv,
                  bg="#a6e3a1", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=12, pady=4, cursor="hand2").pack(side="left", padx=(0, 4))

        tk.Button(top_bar, text="Export XLSX", command=self._export_xlsx,
                  bg="#f9e2af", fg="#1e1e2e", font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=12, pady=4, cursor="hand2").pack(side="left")

        self.file_label = tk.Label(top_bar, text="No file loaded", bg="#181825",
                                   fg="#6c7086", font=("Segoe UI", 9))
        self.file_label.pack(side="left", padx=12)

        self.info_label = tk.Label(top_bar, text="", bg="#181825",
                                   fg="#a6e3a1", font=("Segoe UI", 9))
        self.info_label.pack(side="right", padx=4)

        filter_bar = tk.Frame(self, bg="#1e1e2e", pady=4, padx=10)
        filter_bar.pack(fill="x")

        tk.Label(filter_bar, text="Filter:", bg="#1e1e2e",
                 fg="#89b4fa", font=("Segoe UI", 9)).pack(side="left")

        self.filter_var = tk.StringVar()
        self.filter_var.trace_add("write", self._on_filter_change)
        filter_entry = ttk.Entry(filter_bar, textvariable=self.filter_var, width=40)
        filter_entry.pack(side="left", padx=(4, 16))

        tk.Label(filter_bar, text="Column:", bg="#1e1e2e",
                 fg="#89b4fa", font=("Segoe UI", 9)).pack(side="left")
        self.filter_col_var = tk.StringVar(value="(all columns)")
        self.filter_col_menu = ttk.Combobox(filter_bar, textvariable=self.filter_col_var,
                                             width=22, state="readonly")
        self.filter_col_menu.pack(side="left", padx=(4, 0))
        self.filter_col_menu.bind("<<ComboboxSelected>>", lambda _: self._apply_filter())

        self.page_label = tk.Label(filter_bar, text="", bg="#1e1e2e",
                                   fg="#f38ba8", font=("Segoe UI", 9))
        self.page_label.pack(side="right", padx=4)

        nav = tk.Frame(filter_bar, bg="#1e1e2e")
        nav.pack(side="right", padx=8)
        for text, cmd in [("Prev", self._prev_page), ("Next", self._next_page)]:
            tk.Button(nav, text=text, command=cmd,
                      bg="#313244", fg="#cdd6f4", font=("Segoe UI", 9),
                      relief="flat", padx=8, pady=2, cursor="hand2").pack(side="left", padx=2)

        tree_frame = tk.Frame(self, bg="#1e1e2e")
        tree_frame.pack(fill="both", expand=True, padx=10, pady=(0, 4))

        self.tree = ttk.Treeview(tree_frame, show="headings", selectmode="browse")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")
        self.tree.pack(fill="both", expand=True)

        self.tree.tag_configure("odd", background="#252535")
        self.tree.tag_configure("even", background="#2a2a3e")

        status_bar = tk.Frame(self, bg="#181825", pady=3, padx=10)
        status_bar.pack(fill="x", side="bottom")
        self.status_label = tk.Label(status_bar, text="Ready", bg="#181825",
                                     fg="#6c7086", font=("Segoe UI", 8))
        self.status_label.pack(side="left")

        schema_btn = tk.Button(status_bar, text="Schema / dtypes", command=self._show_schema,
                               bg="#313244", fg="#cdd6f4", font=("Segoe UI", 8),
                               relief="flat", padx=8, pady=1, cursor="hand2")
        schema_btn.pack(side="right")

    def _open_file(self):
        path = filedialog.askopenfilename(
            title="Open Parquet file",
            filetypes=[("Parquet files", "*.parquet *.parq"), ("All files", "*.*")]
        )
        if not path:
            return
        self._load_file_path(resolve_parquet_path(path))

    def _open_folder(self):
        path = filedialog.askdirectory(title="Open Parquet folder")
        if not path:
            return
        self._load_file_path(path)

    def _export_csv(self):
        if self.filtered_df is None:
            messagebox.showinfo("Export", "No data loaded.")
            return
        path = filedialog.asksaveasfilename(
            title="Export to CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if not path:
            return
        self._status("Exporting CSV...")
        threading.Thread(target=self._do_export_csv, args=(path,), daemon=True).start()

    def _do_export_csv(self, path):
        try:
            self.filtered_df.to_csv(path, index=False)
            self.after(0, lambda: self._status(f"Exported to {os.path.basename(path)}"))
            self.after(0, lambda: messagebox.showinfo("Export", f"CSV saved to:\n{path}"))
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Export error", str(e)))
            self.after(0, lambda: self._status("Export failed."))

    def _export_xlsx(self):
        if self.filtered_df is None:
            messagebox.showinfo("Export", "No data loaded.")
            return
        path = filedialog.asksaveasfilename(
            title="Export to Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if not path:
            return
        self._status("Exporting XLSX...")
        threading.Thread(target=self._do_export_xlsx, args=(path,), daemon=True).start()

    def _do_export_xlsx(self, path):
        try:
            self.filtered_df.to_excel(path, index=False, engine="openpyxl")
            self.after(0, lambda: self._status(f"Exported to {os.path.basename(path)}"))
            self.after(0, lambda: messagebox.showinfo("Export", f"Excel saved to:\n{path}"))
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Export error", str(e)))
            self.after(0, lambda: self._status("Export failed."))

    def _load_file_path(self, path):
        name = os.path.basename(path)
        if os.path.isdir(path):
            self._status(f"Loading folder {name}/ ...")
        else:
            self._status(f"Loading {name}...")
        threading.Thread(target=self._load_file, args=(path,), daemon=True).start()

    def _load_file(self, path):
        try:
            df = pd.read_parquet(path)
            self.after(0, lambda: self._on_file_loaded(df, path))
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Load error", str(e)))
            self.after(0, lambda: self._status("Error loading file."))

    def _on_file_loaded(self, df, path):
        self.df = df
        self.filtered_df = df
        self.current_page = 0
        self.sort_col = None
        self.sort_asc = True
        name = os.path.basename(path)
        if os.path.isdir(path):
            name = name + "/"
        self.file_label.config(text=name, fg="#cdd6f4")
        mb = get_path_size(path) / 1_048_576
        self.info_label.config(text=f"{len(df):,} rows  {len(df.columns)} cols  {mb:.1f} MB")
        cols = ["(all columns)"] + list(df.columns)
        self.filter_col_menu.config(values=cols)
        self.filter_col_var.set("(all columns)")
        self.filter_var.set("")
        self._build_columns()
        self._render_page()
        self._status("File loaded.")

    def _build_columns(self):
        self.tree["columns"] = list(self.df.columns)
        sample = self.filtered_df.head(200)
        for col in self.df.columns:
            self.tree.heading(col, text=col,
                              command=lambda c=col: self._sort_by(c))
            max_content = sample[col].astype(str).str.len().max() if len(sample) else 0
            width = max(MIN_COL_WIDTH, min(MAX_COL_WIDTH, max_content * 8, len(col) * 9 + 20))
            self.tree.column(col, width=width, minwidth=MIN_COL_WIDTH, stretch=False)

    def _render_page(self):
        self.tree.delete(*self.tree.get_children())
        df = self.filtered_df
        total = len(df)
        total_pages = max(1, (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)
        start = self.current_page * ROWS_PER_PAGE
        end = min(start + ROWS_PER_PAGE, total)
        page_df = df.iloc[start:end]
        for i, (_, row) in enumerate(page_df.iterrows()):
            tag = "odd" if i % 2 else "even"
            self.tree.insert("", "end", values=list(row.astype(str)), tags=(tag,))
        self.page_label.config(
            text=f"Page {self.current_page + 1} / {total_pages}  ({start+1}-{end} of {total:,})"
        )

    def _sort_by(self, col):
        if self.sort_col == col:
            self.sort_asc = not self.sort_asc
        else:
            self.sort_col = col
            self.sort_asc = True
        self.filtered_df = self.filtered_df.sort_values(col, ascending=self.sort_asc)
        self.current_page = 0
        self._render_page()

    def _on_filter_change(self, *_):
        self._apply_filter()

    def _apply_filter(self):
        if self.df is None:
            return
        query = self.filter_var.get().strip().lower()
        col = self.filter_col_var.get()
        if not query:
            self.filtered_df = self.df
        else:
            if col == "(all columns)":
                mask = self.df.apply(
                    lambda c: c.astype(str).str.lower().str.contains(query, na=False, regex=False)
                ).any(axis=1)
            else:
                mask = self.df[col].astype(str).str.lower().str.contains(query, na=False, regex=False)
            self.filtered_df = self.df[mask]
        self.current_page = 0
        self._render_page()

    def _prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self._render_page()

    def _next_page(self):
        total_pages = (len(self.filtered_df) + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self._render_page()

    def _show_schema(self):
        if self.df is None:
            messagebox.showinfo("Schema", "No file loaded.")
            return
        win = tk.Toplevel(self)
        win.title("Schema / dtypes")
        win.configure(bg="#1e1e2e")
        win.geometry("480x420")
        text = tk.Text(win, bg="#2a2a3e", fg="#cdd6f4", font=("Consolas", 10),
                       relief="flat", padx=10, pady=10)
        text.pack(fill="both", expand=True, padx=10, pady=10)
        lines = [f"{'Column':<35} {'Dtype':<20} {'Non-null'}", "-" * 65]
        total = len(self.df)
        for col in self.df.columns:
            nn = self.df[col].notna().sum()
            lines.append(f"{col:<35} {str(self.df[col].dtype):<20} {nn:,}/{total:,}")
        text.insert("1.0", "\n".join(lines))
        text.config(state="disabled")

    def _status(self, msg):
        self.status_label.config(text=msg)


if __name__ == "__main__":
    app = ParquetViewer()
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        path = resolve_parquet_path(sys.argv[1])
        app.after(100, lambda: app._load_file_path(path))
    app.mainloop()
