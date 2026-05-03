"""Pandera schema proposer (tkinter GUI).

Reads a CSV/XLSX/TXT file, infers a `pa.DataFrameModel` from the sample, and
either copies the generated code to the clipboard or writes it to
`src/medallion_etl/schemas/bronze/<name>.py`.

Run:
    python tools/schema_proposer.py
"""

from __future__ import annotations

import re
import sys
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "src" / "medallion_etl" / "schemas" / "bronze"

SEPARATORS = {"auto": None, "comma": ",", "semicolon": ";", "tab": "\t", "pipe": "|"}


# --------------------------------------------------------------------------- #
# Schema generation
# --------------------------------------------------------------------------- #


def _pa_type(series: pd.Series) -> str:
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "bool"
    if pd.api.types.is_integer_dtype(dtype):
        return "int"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "pd.DatetimeTZDtype" if "tz" in str(dtype) or "UTC" in str(dtype) else "pd.Timestamp"
    return "str"


def _ident(name: str) -> str:
    s = re.sub(r"\W+", "_", str(name)).strip("_").lower()
    if not s or s[0].isdigit():
        s = "col_" + s
    return s


def _filename_for(class_name: str) -> str:
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()
    if snake.startswith("bronze_"):
        snake = snake[len("bronze_") :]
    return f"{snake or 'schema'}.py"


def generate_schema(df: pd.DataFrame, class_name: str) -> str:
    needs_pd = any(_pa_type(df[c]).startswith("pd.") for c in df.columns)

    lines: list[str] = []
    if needs_pd:
        lines.append("import pandas as pd")
    lines += ["import pandera.pandas as pa", "from pandera.typing import Series", "", ""]
    lines.append(f"class {class_name}(pa.DataFrameModel):")
    lines.append('    """Auto-generated from sample data — review before committing."""')
    lines.append("")

    for col in df.columns:
        py_type = _pa_type(df[col])
        ident = _ident(col)
        nullable = bool(df[col].isna().any())
        try:
            unique = bool(df[col].is_unique) and not nullable
        except TypeError:
            unique = False

        opts: list[str] = []
        if ident != str(col):
            opts.append(f'alias="{col}"')
        if nullable:
            opts.append("nullable=True")
        if unique:
            opts.append("unique=True")
        field = f" = pa.Field({', '.join(opts)})" if opts else ""
        lines.append(f"    {ident}: Series[{py_type}]{field}")

    lines += [
        "",
        "    class Config:",
        "        strict = False",
        "        coerce = True",
        "",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# File reading
# --------------------------------------------------------------------------- #


def read_file(
    path: Path,
    *,
    encoding: str,
    separator: str,
    sheet: str | None,
    nrows: int | None,
) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf in (".csv", ".txt", ""):
        kwargs: dict = {"encoding": encoding, "nrows": nrows}
        sep = SEPARATORS.get(separator)
        if sep is None:
            kwargs["sep"] = None
            kwargs["engine"] = "python"
        else:
            kwargs["sep"] = sep
        return pd.read_csv(path, **kwargs)
    if suf in (".xlsx", ".xls"):
        kwargs = {"nrows": nrows}
        if sheet:
            kwargs["sheet_name"] = sheet
        return pd.read_excel(path, **kwargs)
    raise ValueError(f"unsupported file extension: {suf!r}")


# --------------------------------------------------------------------------- #
# GUI
# --------------------------------------------------------------------------- #


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Pandera Schema Proposer")
        self.geometry("960x780")
        self.minsize(720, 560)

        self._df: pd.DataFrame | None = None
        self._build()

    def _build(self) -> None:
        # Pack bottom widgets FIRST so they're always visible regardless of
        # window height. Status bar -> action bar -> form (top) -> output (fills).
        self.status = tk.StringVar(value="Ready.")
        ttk.Label(
            self, textvariable=self.status, anchor=tk.W, relief=tk.SUNKEN, padding=(8, 4)
        ).pack(side=tk.BOTTOM, fill=tk.X)

        act = ttk.Frame(self, padding=12)
        act.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Button(act, text="📋 Copy to clipboard", command=self._copy, width=22).pack(
            side=tk.LEFT, padx=4, ipady=4
        )
        ttk.Button(
            act, text="💾 Save to schemas/bronze/", command=self._save, width=28
        ).pack(side=tk.LEFT, padx=4, ipady=4)

        form = ttk.Frame(self, padding=14)
        form.pack(side=tk.TOP, fill=tk.X)
        form.columnconfigure(1, weight=1)

        # File
        ttk.Label(form, text="File").grid(row=0, column=0, sticky=tk.W, padx=4, pady=4)
        self.file_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.file_var).grid(
            row=0, column=1, sticky=tk.EW, padx=4, pady=4
        )
        ttk.Button(form, text="Browse…", command=self._browse).grid(
            row=0, column=2, padx=4, pady=4
        )

        # Encoding
        ttk.Label(form, text="Encoding").grid(row=1, column=0, sticky=tk.W, padx=4, pady=4)
        self.enc_var = tk.StringVar(value="utf-8")
        ttk.Combobox(
            form, textvariable=self.enc_var, width=18,
            values=["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"],
        ).grid(row=1, column=1, sticky=tk.W, padx=4, pady=4)

        # Separator
        ttk.Label(form, text="Separator (csv/txt)").grid(row=2, column=0, sticky=tk.W, padx=4, pady=4)
        self.sep_var = tk.StringVar(value="auto")
        ttk.Combobox(
            form, textvariable=self.sep_var, width=18,
            values=list(SEPARATORS.keys()),
        ).grid(row=2, column=1, sticky=tk.W, padx=4, pady=4)

        # Sheet
        ttk.Label(form, text="Sheet (xlsx)").grid(row=3, column=0, sticky=tk.W, padx=4, pady=4)
        self.sheet_var = tk.StringVar()
        ttk.Entry(form, textvariable=self.sheet_var, width=22).grid(
            row=3, column=1, sticky=tk.W, padx=4, pady=4
        )

        # Sample rows
        ttk.Label(form, text="Sample rows").grid(row=4, column=0, sticky=tk.W, padx=4, pady=4)
        self.nrows_var = tk.StringVar(value="1000")
        ttk.Entry(form, textvariable=self.nrows_var, width=10).grid(
            row=4, column=1, sticky=tk.W, padx=4, pady=4
        )

        # Class name
        ttk.Label(form, text="Class name").grid(row=5, column=0, sticky=tk.W, padx=4, pady=4)
        self.cls_var = tk.StringVar(value="BronzeMySchema")
        ttk.Entry(form, textvariable=self.cls_var, width=32).grid(
            row=5, column=1, sticky=tk.W, padx=4, pady=4
        )

        ttk.Button(form, text="Read & Infer", command=self._infer).grid(
            row=6, column=1, sticky=tk.W, padx=4, pady=12
        )

        # Output (fills remaining middle space)
        out_frame = ttk.LabelFrame(self, text="Generated schema", padding=8)
        out_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=14, pady=4)
        self.out = tk.Text(out_frame, font=("Consolas", 10), wrap=tk.NONE, undo=True)
        yscroll = ttk.Scrollbar(out_frame, orient=tk.VERTICAL, command=self.out.yview)
        xscroll = ttk.Scrollbar(out_frame, orient=tk.HORIZONTAL, command=self.out.xview)
        self.out.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.out.grid(row=0, column=0, sticky=tk.NSEW)
        yscroll.grid(row=0, column=1, sticky=tk.NS)
        xscroll.grid(row=1, column=0, sticky=tk.EW)
        out_frame.rowconfigure(0, weight=1)
        out_frame.columnconfigure(0, weight=1)


    # -- handlers -------------------------------------------------------- #

    def _browse(self) -> None:
        path = filedialog.askopenfilename(
            filetypes=[
                ("All", "*.*"),
                ("CSV", "*.csv"),
                ("TXT", "*.txt"),
                ("Excel", "*.xlsx *.xls"),
            ],
        )
        if path:
            self.file_var.set(path)
            stem = Path(path).stem
            self.cls_var.set(self._suggest_class_name(stem))

    @staticmethod
    def _suggest_class_name(stem: str) -> str:
        parts = [p for p in re.split(r"[\W_]+", stem) if p]
        camel = "".join(p.capitalize() for p in parts) or "Schema"
        return f"Bronze{camel}"

    def _infer(self) -> None:
        path_s = self.file_var.get().strip()
        if not path_s:
            messagebox.showerror("No file", "Pick a file first.")
            return
        path = Path(path_s)
        if not path.exists():
            messagebox.showerror("Missing", f"File not found:\n{path}")
            return
        try:
            nrows = int(self.nrows_var.get() or "0") or None
        except ValueError:
            nrows = None
        try:
            df = read_file(
                path,
                encoding=self.enc_var.get() or "utf-8",
                separator=self.sep_var.get() or "auto",
                sheet=self.sheet_var.get().strip() or None,
                nrows=nrows,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Read failed", f"{type(exc).__name__}: {exc}")
            return

        if df.empty:
            messagebox.showwarning("Empty", "File parsed but produced 0 rows.")
        self._df = df
        code = generate_schema(df, self.cls_var.get().strip() or "Schema")
        self.out.delete("1.0", tk.END)
        self.out.insert(tk.END, code)
        self.status.set(
            f"{len(df):,} rows · {len(df.columns)} columns inferred from {path.name}"
        )

    def _current_code(self) -> str:
        return self.out.get("1.0", tk.END).rstrip() + "\n"

    def _copy(self) -> None:
        code = self._current_code()
        if not code.strip():
            self.status.set("Nothing to copy yet — Read & Infer first.")
            return
        self.clipboard_clear()
        self.clipboard_append(code)
        self.update()  # ensure clipboard persists after window closes
        self.status.set("Copied to clipboard.")

    def _save(self) -> None:
        code = self._current_code()
        if not code.strip():
            self.status.set("Nothing to save yet — Read & Infer first.")
            return
        cls = self.cls_var.get().strip() or "Schema"
        filename = _filename_for(cls)
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        target = BRONZE_DIR / filename
        if target.exists() and not messagebox.askyesno(
            "Overwrite?", f"{target.relative_to(PROJECT_ROOT)} exists. Overwrite?"
        ):
            return
        target.write_text(code, encoding="utf-8")
        self.status.set(f"Saved {target.relative_to(PROJECT_ROOT)}")


def main() -> int:
    App().mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
