"""Headless smoke test for the bundled ParquetViewer.

Creates a test parquet file, then imports the viewer module and verifies:
1. All dependencies load (pandas, pyarrow, openpyxl, tkinter)
2. Parquet read works
3. CSV export works
4. XLSX export works
5. Folder/part-file resolution works
"""
import subprocess
import sys
import os
import tempfile

def main():
    failures = []

    # Test 1: imports
    print("Test 1: Checking imports...")
    try:
        import pandas as pd
        import pyarrow
        import openpyxl
        import tkinter
        print("  PASS: All imports OK")
    except ImportError as e:
        failures.append(f"Import failed: {e}")
        print(f"  FAIL: {e}")

    # Test 2: create and read parquet
    print("Test 2: Parquet read/write...")
    try:
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95, 87, 92],
            "passed": [True, True, True],
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            pq_path = os.path.join(tmpdir, "test.parquet")
            df.to_parquet(pq_path)
            df2 = pd.read_parquet(pq_path)
            assert len(df2) == 3, f"Expected 3 rows, got {len(df2)}"
            assert list(df2.columns) == ["name", "score", "passed"]
            print("  PASS: Parquet read/write OK")
    except Exception as e:
        failures.append(f"Parquet read/write: {e}")
        print(f"  FAIL: {e}")

    # Test 3: CSV export
    print("Test 3: CSV export...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test.csv")
            df.to_csv(csv_path, index=False)
            assert os.path.getsize(csv_path) > 0
            print("  PASS: CSV export OK")
    except Exception as e:
        failures.append(f"CSV export: {e}")
        print(f"  FAIL: {e}")

    # Test 4: XLSX export
    print("Test 4: XLSX export...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            xlsx_path = os.path.join(tmpdir, "test.xlsx")
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
            assert os.path.getsize(xlsx_path) > 0
            print("  PASS: XLSX export OK")
    except Exception as e:
        failures.append(f"XLSX export: {e}")
        print(f"  FAIL: {e}")

    # Test 5: parquet directory (partitioned) read
    print("Test 5: Partitioned parquet folder read...")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            part_dir = os.path.join(tmpdir, "data.parquet")
            os.makedirs(part_dir)
            for i in range(3):
                chunk = df.iloc[i:i+1]
                chunk.to_parquet(os.path.join(part_dir, f"part-{i:05d}.snappy.parquet"))
            df3 = pd.read_parquet(part_dir)
            assert len(df3) == 3, f"Expected 3 rows from folder, got {len(df3)}"
            print("  PASS: Folder read OK")
    except Exception as e:
        failures.append(f"Folder read: {e}")
        print(f"  FAIL: {e}")

    # Test 6: resolve_parquet_path logic
    print("Test 6: Path resolution logic...")
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from parquet_viewer import resolve_parquet_path
        with tempfile.TemporaryDirectory() as tmpdir:
            part_dir = os.path.join(tmpdir, "dataset")
            os.makedirs(part_dir)
            part_file = os.path.join(part_dir, "part-00000.snappy.parquet")
            df.to_parquet(part_file)
            resolved = resolve_parquet_path(part_file)
            assert resolved == part_dir, f"Expected {part_dir}, got {resolved}"
            resolved2 = resolve_parquet_path(part_dir)
            assert resolved2 == part_dir
            print("  PASS: Path resolution OK")
    except Exception as e:
        failures.append(f"Path resolution: {e}")
        print(f"  FAIL: {e}")

    print()
    if failures:
        print(f"FAILED ({len(failures)} failures):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
