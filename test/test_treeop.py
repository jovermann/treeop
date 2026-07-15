import os
import re
import subprocess
import time
from pathlib import Path
import pytest


def treeop_bin() -> Path:
    root = Path(__file__).resolve().parents[1]
    bin_path = Path(os.environ.get("TREEOP_BIN", root / "treeop"))
    return bin_path


def run_treeop(args, cwd: Path):
    bin_path = treeop_bin()
    if "TREEOP_BIN" not in os.environ:
        subprocess.run(["make"], cwd=cwd, check=True, capture_output=True, text=True)
    if not bin_path.exists():
        subprocess.run(["make"], cwd=cwd, check=True, capture_output=True, text=True)
        if not bin_path.exists():
            raise FileNotFoundError(f"treeop binary not found after make: {bin_path}")
    result = subprocess.run(
        [str(bin_path)] + args,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def run_treeop_result(args, cwd: Path):
    bin_path = treeop_bin()
    if "TREEOP_BIN" not in os.environ:
        subprocess.run(["make"], cwd=cwd, check=True, capture_output=True, text=True)
    if not bin_path.exists():
        subprocess.run(["make"], cwd=cwd, check=True, capture_output=True, text=True)
        if not bin_path.exists():
            raise FileNotFoundError(f"treeop binary not found after make: {bin_path}")
    return subprocess.run(
        [str(bin_path)] + args,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


def write_file(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def extract_hash(output: str, filename: str) -> str:
    for line in output.splitlines():
        if line.endswith(filename):
            parts = line.split()
            if len(parts) >= 2:
                return parts[1]
    raise AssertionError(f"hash not found for {filename} in output:\n{output}")


def test_intersect_stats_two_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same.txt", "hello")
    write_file(dir_a / "onlyA.txt", "only a")
    write_file(dir_b / "same.txt", "hello")
    write_file(dir_b / "onlyB.txt", "only b")

    out = run_treeop(["--intersect", str(dir_a), str(dir_b)], root)
    assert f"{dir_a}:" in out
    assert f"{dir_b}:" in out
    assert "unique-files:" in out
    assert "shared-files:" in out
    assert "total:" in out
    assert "total-files:" in out
    assert out.splitlines().count("----------------------------------------") == 2


def test_intersect_min_size_filters_file_sets(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "small-shared.txt", "xx")
    write_file(dir_b / "small-shared.txt", "xx")
    write_file(dir_a / "large-shared.txt", "large-data")
    write_file(dir_b / "large-shared.txt", "large-data")

    out = run_treeop(["--intersect", "--min-size", "5", str(dir_a), str(dir_b)], root)
    total_section = out.split("total:\n", 1)[1]

    assert re.search(r"total-files:\s+2", total_section)
    assert re.search(r"total-size:\s+20 bytes", total_section)
    assert re.search(r"shared-files:\s+2", total_section)
    assert "small-shared.txt" not in out


def test_containment_reports_nested_complete_mostly_and_missing_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "complete" / "nested" / "all.txt", "all")
    write_file(dir_b / "copies" / "all-copy.txt", "all")

    for i in range(10):
        write_file(dir_a / "mostly" / f"match-{i}.txt", f"match-{i}")
        write_file(dir_b / "elsewhere" / f"copy-{i}.txt", f"match-{i}")
    write_file(dir_a / "mostly" / "missing.txt", "missing")
    write_file(dir_a / "absent" / "only-a.txt", "only-a")
    write_file(dir_a / "absent" / "deep" / "nested.txt", "nested-only-a")
    write_file(dir_a / "mostly_not" / "match.txt", "mostly-not-match")
    write_file(dir_b / "mostly-not-copy.txt", "mostly-not-match")
    for i in range(10):
        write_file(dir_a / "mostly_not" / f"missing-{i}.txt", f"mostly-not-missing-{i}")
    write_file(dir_b / "extra" / "only-b.txt", "only-b")

    out = run_treeop(["--containment", str(dir_b), str(dir_a)], root)

    assert f"{dir_a} in previous roots ({dir_b}):" in out
    assert f"{dir_b} in previous roots" not in out
    assert "complete-dirs:" in out
    assert re.search(r"complete .*files=1 / 1 \(100.0%\)", out)
    assert "complete/nested" not in out
    assert "mostly-contained-dirs:" in out
    assert re.search(r"mostly .*files=10 / 11 \(90.9%\)", out)
    assert "mostly-not-contained-dirs:" not in out
    assert "not-contained-dirs:" not in out

    out = run_treeop(["--containment", "--show-not-contained", str(dir_b), str(dir_a)], root)

    assert "mostly-not-contained-dirs:" in out
    assert re.search(r"mostly_not .*files=1 / 11 \(9.1%\)", out)
    assert "not-contained-dirs:" in out
    assert re.search(r"absent .*files=0 / 2 \(0.0%\)", out)
    assert "absent/deep" not in out


def test_containment_combines_first_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_c = tmp_path / "c"
    dir_a.mkdir()
    dir_b.mkdir()
    dir_c.mkdir()

    write_file(dir_a / "copy-one.txt", "one")
    write_file(dir_b / "copy-two.txt", "two")
    write_file(dir_c / "nested" / "one.txt", "one")
    write_file(dir_c / "nested" / "two.txt", "two")

    out = run_treeop(["-c", str(dir_a), str(dir_b), str(dir_c)], root)

    assert f"{dir_c} in previous roots ({dir_a}, {dir_b}):" in out
    assert re.search(r"files:\s+2 / 2 \(100.0%\)", out)
    assert re.search(r"\n  \. files=2 / 2 \(100.0%\)", out)
    assert "nested" not in out


def test_containment_suppresses_children_when_root_complete(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy.txt", "same")
    write_file(dir_b / "x" / "y" / "file.txt", "same")

    out = run_treeop(["--containment", str(dir_a), str(dir_b)], root)

    assert re.search(r"\n  \. files=1 / 1 \(100.0%\)", out)
    assert "x/y" not in out


def test_containment_suppresses_children_when_root_not_contained(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "different.txt", "different")
    write_file(dir_b / "x" / "y" / "file.txt", "missing")

    out = run_treeop(["--containment", "--show-not-contained", str(dir_a), str(dir_b)], root)

    assert re.search(r"\n  \. files=0 / 1 \(0.0%\)", out)
    assert "x/y" not in out


def test_containment_file_lists_honor_min_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy-contained.txt", "contained-large")
    write_file(dir_a / "copy-small.txt", "xx")
    write_file(dir_b / "contained.txt", "contained-large")
    write_file(dir_b / "missing.txt", "missing-large")
    write_file(dir_b / "small-contained.txt", "xx")
    write_file(dir_b / "small-missing.txt", "yy")

    out = run_treeop([
        "--containment",
        "--show-contained-files",
        "--show-not-contained-files",
        "--min-size",
        "5",
        str(dir_a),
        str(dir_b),
    ], root)

    contained = out.split("contained-files:\n", 1)[1].split("not-contained-files:\n", 1)[0]
    not_contained = out.split("not-contained-files:\n", 1)[1]

    assert "contained.txt" in contained
    assert "missing.txt" not in contained
    assert "small-contained.txt" not in contained
    assert "missing.txt" in not_contained
    assert "contained.txt" not in not_contained
    assert "small-missing.txt" not in not_contained


def test_show_contained_files_lists_contained_files(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy.txt", "contained")
    write_file(dir_b / "contained.txt", "contained")
    write_file(dir_b / "missing.txt", "missing")

    out = run_treeop(["--containment", "--show-contained-files", str(dir_a), str(dir_b)], root)

    contained = out.split("contained-files:\n", 1)[1]
    assert "contained.txt" in contained
    assert "missing.txt" not in contained


def test_containment_file_lists_require_containment(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    result = run_treeop_result(["--show-contained-files", str(dir_a), str(dir_b)], root)
    assert result.returncode != 0
    assert "--show-contained-files/--show-not-contained-files require --containment." in result.stdout
    assert not (dir_a / ".dirdb").exists()
    assert not (dir_b / ".dirdb").exists()


def test_show_not_contained_requires_containment(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    result = run_treeop_result(["--show-not-contained", str(dir_a), str(dir_b)], root)
    assert result.returncode != 0
    assert "--show-not-contained requires --containment." in result.stdout
    assert not (dir_a / ".dirdb").exists()
    assert not (dir_b / ".dirdb").exists()


def test_containment_requires_at_least_two_dirs_before_processing(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    result = run_treeop_result(["--containment", str(dir_a)], root)
    assert result.returncode != 0
    assert "--containment requires at least two directories." in result.stdout
    assert not (dir_a / ".dirdb").exists()


def test_remove_contained_dirs_dry_run(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy-one.txt", "one")
    write_file(dir_a / "copy-two.txt", "two")
    write_file(dir_b / "complete" / "nested" / "one.txt", "one")
    write_file(dir_b / "complete" / "two.txt", "two")
    write_file(dir_b / "partial" / "one.txt", "one")
    write_file(dir_b / "partial" / "unique.txt", "unique")

    out = run_treeop(["--containment", "--remove-contained-dirs", "--dry-run", str(dir_a), str(dir_b)], root)

    assert "remove-contained-dirs:" in out
    assert f"Would remove dir {dir_b / 'complete'}" in out
    assert re.search(r"removed-dirs:\s+2", out)
    assert re.search(r"removed-files:\s+2", out)
    assert (dir_b / "complete" / "nested" / "one.txt").exists()
    assert (dir_b / "partial" / "unique.txt").exists()


def test_remove_contained_dirs_actual(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy-one.txt", "one")
    write_file(dir_a / "copy-two.txt", "two")
    write_file(dir_b / "complete" / "nested" / "one.txt", "one")
    write_file(dir_b / "complete" / "two.txt", "two")
    write_file(dir_b / "partial" / "one.txt", "one")
    write_file(dir_b / "partial" / "unique.txt", "unique")

    out = run_treeop(["--containment", "--remove-contained-dirs", str(dir_a), str(dir_b)], root)

    assert "remove-contained-dirs:" in out
    assert re.search(r"removed-dirs:\s+2", out)
    assert re.search(r"removed-files:\s+2", out)
    assert not (dir_b / "complete").exists()
    assert (dir_b / "partial" / "one.txt").exists()
    assert (dir_b / "partial" / "unique.txt").exists()


def test_remove_contained_requires_containment(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    result = run_treeop_result(["--remove-contained-dirs", str(dir_a), str(dir_b)], root)

    assert result.returncode != 0
    assert "--remove-contained-dirs/--remove-contained-files require --containment." in result.stdout
    assert not (dir_a / ".dirdb").exists()
    assert not (dir_b / ".dirdb").exists()


def test_remove_contained_files_dry_run(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy.txt", "contained")
    write_file(dir_b / "contained.txt", "contained")
    write_file(dir_b / "missing.txt", "missing")

    out = run_treeop(["--containment", "--remove-contained-files", "--dry-run", str(dir_a), str(dir_b)], root)

    assert "remove-contained-files:" in out
    assert f"Would remove {dir_b / 'contained.txt'}" in out
    assert re.search(r"removed-files:\s+1", out)
    assert (dir_b / "contained.txt").exists()
    assert (dir_b / "missing.txt").exists()


def test_remove_contained_files_actual(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "copy.txt", "contained")
    write_file(dir_a / "small-copy.txt", "xx")
    write_file(dir_b / "contained.txt", "contained")
    write_file(dir_b / "small-contained.txt", "xx")
    write_file(dir_b / "missing.txt", "missing")

    out = run_treeop(["--containment", "--remove-contained-files", "--min-size", "5", str(dir_a), str(dir_b)], root)

    assert "remove-contained-files:" in out
    assert re.search(r"removed-files:\s+1", out)
    assert not (dir_b / "contained.txt").exists()
    assert (dir_b / "small-contained.txt").exists()
    assert (dir_b / "missing.txt").exists()


def test_remove_contained_dirs_and_files_conflict(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    result = run_treeop_result(["--containment", "--remove-contained-dirs", "--remove-contained-files", str(dir_a), str(dir_b)], root)

    assert result.returncode != 0
    assert "Cannot combine --remove-contained-dirs with --remove-contained-files." in result.stdout
    assert not (dir_a / ".dirdb").exists()
    assert not (dir_b / ".dirdb").exists()


def test_find_overlapping_dirs_top_lists_best_pair(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "source" / "shared-one.txt", "shared-one")
    write_file(dir_a / "source" / "shared-two.txt", "shared-two")
    write_file(dir_a / "source" / "only-a.txt", "only-a")
    write_file(dir_b / "copy" / "copy-one.txt", "shared-one")
    write_file(dir_b / "copy" / "copy-two.txt", "shared-two")
    write_file(dir_b / "copy" / "only-b.txt", "only-b")
    write_file(dir_b / "partial" / "copy-one.txt", "shared-one")
    write_file(dir_b / "partial" / "different.txt", "different")

    out = run_treeop(["--find-overlapping-dirs", "--top", "1", str(dir_a), str(dir_b)], root)

    assert "overlapping-dirs:" in out
    assert f"A: {dir_a / 'source'}" in out or f"A: {dir_b / 'copy'}" in out
    assert f"B: {dir_b / 'copy'}" in out or f"B: {dir_a / 'source'}" in out
    assert "partial" not in out
    assert "shared in files:" not in out
    assert "shared in bytes:" not in out
    assert re.search(r"shared:\s+\d+\.?\d*%/\s*\d+\.?\d* bytes,\s+66\.7%/\s*2 files", out)
    assert re.search(r"only in A:\s+\d+\.?\d*%/\s*\d+\.?\d* bytes,\s+33\.3%/\s*1 files\s+\(\d+\.?\d* bytes/3 files total in A\)", out)
    assert re.search(r"only in B:\s+\d+\.?\d*%/\s*\d+\.?\d* bytes,\s+33\.3%/\s*1 files\s+\(\d+\.?\d* bytes/3 files total in B\)", out)
    assert out.index("shared:") < out.index("only in A:")


def test_find_overlapping_dirs_honors_min_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "x" / "small.txt", "hi")
    write_file(dir_b / "y" / "small-copy.txt", "hi")
    write_file(dir_a / "x" / "large.txt", "large-shared")
    write_file(dir_b / "y" / "large-copy.txt", "large-shared")

    out = run_treeop(["--find-overlapping-dirs", "--top", "1", "--min-size", "5", str(dir_a), str(dir_b)], root)

    assert re.search(r"shared:\s+100\.0%/\s*\d+\.?\d* bytes,\s+100\.0%/\s*1 files", out)


def test_find_overlapping_dirs_sorts_by_shared_bytes_percent(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "mostly_bytes" / "big.txt", "x" * 100)
    write_file(dir_a / "mostly_bytes" / "small-unique.txt", "u")
    write_file(dir_b / "big_copy" / "big-copy.txt", "x" * 100)

    write_file(dir_a / "mostly_files" / "one.txt", "one")
    write_file(dir_a / "mostly_files" / "two.txt", "two")
    write_file(dir_a / "mostly_files" / "large-unique.txt", "y" * 100)
    write_file(dir_b / "small_copies" / "one-copy.txt", "one")
    write_file(dir_b / "small_copies" / "two-copy.txt", "two")

    out = run_treeop(["--find-overlapping-dirs", "--top", "1", str(dir_a), str(dir_b)], root)

    assert f"A: {dir_b / 'big_copy'}" in out
    assert f"B: {dir_a / 'mostly_bytes'}" in out
    assert "mostly_files" not in out


def test_find_overlapping_dirs_omits_zero_shared_bytes(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "x" / "one.txt", "one")
    write_file(dir_b / "y" / "two.txt", "two")

    out = run_treeop(["--find-overlapping-dirs", str(dir_a), str(dir_b)], root)

    assert "overlapping-dirs:" in out
    assert "  (none)" in out
    assert "A:" not in out
    assert "shared:" not in out


def test_find_overlapping_dirs_prints_only_best_direction_per_pair(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "large" / "shared.txt", "shared")
    write_file(dir_a / "large" / "unique.txt", "unique")
    write_file(dir_b / "small" / "copy.txt", "shared")

    out = run_treeop(["--find-overlapping-dirs", str(dir_a), str(dir_b)], root)

    assert out.count("\nA: ") == 1
    assert f"A: {dir_b / 'small'}" in out
    assert f"B: {dir_a / 'large'}" in out


def test_find_overlapping_dirs_warns_and_skips_remove_when_internal_duplicates(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "dupdir" / "one.txt", "same")
    write_file(dir_a / "dupdir" / "two.txt", "same")
    write_file(dir_b / "copydir" / "copy.txt", "same")

    out = run_treeop(["--find-overlapping-dirs", "--remove-copies", "--dry-run", "--top", "1", str(dir_a), str(dir_b)], root)

    assert "warning: B contains internal duplicates:" in out or "warning: A contains internal duplicates:" in out
    assert re.search(r"remove from A:\s+0 bytes,\s+0 files", out)
    assert re.search(r"remove from B:\s+0 bytes,\s+0 files", out)
    assert (dir_a / "dupdir" / "one.txt").exists()
    assert (dir_a / "dupdir" / "two.txt").exists()
    assert (dir_b / "copydir" / "copy.txt").exists()


def test_remove_dir_internal_copies_dry_run_verbose(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    old = dir_a / "dupdir" / "old.txt"
    new = dir_a / "dupdir" / "new.txt"
    write_file(old, "same")
    write_file(new, "same")
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    out = run_treeop(["--remove-dir-internal-copies", "--dry-run", "-vvv", str(dir_a)], root)

    assert "remove-dir-internal-copies:" in out
    assert re.search(r"removed-files:\s+1", out)
    assert re.search(r"^[0-9a-f]+: Would remove ", out, re.MULTILINE)
    assert f"Would remove {new}" in out
    assert "kept old.txt" in out
    assert f"kept {old}" not in out
    assert "removed-date=" in out
    assert "kept-date=" in out
    assert "size=4 bytes" in out
    assert "hash=" not in out
    assert old.exists()
    assert new.exists()


def test_remove_dir_internal_copies_verbose_prints_kept_file(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    old = dir_a / "dupdir" / "old.txt"
    new = dir_a / "dupdir" / "new.txt"
    write_file(old, "same")
    write_file(new, "same")
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    out = run_treeop(["--remove-dir-internal-copies", "--dry-run", "-v", str(dir_a)], root)

    assert re.search(r"^[0-9a-f]+: Would remove ", out, re.MULTILINE)
    assert f"Would remove {new}" in out
    assert "kept old.txt" in out
    assert "removed-date=" not in out
    assert "size=4 bytes" not in out


def test_remove_dir_internal_copies_actual(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    old = dir_a / "dupdir" / "old.txt"
    new = dir_a / "dupdir" / "new.txt"
    write_file(old, "same")
    write_file(new, "same")
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    out = run_treeop(["--remove-dir-internal-copies", str(dir_a)], root)

    assert "remove-dir-internal-copies:" in out
    assert re.search(r"removed-files:\s+1", out)
    assert old.exists()
    assert not new.exists()


def test_remove_dir_internal_copies_runs_before_overlap_remove_copies(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    a_old = dir_a / "dupdir" / "old.txt"
    a_new = dir_a / "dupdir" / "new.txt"
    b_copy = dir_b / "copydir" / "copy.txt"
    write_file(a_old, "same")
    write_file(a_new, "same")
    write_file(b_copy, "same")
    os.utime(a_old, (1000, 1000))
    os.utime(a_new, (2000, 2000))
    os.utime(b_copy, (3000, 3000))

    out = run_treeop([
        "--remove-dir-internal-copies",
        "--find-overlapping-dirs",
        "--remove-copies",
        "--top",
        "1",
        str(dir_a),
        str(dir_b),
    ], root)

    assert "warning:" not in out
    assert re.search(r"remove from B:\s+\d+ bytes,\s+1 files", out)
    assert a_old.exists()
    assert not a_new.exists()
    assert not b_copy.exists()


def test_find_overlapping_dirs_remove_copies_dry_run_and_verbose(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    a_old = dir_a / "pair" / "a-old.txt"
    a_new = dir_a / "pair" / "a-new.txt"
    b_old = dir_b / "pair" / "b-old.txt"
    b_new = dir_b / "pair" / "b-new.txt"
    write_file(a_old, "keep-a")
    write_file(b_new, "keep-a")
    write_file(b_old, "keep-b")
    write_file(a_new, "keep-b")
    os.utime(a_old, (1000, 1000))
    os.utime(b_new, (2000, 2000))
    os.utime(b_old, (1000, 1000))
    os.utime(a_new, (2000, 2000))

    out = run_treeop([
        "--find-overlapping-dirs",
        "--remove-copies",
        "--dry-run",
        "-vv",
        "--top",
        "1",
        str(dir_a),
        str(dir_b),
    ], root)

    assert re.search(r"remove from A:\s+\d+\.?\d* bytes,\s+1 files", out)
    assert re.search(r"remove from B:\s+\d+\.?\d* bytes,\s+1 files", out)
    assert f"Would remove A {a_new}" in out
    assert f"Would remove B {b_new}" in out
    assert f"kept {b_old}" in out
    assert f"kept {a_old}" in out
    assert "removed-date=" in out
    assert "kept-date=" in out
    assert a_old.exists()
    assert a_new.exists()
    assert b_old.exists()
    assert b_new.exists()


def test_find_overlapping_dirs_remove_copies_actual_preserves_oldest(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    a_old = dir_a / "pair" / "a-old.txt"
    a_new = dir_a / "pair" / "a-new.txt"
    b_old = dir_b / "pair" / "b-old.txt"
    b_new = dir_b / "pair" / "b-new.txt"
    write_file(a_old, "keep-a")
    write_file(b_new, "keep-a")
    write_file(b_old, "keep-b")
    write_file(a_new, "keep-b")
    os.utime(a_old, (1000, 1000))
    os.utime(b_new, (2000, 2000))
    os.utime(b_old, (1000, 1000))
    os.utime(a_new, (2000, 2000))

    out = run_treeop(["--find-overlapping-dirs", "--remove-copies", "--top", "1", str(dir_a), str(dir_b)], root)

    assert re.search(r"remove from A:\s+\d+\.?\d* bytes,\s+1 files", out)
    assert re.search(r"remove from B:\s+\d+\.?\d* bytes,\s+1 files", out)
    assert a_old.exists()
    assert not a_new.exists()
    assert b_old.exists()
    assert not b_new.exists()


def test_top_requires_find_overlapping_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()

    result = run_treeop_result(["--top", "1", str(dir_a)], root)

    assert result.returncode != 0
    assert "--top requires --find-overlapping-dirs." in result.stdout
    assert not (dir_a / ".dirdb").exists()


def test_remove_copies_dry_run(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same.txt", "hello")
    write_file(dir_b / "same.txt", "hello")

    out = run_treeop(["--intersect", "--remove-copies", "--dry-run", str(dir_a), str(dir_b)], root)
    assert (dir_a / "same.txt").exists()
    assert (dir_b / "same.txt").exists()
    assert "Would remove" in out
    assert re.search(r"removed-files:\s+1", out)


def test_remove_copies_actual(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same.txt", "hello")
    write_file(dir_b / "same.txt", "hello")

    out = run_treeop(["--intersect", "--remove-copies", str(dir_a), str(dir_b)], root)
    assert (dir_a / "same.txt").exists()
    assert not (dir_b / "same.txt").exists()
    assert re.search(r"removed-files:\s+1", out)


def test_remove_copies_from_last(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_c = tmp_path / "c"
    dir_a.mkdir()
    dir_b.mkdir()
    dir_c.mkdir()

    write_file(dir_a / "same.txt", "hello")
    write_file(dir_b / "same.txt", "hello")
    write_file(dir_c / "same.txt", "hello")

    out = run_treeop(["--intersect", "--remove-copies-from-last", str(dir_a), str(dir_b), str(dir_c)], root)
    assert (dir_a / "same.txt").exists()
    assert (dir_b / "same.txt").exists()
    assert not (dir_c / "same.txt").exists()
    assert re.search(r"removed-files:\s+1", out)


def test_remove_copies_without_intersect(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same.txt", "hello")
    time.sleep(1.1)
    write_file(dir_b / "same.txt", "hello")

    out = run_treeop(["--remove-copies", str(dir_a), str(dir_b)], root)
    assert (dir_a / "same.txt").exists()
    assert not (dir_b / "same.txt").exists()
    assert re.search(r"removed-files:\s+1", out)


def test_stats_hardlinked_and_redundant(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    write_file(root_dir / "unique.txt", "abc")
    write_file(root_dir / "dup.txt", "dupe")
    write_file(sub_dir / "dup_copy.txt", "dupe")
    write_file(root_dir / "hl.txt", "hlink!")
    os.link(root_dir / "hl.txt", sub_dir / "hl_link.txt")

    out = run_treeop(["--stats", str(root_dir)], root)

    def stat_value(label: str) -> int:
        match = re.search(rf"{label}\s+([0-9]+)", out, re.MULTILINE)
        assert match, f"Missing {label} in output: {out}"
        return int(match.group(1))

    assert stat_value("files:") == 5
    assert stat_value("dirs:") == 2
    assert stat_value("total-size:") == 23
    assert stat_value("redundant-files:") == 1
    assert stat_value("redundant-size:") == 4
    assert stat_value("hardlinked-files:") == 1
    assert stat_value("hardlinked-size:") == 6


def test_stats_total_for_multiple_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same-a.txt", "dupe")
    write_file(dir_a / "only-a.txt", "aaa")
    write_file(dir_b / "same-b.txt", "dupe")
    write_file(dir_b / "only-b.txt", "bbbbb")

    out = run_treeop([str(dir_a), str(dir_b)], root)
    assert f"{dir_a}\n" in out
    assert f"{dir_b}\n" in out
    assert "total:\n" in out
    assert out.splitlines().count("----------------------------------------") == 2

    total_section = out.split("total:\n", 1)[1]

    def total_stat_value(label: str) -> int:
        match = re.search(rf"{label}\s+([0-9]+)", total_section, re.MULTILINE)
        assert match, f"Missing {label} in total section: {out}"
        return int(match.group(1))

    assert total_stat_value("files:") == 4
    assert total_stat_value("dirs:") == 2
    assert total_stat_value("total-size:") == 16
    assert total_stat_value("redundant-files:") == 1
    assert total_stat_value("redundant-size:") == 4


def test_stats_min_size_filters_stats_not_dirdb(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "small.txt", "xx")
    write_file(dir_a / "large.txt", "large-data")

    out = run_treeop(["--stats", "--min-size", "5", str(dir_a)], root)
    assert re.search(r"files:\s+1", out)
    assert re.search(r"total-size:\s+10 bytes", out)
    assert (dir_a / ".dirdb").exists()

    file_list = run_treeop(["--list-files", "--min-size", "5", str(dir_a)], root)
    assert "small.txt" not in file_list
    assert "large.txt" in file_list


def test_stats_max_size_filter(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "small.txt", "xx")
    write_file(dir_a / "large.txt", "large-data")

    out = run_treeop(["--stats", "--max-size", "5", str(dir_a)], root)
    assert re.search(r"files:\s+1", out)
    assert re.search(r"total-size:\s+2 bytes", out)


def test_list_files_only_and_exclude_filters_filename(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    (dir_a / "sub").mkdir(parents=True)
    write_file(dir_a / "photo.jpg", "jpg")
    write_file(dir_a / "sub" / "nested.png", "png")
    write_file(dir_a / "note.txt", "txt")
    write_file(dir_a / "photo.jpg~", "backup")

    out = run_treeop(["--list-files", "--only", "*.jpg,*.png", "--exclude", "*~", str(dir_a)], root)
    assert "photo.jpg" in out
    assert "nested.png" in out
    assert "note.txt" not in out
    assert "photo.jpg~" not in out


def test_list_files_ionly_and_iexclude_filters_case_insensitively(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "PHOTO.JPG", "jpg")
    write_file(dir_a / "diagram.PNG", "png")
    write_file(dir_a / "note.TXT", "txt")
    write_file(dir_a / "PHOTO.JPG~", "backup")

    out = run_treeop(["--list-files", "--ionly", "*.jpg,*.png", "--iexclude", "*~", str(dir_a)], root)
    assert "PHOTO.JPG" in out
    assert "diagram.PNG" in out
    assert "note.TXT" not in out
    assert "PHOTO.JPG~" not in out


def test_invalid_min_size_greater_than_max_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    result = run_treeop_result(["--stats", "--min-size", "10", "--max-size", "5", str(dir_a)], root)
    assert result.returncode != 0
    assert "--min-size must be less than or equal to --max-size." in result.stdout


def test_break_hardlinks(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    file_a = root_dir / "hl.txt"
    file_b = sub_dir / "hl_link.txt"
    write_file(file_a, "hlink!")
    os.link(file_a, file_b)
    assert file_a.stat().st_ino == file_b.stat().st_ino

    out = run_treeop(["--break-hardlinks", str(root_dir)], root)
    assert "break-hardlinks:" in out
    assert file_a.stat().st_ino != file_b.stat().st_ino


def test_break_hardlinks_min_size(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    small_a = root_dir / "small.txt"
    small_b = sub_dir / "small_link.txt"
    large_a = root_dir / "large.txt"
    large_b = sub_dir / "large_link.txt"
    write_file(small_a, "hi")
    os.link(small_a, small_b)
    write_file(large_a, "large!")
    os.link(large_a, large_b)

    out = run_treeop(["--break-hardlinks", "--min-size", "5", str(root_dir)], root)
    assert "break-hardlinks:" in out
    assert small_a.stat().st_ino == small_b.stat().st_ino
    assert large_a.stat().st_ino != large_b.stat().st_ino


def test_list_hardlinks(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    file_a = root_dir / "hl.txt"
    file_b = sub_dir / "hl_link.txt"
    write_file(file_a, "hlink!")
    os.link(file_a, file_b)

    out = run_treeop(["--list-hardlinks", str(root_dir)], root)
    assert str(file_a) in out
    assert str(file_b) in out


def test_list_hardlinks_min_size(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    small_a = root_dir / "small.txt"
    small_b = sub_dir / "small_link.txt"
    large_a = root_dir / "large.txt"
    large_b = sub_dir / "large_link.txt"
    write_file(small_a, "hi")
    os.link(small_a, small_b)
    write_file(large_a, "large!")
    os.link(large_a, large_b)

    out = run_treeop(["--list-hardlinks", "--min-size", "5", str(root_dir)], root)
    assert str(small_a) not in out
    assert str(small_b) not in out
    assert str(large_a) in out
    assert str(large_b) in out


def test_list_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = dir_a / "b"
    dir_b.mkdir(parents=True)
    write_file(dir_a / "file.txt", "hello")
    write_file(dir_b / "file.txt", "world")

    out = run_treeop(["--list-dirs", str(dir_a)], root)
    assert str(dir_a) in out
    assert str(dir_b) in out


def test_stats_after_break_hardlinks_no_warning(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    root_dir = tmp_path / "root"
    sub_dir = root_dir / "sub"
    root_dir.mkdir()
    sub_dir.mkdir()

    write_file(root_dir / "a.txt", "same")
    write_file(sub_dir / "b.txt", "same")

    run_treeop(["--hardlink-copies", "--min-size", "1", str(root_dir)], root)
    run_treeop(["--break-hardlinks", str(root_dir)], root)
    out = run_treeop(["--stats", str(root_dir)], root)
    assert "hardlinks outside root" not in out


def test_list_both(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "same.txt", "same")
    write_file(dir_b / "same.txt", "same")

    out = run_treeop(["--intersect", "--list-both", str(dir_a), str(dir_b)], root)
    assert "in-both:" in out
    assert "first:" in out
    assert "last:" in out


def test_unique_hash_len(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "a.txt", "one")
    write_file(dir_a / "b.txt", "two")

    out = run_treeop(["--get-unique-hash-len", str(dir_a)], root)
    match = re.search(r"unique-hash-len:\s+([0-9]+)", out)
    assert match
    assert int(match.group(1)) > 0


def test_size_histogram_max_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "small.txt", "ab")
    write_file(dir_a / "large.txt", "x" * 10)

    out = run_treeop(["--size-histogram", "4", "--max-size", "4", str(dir_a)], root)
    assert re.search(r":\s+1\s+2 bytes", out)
    assert "10 bytes" not in out


def test_size_histogram_min_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "small.txt", "ab")
    write_file(dir_a / "large.txt", "x" * 10)

    out = run_treeop(["--size-histogram", "4", "--min-size", "5", str(dir_a)], root)
    assert re.search(r":\s+1\s+10 bytes", out)
    assert "2 bytes" not in out


def test_invalid_size_histogram_fails_before_processing_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    result = run_treeop_result(["--size-histogram", "0", str(dir_a)], root)
    assert result.returncode != 0
    assert "--size-histogram must be greater than 0." in result.stdout
    assert not (dir_a / ".dirdb").exists()


def test_bufsize_readbench(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    out = run_treeop(["--readbench", "--bufsize", "4k", str(dir_a)], root)
    assert "bufsize:" in out


def test_lowercase_m_size_suffix(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    out = run_treeop(["--readbench", "--bufsize", "1m", str(dir_a)], root)
    assert "bufsize: 1 MB" in out


def test_max_hardlinks(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    file_a = dir_a / "same.txt"
    file_b = dir_b / "same.txt"
    write_file(file_a, "hello")
    write_file(file_b, "hello")

    out = run_treeop(["--hardlink-copies", "--min-size", "1", "--max-hardlinks", "1", str(dir_a), str(dir_b)], root)
    assert file_a.stat().st_ino != file_b.stat().st_ino
    assert re.search(r"hardlinks-created:\s+0", out)


def test_progress_width(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    out = run_treeop(["--progress", "--width", "10", str(dir_a)], root)
    assert "files:" in out


def setup_three_roots(tmp_path: Path):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_c = tmp_path / "c"
    dir_a.mkdir()
    dir_b.mkdir()
    dir_c.mkdir()

    write_file(dir_a / "first_only_a.txt", "fa")
    write_file(dir_b / "first_only_b.txt", "fb")
    write_file(dir_c / "last_only.txt", "lc")
    write_file(dir_a / "shared.txt", "same")
    write_file(dir_c / "shared.txt", "same")
    write_file(dir_b / "shared2.txt", "same2")
    write_file(dir_c / "shared2.txt", "same2")

    return dir_a, dir_b, dir_c


def test_list_first_three_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a, dir_b, dir_c = setup_three_roots(tmp_path)
    out = run_treeop(["--intersect", "--list-first", str(dir_a), str(dir_b), str(dir_c)], root)
    assert "only-in-first:" in out
    assert str(dir_a / "first_only_a.txt") in out
    assert str(dir_b / "first_only_b.txt") in out
    assert str(dir_c / "last_only.txt") not in out


def test_list_last_three_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a, dir_b, dir_c = setup_three_roots(tmp_path)
    out = run_treeop(["--intersect", "--list-last", str(dir_a), str(dir_b), str(dir_c)], root)
    assert "only-in-last:" in out
    assert str(dir_c / "last_only.txt") in out
    assert str(dir_a / "first_only_a.txt") not in out
    assert str(dir_b / "first_only_b.txt") not in out


def test_extract_first_three_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a, dir_b, dir_c = setup_three_roots(tmp_path)
    dest = tmp_path / "out_first"
    run_treeop(["--intersect", "--extract-first", str(dest), str(dir_a), str(dir_b), str(dir_c)], root)
    assert (dest / "first_only_a.txt").exists()
    assert (dest / "first_only_b.txt").exists()
    assert not (dest / "last_only.txt").exists()
    assert not (dest / "shared.txt").exists()
    assert not (dest / "shared2.txt").exists()


def test_extract_last_three_roots(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a, dir_b, dir_c = setup_three_roots(tmp_path)
    dest = tmp_path / "out_last"
    run_treeop(["--intersect", "--extract-last", str(dest), str(dir_a), str(dir_b), str(dir_c)], root)
    assert (dest / "last_only.txt").exists()
    assert not (dest / "first_only_a.txt").exists()
    assert not (dest / "first_only_b.txt").exists()
    assert not (dest / "shared.txt").exists()
    assert not (dest / "shared2.txt").exists()


def test_list_redundant_alignment(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "small.txt", "hi")
    write_file(dir_b / "small_copy.txt", "hi")
    write_file(dir_a / "big.txt", "x" * 3000)
    write_file(dir_b / "big_copy.txt", "x" * 3000)

    out = run_treeop(["--list-redundant", str(dir_a), str(dir_b)], root)
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 4
    def hash_start(line: str) -> int:
        size_start = len(line) - len(line.lstrip(" "))
        size_end = line.find(" ", size_start)
        tail = line[size_end + 1:]
        return size_end + 1 + (len(tail) - len(tail.lstrip(" ")))

    starts = [hash_start(line) for line in lines]
    assert all(start == starts[0] for start in starts)


def test_list_redundant_min_size(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "small.txt", "hi")
    write_file(dir_b / "small_copy.txt", "hi")
    write_file(dir_a / "big.txt", "x" * 3000)
    write_file(dir_b / "big_copy.txt", "x" * 3000)

    out = run_treeop(["--list-redundant", "--min-size", "5", str(dir_a), str(dir_b)], root)
    assert "small.txt" not in out
    assert "small_copy.txt" not in out
    assert "big.txt" in out
    assert "big_copy.txt" in out


def supports_hardlinks(tmp_path: Path) -> bool:
    src = tmp_path / "hl_src"
    dst = tmp_path / "hl_dst"
    src.write_text("x", encoding="utf-8")
    try:
        os.link(src, dst)
    except OSError:
        return False
    return True


def test_hardlink_copies(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    file_a = dir_a / "same.txt"
    file_b = dir_b / "same.txt"
    write_file(file_a, "hello")
    write_file(file_b, "hello")

    out = run_treeop(["--hardlink-copies", "--min-size", "1", "--dry-run", str(dir_a), str(dir_b)], root)
    assert file_a.exists()
    assert file_b.exists()
    assert "Would hardlink" in out

    out = run_treeop(["--hardlink-copies", "--min-size", "1", str(dir_a), str(dir_b)], root)
    st_a = file_a.stat()
    st_b = file_b.stat()
    assert st_a.st_ino == st_b.st_ino
    assert re.search(r"hardlinks-created:\s+1", out)


def test_same_filename_intersect(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "one.txt", "same")
    write_file(dir_b / "two.txt", "same")

    out = run_treeop(["--intersect", str(dir_a), str(dir_b)], root)
    assert re.search(r"shared-files:\s+2", out)

    out = run_treeop(["--intersect", "--same-filename", str(dir_a), str(dir_b)], root)
    assert re.search(r"shared-files:\s+0", out)


def test_same_filename_hardlink(tmp_path: Path):
    if not supports_hardlinks(tmp_path):
        pytest.skip("Filesystem does not support hardlinks")

    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    file_a = dir_a / "one.txt"
    file_b = dir_b / "two.txt"
    write_file(file_a, "same")
    write_file(file_b, "same")

    run_treeop(["--hardlink-copies", "--same-filename", str(dir_a), str(dir_b)], root)
    st_a = file_a.stat()
    st_b = file_b.stat()
    assert st_a.st_ino != st_b.st_ino


def test_same_filename_remove_copies(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "one.txt", "same")
    write_file(dir_b / "two.txt", "same")

    run_treeop(["--intersect", "--remove-copies", "--same-filename", str(dir_a), str(dir_b)], root)
    assert (dir_a / "one.txt").exists()
    assert (dir_b / "two.txt").exists()


def test_update_dirdb(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "one")

    run_treeop([str(dir_a)], root)
    db_path = dir_a / ".dirdb"
    assert db_path.exists()
    out = run_treeop(["--list-files", str(dir_a)], root)
    first_hash = extract_hash(out, "file.txt")

    time.sleep(1.1)
    write_file(dir_a / "file.txt", "two")
    run_treeop(["--update-dirdb", str(dir_a)], root)
    assert db_path.exists()
    out = run_treeop(["--list-files", str(dir_a)], root)
    second_hash = extract_hash(out, "file.txt")
    assert second_hash != first_hash


def test_new_dirdb(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "one")

    run_treeop([str(dir_a)], root)
    db_path = dir_a / ".dirdb"
    assert db_path.exists()
    out = run_treeop(["--list-files", str(dir_a)], root)
    first_hash = extract_hash(out, "file.txt")

    time.sleep(1.1)
    write_file(dir_a / "file.txt", "two")
    run_treeop(["--new-dirdb", str(dir_a)], root)
    assert db_path.exists()
    out = run_treeop(["--list-files", str(dir_a)], root)
    second_hash = extract_hash(out, "file.txt")
    assert second_hash != first_hash


def test_remove_dirdb(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = dir_a / "b"
    dir_b.mkdir(parents=True)
    write_file(dir_a / "file.txt", "one")
    write_file(dir_b / "file.txt", "two")

    run_treeop([str(dir_a)], root)
    assert (dir_a / ".dirdb").exists()
    assert (dir_b / ".dirdb").exists()

    run_treeop(["--remove-dirdb", str(dir_a)], root)
    assert not (dir_a / ".dirdb").exists()
    assert not (dir_b / ".dirdb").exists()


def test_readbench(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_a.mkdir()
    write_file(dir_a / "file.txt", "hello")

    out = run_treeop(["--readbench", str(dir_a)], root)
    assert "total-files:" in out
    assert "total-dirs:" in out
    assert "total-size:" in out
    assert "bufsize:" in out
    assert "read-rate:" in out
    assert "elapsed:" in out


def test_remove_empty_dirs(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = dir_a / "b"
    dir_b.mkdir(parents=True)
    write_file(dir_b / "file.txt", "hello")

    run_treeop([str(dir_a)], root)
    assert (dir_b / ".dirdb").exists()

    os.remove(dir_b / "file.txt")
    out = run_treeop(["--remove-empty-dirs", str(dir_a)], root)
    assert "removed-dirs:" in out
    assert not dir_b.exists()


def test_remove_empty_dirs_after_remove_copies(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    write_file(dir_a / "file.txt", "same")
    write_file(dir_b / "file.txt", "same")

    run_treeop(["--intersect", "--remove-copies", "--remove-empty-dirs", "-v", str(dir_a), str(dir_b)], root)
    assert dir_a.exists()
    assert not dir_b.exists()


def test_remove_empty_dirs_dry_run(tmp_path: Path):
    root = Path(__file__).resolve().parents[1]
    bin_path = treeop_bin()
    if not bin_path.exists():
        return

    dir_a = tmp_path / "a"
    dir_b = dir_a / "b"
    dir_b.mkdir(parents=True)
    write_file(dir_b / "file.txt", "hello")

    run_treeop([str(dir_a)], root)
    os.remove(dir_b / "file.txt")
    out = run_treeop(["--remove-empty-dirs", "--dry-run", str(dir_a)], root)
    assert "removed-dirs:" in out
    assert dir_b.exists()
