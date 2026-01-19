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
