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
