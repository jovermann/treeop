import os
import re
import subprocess
from pathlib import Path


def treeop_bin() -> Path:
    root = Path(__file__).resolve().parents[1]
    bin_path = Path(os.environ.get("TREEOP_BIN", root / "treeop"))
    return bin_path


def run_treeop(args, cwd: Path):
    bin_path = treeop_bin()
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
