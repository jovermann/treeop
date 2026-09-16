# treeop

Operations on huge directory trees.

## Warnings

`treeop` can remove files (with the `--remove-copies` option for example). Always make a backup before using `treeop`. Most removal operations cannot be undone; `--explore-interactive` is the exception and uses a persistent per-root trash directory.

`treeop` automatically creates and maintains a `.dirdb` file in each dir to cache file meta-data and hashes. During normal processing, an unreadable or corrupt `.dirdb` is removed and regenerated automatically. You can remove these files using `treeop --remove-dirdb DIRS...`.

## Features

- Intersect directory trees and report unique vs shared content per root
- Remove redundant copies across trees (keep files in earliest root)
- Extract unique files of a root of an intersection into a new destination
- Fast operation by caching directory contents and hashes in .dirdb files
- Aggregate `.treedb` snapshots for fast loading of large trees on slow/network drives
- Interactive file-explorer mode with directory sizes, recoverable removal, persistent undo, and trash browsing

## Examples

Intersection and redundancy are especially useful for photo backups from mobile phones. A common workflow is that the phone keeps all old photos, and each time you download, it re-downloads everything. Over time, your desktop accumulates many redundant copies across dated folders, either by accident or intentionally. `treeop --intersect --remove-copies A B C` can fix that.

- Compare two or more photo folders and see what is unique/shared:

  ```sh
  treeop --intersect 2025-07-01_Photos 2025-10-19_Photos 2026-01-12_Photos
  ```

- Remove copies from later folders while keeping the copies in the earliest folder intact. Dry-run to preview deletions before actually deleting anything:

  ```sh
  treeop --intersect --remove-copies --dry-run 2025-07-01_Photos 2025-10-19_Photos 2026-01-12_Photos
  ```

- Actually remove the files if you are happy with the removal:

  ```sh
  treeop --intersect --remove-copies 2025-07-01_Photos 2025-10-19_Photos 2026-01-12_Photos
  ```

- Groom one or more old trees interactively. Removed items are moved to
  `<root>/.treeop_trash` and remain undoable after restarting `treeop`:

  ```sh
  treeop --explore-interactive OLD_TREE ANOTHER_TREE
  ```

## Aggregate tree snapshots

Generate one `.treedb` in each given directory root:

```sh
treeop --generate-treedb ARCHIVE ANOTHER_ARCHIVE
treeop --intersect --remove-copies-from-last ARCHIVE RECENT_BACKUP
```

Read-only operations automatically prefer a root's `.treedb`, loading the entire snapshot with one file read instead of walking the tree and opening every `.dirdb`. With `--intersect --remove-copies-from-last`, earlier reference trees can use snapshots; the last, mutable tree uses ordinary directory loading. Local mutation targets with snapshots are refreshed and their aggregate snapshots invalidated before modifying files (not during dry-run). Reference snapshots are left untouched.

Snapshots contain all regular files, including hidden files, and preserve empty directories. Symlinks, special files, and `.dirdb`/`.treedb` metadata are excluded. Paths are root-relative, so snapshots can move with their tree. The hunk-based container embeds the same DirDB format used by `.dirdb`; the full binary layout is documented in `src/treeop.cpp`.

Generated and regenerated snapshots use ordinary file permissions (`0666` filtered by your umask, typically `0644`). Regeneration corrects older owner-only snapshots and reapplies this policy rather than preserving manually changed permissions.

A `.treedb` is an explicit snapshot, not a live index: loading it does not check file freshness or discover changes made by other tools. Run `--generate-treedb` again after changes. Generation refreshes local `.dirdb` data, reusing cached hashes; existing network `.dirdb` files remain unchanged and supply their cached snapshot data. `--new-dirdb`/`--update-dirdb` bypass aggregate snapshots. Generation cannot be combined with filters, `--max-depth`, dry-run, or other operations. A corrupt aggregate snapshot produces an error rather than silently falling back to thousands of network reads.

## Filtered removal

Remove files selected by the standard name and size filters:

```sh
treeop --remove-files --only '*.tmp,*.bak' OLD_TREE
treeop --remove-files --ionly '*.jpg' --min-size 10M --dry-run PHOTOS
```

Remove matching directories recursively:

```sh
treeop --remove-dirs --only 'cache-*' OLD_TREE
```

`--remove-files` and `--remove-dirs` are standalone operations and require an effective filter. Name patterns match basenames. File removal accepts `--min-size`, `--max-size`, `--only`, `--ionly`, `--exclude`, and `--iexclude`. Directory removal accepts only the four name filters; combining it with either size filter is an error. Command-line roots are always protected. If matching directories are nested, only the topmost one is processed and its entire subtree is removed. Use `--dry-run` to review every target first.

`--only` and `--ionly` form one inclusion set: matching any inclusion pattern is sufficient. If neither is supplied, every basename starts included. `--exclude` and `--iexclude` are applied afterward and always win, even when the same basename matched an inclusion pattern. Size filters are ANDed with the final name-filter result.

Local `.dirdb` files are refreshed before filtering, and directories touched by file removal are updated afterward. Aggregate `.treedb` snapshots on mutation targets are bypassed and invalidated. Existing `.dirdb` files on network drives remain read-only snapshots.

## Interactive file explorer

Use `-X` or `--explore-interactive` with one or more directory roots.

The explorer shows recursively calculated sizes and modification dates for files and directories. Directories and files have different colors, and the top line always shows the combined trash size.

- `↑`/`↓`, Page Up/Page Down: move the selection
- `Home` (Pos 1)/`End`: jump to the first/last visible entry
- `Space`: toggle expansion; `→` expands; `←` collapses or moves to the parent
- `*`/Return: recursively expand/collapse the selected directory
- `i`: show recursive file-extension counts (with underscore grouping) and human-readable approximate sizes, largest first, plus a hidden-file subtotal (including files in hidden directories). On files, show a safe ASCII text preview or a hex dump with ASCII. Previews read at most 64 KiB; arrow/page keys and Home/End scroll, `i`/Escape returns to browsing. Symlinks and special files are not read.
- `+`/`-`: expand/collapse every directory
- `d`: delete—move the selected file or directory to its root's `.treeop_trash`
- `u`: restore the selected trash entry, or the newest entry from tree view
- `1`/`2`/`3` or Tab: tree, persistent undo stack, and trash explorer views
- `s`: cycle name, size, and date sorting
- `o`: open the selected file in its default application using `open` on macOS or `xdg-open` on Linux
- `H`: show/hide dotfiles without rescanning. Recursive sizes always include hidden files.
- `/`: filter by name; `r`: rescan
- `E` twice: permanently empty all per-root trash directories
- `?`/`h`: show the detailed, scrollable key reference; `q`: quit without emptying trash

The current selected path is shown directly below the title; short key help is on the bottom status bar. Undo history is persisted in each root's `.treeop_trash/entries/<unique-id>/`: `payload` contains the moved item and `original` stores its original root-relative path. History is reconstructed from these entries on startup, rather than kept in a separate stack file.


## Command line options

```
> treeop --help
treeop: Operations on huge directory trees.

Usage: treeop [OPTIONS] DIR...

All sizes may be specified with kMGTPE suffixes indicating powers of 1024.

Options:
  -i --intersect           Determine intersections of two or more dirs. Print unique/shared statistics per
                           dir.
  -c --containment         Show how much of the last dir is contained in the previous dirs.
     --show-contained-files
                           List files in the last dir that are contained in the previous dirs (with
                           --containment).
     --show-not-contained-files
                           List files in the last dir that are not contained in the previous dirs (with
                           --containment).
     --show-not-contained  Show mostly-not-contained and not-contained dir sections (with
                           --containment).
     --remove-contained-dirs
                           Delete dirs from the last root that are completely contained in previous
                           roots (with --containment).
     --remove-contained-files
                           Delete files from the last root that are contained in previous roots (with
                           --containment).
     --find-overlapping-dirs
                           Find and rank overlapping directory pairs within the specified trees.
     --find-redundant-dirs Find and rank dirs by bytes whose content appears elsewhere.
  -s --stats               Print statistics about each dir (number of files and total size etc).
  -l --list-files          List all files with stored meta-data.
     --list-a              List files only in A when used with --intersect.
     --list-b              List files only in B when used with --intersect.
     --list-both           List files in both A and B when used with --intersect.
     --extract-a=DIR       Extract files only in A into DIR when used with --intersect.
     --extract-b=DIR       Extract files only in B into DIR when used with --intersect.
     --remove-copies       Delete files from later roots when content exists in earlier roots (with
                           --intersect).
     --remove-dir-internal-copies
                           Delete duplicate files within each directory independently, keeping the
                           oldest file.
  -d --dry-run             Show what would change, but do not modify files.
  -i --interactive         Open an interactive TUI for --remove-dir-internal-copies.
     --new-dirdb           Force creation of new .dirdb files (overwrite existing).
  -u --update-dirdb        Update .dirdb files, reusing hashes when inode/size/mtime match.
     --make-dirs-writable  Add owner-write permission to directories where .dirdb files are written.
     --remove-dirdb        Recursively remove all .dirdb files under specified dirs.
     --remove-corrupt-dirdbs
                           Validate existing .dirdb files and remove those that cannot be read.
     --remove-dirs-that-contain-file=FILE_PATTERN
                           Recursively remove directories containing a file matching FILE_PATTERN.
     --get-unique-hash-len Calculate the minimum hash length in bits that makes all file contents unique.
     --hashrate            Hash memory for 2 seconds to measure CPU hashing performance without filesystem IO.
     --size-histogram=N    Print size histogram for all files in all dirs where N in the batch size in
                           bytes. (default=0)
     --top=N               Maximum number of results to print (with --find-overlapping-dirs or
                           --find-redundant-dirs). (default=0)
     --max-depth=N         Maximum directory recursion depth; command-line roots are at depth 0.
     --min-size=N          Minimum file size for operations that support file filtering. (default=0)
     --max-size=N          Maximum file size for operations that support file filtering. (default=0)
     --only=PATTERNS       Only include filenames matching comma-separated fnmatch patterns.
     --ionly=PATTERNS      Only include filenames matching comma-separated fnmatch patterns,
                           case-insensitively.
     --exclude=PATTERNS    Exclude filenames matching comma-separated fnmatch patterns.
     --iexclude=PATTERNS   Exclude filenames matching comma-separated fnmatch patterns,
                           case-insensitively.
  -p --progress            Print progress once per second.
  -W --width=N             Max width for progress line. (default=199)
  -v --verbose             Increase verbosity. Specify multiple times to be more verbose.
  -h --help                Print this help message and exit. (set)
     --version             Print version and exit.

 treeop version 0.1.1 *** Copyright (c) 2026 Johannes Overmann *** https://github.com/jovermann/treeop
```

## Building

Requires at least GCC 13 or Clang 15 for std::format.

Linux/MacOS:

`make CXX=clang++-18`

## Running tests

`make test`
