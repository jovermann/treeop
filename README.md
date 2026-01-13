# treeop

Operations on huge directory trees.

## Warnings

`treeop` can remove files (with the `--remove-copies` option for example). Always make a backup before using `treeop`. Removal cannot be undone.

`treeop` automatically creates and maintains a `.dirdb` file in each dir to cache file meta-data and hashes. You can remove these files using `treeop --remove-dirdb DIRS...'.

## Features

- Intersect directory trees and report unique vs shared content per root
- Remove redundant copies across trees (keep files in earliest root)
- Extract unique files of a root of an intersection into a new destination
- Fast operation by caching directory contents and hashes in .dirdb files

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


## Command line options

```
> treeop --help
treeop: Operations on huge directory trees.

Usage: treeop [OPTIONS] DIR...

All sizes may be specified with kMGTPE suffixes indicating powers of 1024.

Options:
  -i --intersect           Determine intersections of two or more dirs. Print unique/shared statistics per
                           dir.
  -s --stats               Print statistics about each dir (number of files and total size etc).
  -l --list-files          List all files with stored meta-data.
     --list-a              List files only in A when used with --intersect.
     --list-b              List files only in B when used with --intersect.
     --list-both           List files in both A and B when used with --intersect.
     --extract-a=DIR       Extract files only in A into DIR when used with --intersect.
     --extract-b=DIR       Extract files only in B into DIR when used with --intersect.
     --remove-copies       Delete files from later roots when content exists in earlier roots (with
                           --intersect).
  -d --dry-run             Show what would change, but do not modify files.
     --new-dirdb           Force creation of new .dirdb files (overwrite existing).
  -u --update-dirdb        Update .dirdb files, reusing hashes when inode/size/mtime match.
     --remove-dirdb        Recursively remove all .dirdb files under specified dirs.
     --get-unique-hash-len Calculate the minimum hash length in bits that makes all file contents unique.
     --size-histogram=N    Print size histogram for all files in all dirs where N in the batch size in
                           bytes. (default=0)
     --max-size=N          Maximum file size to include in size histogram. (default=0)
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




