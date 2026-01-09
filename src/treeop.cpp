// treeop - Operations on huge directory trees
//
// Copyright (c) 2026 Johannes Overmann
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

#include "MiscUtils.hpp"
#include "CommandLineParser.hpp"
#include "Hash.hpp"
#include "HashSha3.hpp"
#include <exception>
#include <iomanip>
#include <vector>
#include <span>
#include <list>
#include <map>

static unsigned clVerbose = 0;

using FileSize = uint64_t;
using NumFiles = size_t;
using DirIndex = size_t;
using FileIndex = size_t;


/// DirDB file format (.dirdb): There is one such file in each directory, representing all files in this directory (excluding the .dirdb file).
/// - All integer fields are stored little endian.
/// - All tag strings are stored as uint64_t, zero padded.
///
/// Data layout:
/// uint64_t "DirDB" tag;
/// uint64_t version
/// uint64_t "TOC" tag;  // TOC: Table of contents. Here string offset for a certain size. FileEntries are sorted by size (ascending) and this table can be used to locate a specific size.
/// uint64_t numberOfTocEntries;
/// uint64_t tocEntrySizeInBytes;
/// TocEntry tocEntries[numberOfTocEntries];
/// uint64_t "FILES" tag;
/// uint64_t numberOfFileEntries;
/// uint64_t fileEntrySizeInBytes;
/// FileEntry fileEntries[numberOfFileEntries]; // Flat list of regular files in this directory, sorted by size (ascending) and within size by name.
/// uint64_t "STRINGS" tag;
/// uint64_t totalSizeOfStringData;
/// uint8_t stringData[totalSizeOfStringData]
/// LeadingLengthString sequence:
///   First byte;
///     0-0xfc: 1-byte length
///     0xff: 2-byte length follows (little endian)
///     0xfe: 4-byte length follows (little endian)
///     0xfd: 8-byte length follows (little endian)
///   String bytes (not zero terminated)
///
/// TocEntry: Describing the start of "size" in fileEntries.
/// uint64_t size; // Size in bytes of all files at and following fileIndex.
/// uint64_t fileIndex; // Index into fileEntries[] array.
///
/// FileEntry: Describing a single regular file. (Dirs and other special files are not present in this table and are ignored.)
/// uint64_t nameIndex; // Byte offset into STRINGSIndex into STRINGS table.
/// uint64_t hashLo;
/// uint64_t hashHi;
/// uint64_t inodeNumber;
/// uint64_t date; // In FILETIME format, 100ns since January 1, 1601, UTC, unsigned (Range until around year 30828).
/// uint64_t numLinks; // Number of hardlinks.


void printStats()
{

}

/// Main.
int main(int argc, char *argv[])
{
    // Command line options.
    const char *usage = "Operations on huge directory trees.\n"
                        "\n"
                        "Usage: $programName [OPTIONS] DIR...\n"
                        "\n"
                        "All sizes may be specified with kMGTPE suffixes indicating powers of 1024.";
    ut1::CommandLineParser cl("treeop", usage,
        "\n$programName version $version *** Copyright (c) 2026 Johannes Overmann *** https://github.com/jovermann/treeop",
        "0.0.1");

    cl.addHeader("\nOptions:\n");
    cl.addOption('s', "stats", "Print statistics about each dir (number of files and total size etc).");
    cl.addOption('H', "size-histogram", "Print size histogram for all files in all dirs where N in the batch size in bytes.", "N", "0");
    cl.addOption('v', "verbose", "Increase verbosity. Specify multiple times to be more verbose.");

    // Parse command line options.
    cl.parse(argc, argv);
    clVerbose = cl.getCount("verbose");

    // Implicit options.
    if (!(cl("stats")))
    {
        cl.setOption("stats");
    }

    try
    {
        // Check all args to avoid late errors.
        for (const std::string& path : cl.getArgs())
        {
            if (!ut1::fsExists(path))
            {
                cl.error("Path '" + path + "' does not exist.");
            }
            if (!ut1::fsIsDirectory(path))
            {
                cl.error("Path '" + path + "' is not a directory.");
            }
        }

        // Recursively walk all dirs specified on the command line and either read existing .dirdb files or create missing .dirdb files.
        for (size_t i = 0; i < cl.getArgs().size(); i++)
        {
            // todo
        }

        if (cl("stats"))
        {
            printStats();
        }

        if (clVerbose)
        {
            std::cout << "Done.\n";
        }
    }
    catch (const std::exception& e)
    {
        cl.error(e.what());
    }

    return 0;
}
