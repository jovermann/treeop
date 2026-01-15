// treeop - Operations on huge directory trees
//
// Copyright (c) 2026 Johannes Overmann
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

#include "MiscUtils.hpp"
#include "UnitTest.hpp"
#include "CommandLineParser.hpp"
#include "Hash.hpp"
#include "HashSha3.hpp"
#include <exception>
#include <iomanip>
#include <iostream>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>
#include <unordered_map>
#include <bit>
#include <optional>
#include <set>
#include <span>
#include <list>
#include <map>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <cstdint>

static unsigned clVerbose = 0;

using FileSize = uint64_t;
using NumFiles = size_t;
using DirIndex = size_t;
using FileIndex = size_t;
namespace fs = std::filesystem;

static constexpr uint64_t kDirDbVersion = 1;
static constexpr uint64_t kWindowsToUnixEpoch = 11644473600ULL; // seconds
static uint64_t gBufSize = 1024ULL * 1024ULL;

struct Hash128
{
    uint64_t hi{};
    uint64_t lo{};
    /// Compare hashes for ordering.
    bool operator<(const Hash128& other) const
    {
        if (hi != other.hi)
        {
            return hi < other.hi;
        }
        return lo < other.lo;
    }
    /// Compare hashes for equality.
    bool operator==(const Hash128& other) const
    {
        return hi == other.hi && lo == other.lo;
    }
    /// Format the hash as a hex string (low then high).
    std::string toHex() const
    {
        std::ostringstream os;
        os << std::hex << std::setw(16) << std::setfill('0') << lo
           << std::setw(16) << std::setfill('0') << hi;
        return os.str();
    }

    /// Convert a 16-byte vector into a Hash128 (little-endian).
    static Hash128 fromBytes(const std::vector<uint8_t>& bytes)
    {
        if (bytes.size() < 16)
        {
            return {};
        }
        uint64_t outLo = 0;
        uint64_t outHi = 0;
        for (size_t i = 0; i < 8; i++)
        {
            outLo |= (static_cast<uint64_t>(bytes[i]) << (8 * i));
            outHi |= (static_cast<uint64_t>(bytes[8 + i]) << (8 * i));
        }
        return Hash128{outHi, outLo};
    }

    /// Convert the hash into a 16-byte vector (little-endian).
    std::vector<uint8_t> toBytes() const
    {
        std::vector<uint8_t> bytes(16);
        for (size_t i = 0; i < 8; i++)
        {
            bytes[i] = static_cast<uint8_t>((lo >> (8 * i)) & 0xff);
            bytes[8 + i] = static_cast<uint8_t>((hi >> (8 * i)) & 0xff);
        }
        return bytes;
    }
};

class ProgressTracker
{
public:
    /// Initialize progress tracking with width and linefeed mode.
    explicit ProgressTracker(size_t maxWidth_ = 199, bool linefeed_ = false)
        : startTime(ut1::getTimeSec()),
          lastPrintTime(startTime),
          lastRateTime(startTime),
          maxWidth(maxWidth_),
          linefeed(linefeed_) {}

    /// Note that directory processing started.
    void onDirStart(const fs::path& dirPath)
    {
        if (!hashing)
        {
            currentDir = dirPath.string();
        }
        tick();
    }

    /// Note that one directory finished.
    void onDirDone()
    {
        dirs++;
        tick();
    }

    /// Add a completed directory summary without per-file callbacks.
    void addDirSummary(uint64_t fileCount, uint64_t totalBytes)
    {
        dirs++;
        this->files += fileCount;
        this->bytes += totalBytes;
        tick();
    }

    /// Note that a file was processed.
    void onFileProcessed(uint64_t size)
    {
        files++;
        bytes += size;
        tick();
    }

    /// Begin tracking hashing of a file.
    void onHashStart(const fs::path& filePath, uint64_t fileSize)
    {
        hashing = true;
        currentFile = filePath.string();
        currentFileSize = fileSize;
        currentFileDone = 0;
        tick();
    }

    /// Update hashing progress for the current file.
    void onHashProgress(uint64_t bytesRead)
    {
        hashedBytes += bytesRead;
        currentFileDone += bytesRead;
        tick();
    }

    /// End tracking hashing of a file.
    void onHashEnd()
    {
        hashing = false;
        currentFile.clear();
        currentFileSize = 0;
        currentFileDone = 0;
        tick();
    }

    /// Clear the progress line and finish output.
    void finish()
    {
        if (lastLineLen > 0)
        {
            std::cout << "\r" << std::string(lastLineLen, ' ') << "\r" << std::flush;
            std::cout << "\n";
            lastLineLen = 0;
        }
    }

private:
    /// Print an updated progress line once per second.
    void tick()
    {
        double now = ut1::getTimeSec();
        if (now - lastPrintTime < 1.0)
        {
            return;
        }
        lastPrintTime = now;
        printLine(now);
    }

    /// Render one progress line based on current state.
    void printLine(double now)
    {
        double elapsed = now - startTime;
        double avgRate = (elapsed > 0.0) ? (double(hashedBytes) / elapsed) : 0.0;
        double deltaTime = now - lastRateTime;
        uint64_t deltaBytes = hashedBytes - lastRateBytes;
        double curRate = (deltaTime > 0.0) ? (double(deltaBytes) / deltaTime) : 0.0;
        std::string avgRateStr = ut1::getApproxSizeStr(avgRate, 1, false, true) + "/s";
        std::string curRateStr = ut1::getApproxSizeStr(curRate, 1, false, true) + "/s";
        std::string sizeStr = ut1::getApproxSizeStr(bytes, 1, false, true);
        std::string prefix = ut1::toStr(files) + "f/" + ut1::toStr(dirs) + "d (" + sizeStr + ", " + avgRateStr + ", " + curRateStr + ")";

        std::string suffix;
        if (hashing && !currentFile.empty())
        {
            unsigned percent = 0;
            if (currentFileSize > 0)
            {
                percent = unsigned((currentFileDone * 100) / currentFileSize);
            }
            std::string percentStr = ut1::toStr(percent) + "%";
            size_t maxPath = availablePathLen(prefix.size(), percentStr.size());
            suffix = percentStr + " " + abbreviatePath(currentFile, maxPath);
        }
        else if (!currentDir.empty())
        {
            size_t maxPath = availablePathLen(prefix.size(), 0);
            suffix = abbreviatePath(currentDir, maxPath);
        }

        std::string line = prefix;
        if (!suffix.empty())
        {
            line += " " + suffix;
        }
        if (line.size() > maxWidth)
        {
            line.resize(maxWidth);
        }
        if (linefeed)
        {
            std::cout << line << "\n" << std::flush;
        }
        else
        {
            size_t pad = (lastLineLen > line.size()) ? (lastLineLen - line.size()) : 0;
            std::cout << "\r" << line << std::string(pad, ' ') << "\r" << std::flush;
            lastLineLen = line.size();
        }
        lastRateTime = now;
        lastRateBytes = hashedBytes;
    }

    /// Compute available path length for the progress line.
    size_t availablePathLen(size_t prefixLen, size_t extraLen) const
    {
        size_t used = prefixLen + 1;
        if (extraLen > 0)
        {
            used += extraLen + 1;
        }
        if (used >= maxWidth)
        {
            return 0;
        }
        return maxWidth - used;
    }

    /// Abbreviate a path to fit within the given length.
    static std::string abbreviatePath(const std::string& path, size_t maxLen)
    {
        if (maxLen == 0)
        {
            return std::string();
        }
        if (path.size() <= maxLen)
        {
            return path;
        }
        if (maxLen <= 3)
        {
            return path.substr(path.size() - maxLen);
        }
        return "..." + path.substr(path.size() - (maxLen - 3));
    }

    uint64_t dirs = 0;
    uint64_t files = 0;
    uint64_t bytes = 0;
    uint64_t hashedBytes = 0;
    double startTime = 0.0;
    double lastPrintTime = 0.0;
    double lastRateTime = 0.0;
    std::string currentDir;
    std::string currentFile;
    uint64_t currentFileSize = 0;
    uint64_t currentFileDone = 0;
    bool hashing = false;
    uint64_t lastRateBytes = 0;
    size_t lastLineLen = 0;
    size_t maxWidth = 199;
    bool linefeed = false;
};

static ProgressTracker* gProgress = nullptr;


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

struct FileEntry
{
    std::string path;
    FileSize size{};
    Hash128 hash{};
    uint64_t inode{};
    uint64_t date{}; // FILETIME ticks (100ns since 1601-01-01 UTC).
    uint64_t numLinks{};
};

struct DirDbData
{
    fs::path path;
    std::vector<FileEntry> files;
    uint64_t dbSize{};
    uint64_t hashedBytes{};
    double hashSeconds{};
};

static DirDbData loadOrCreateDirDb(const fs::path& dirPath, bool forceCreate, bool update);
static DirDbData createDirDb(const fs::path& dirPath);
static DirDbData updateDirDb(const fs::path& dirPath);

class MainDb
{
public:
    struct RootData
    {
        fs::path path;
        double elapsedSeconds{};
    };

    /// Initialize the database with a list of root directories.
    /// Initialize the database with a list of root directories.
    explicit MainDb(std::vector<fs::path> rootDirs, bool sameFilename_)
        : sameFilename(sameFilename_)
    {
        for (auto& path : rootDirs)
        {
            roots.push_back(RootData{std::move(path), 0.0});
        }
    }

    /// Add directory data to the main database.
    void addDir(DirDbData dir)
    {
        dirs.push_back(std::move(dir));
    }

    /// Load or create .dirdb files for all roots and record elapsed time.
    void processRoots(bool forceCreate, bool update)
    {
        for (auto& rootData : roots)
        {
            double start = ut1::getTimeSec();
            processDirTree(rootData.path, forceCreate, update);
            rootData.elapsedSeconds = ut1::getTimeSec() - start;
        }
    }

    /// Print per-root statistics.
    void printStats() const
    {
        for (const auto& rootData : roots)
        {
            size_t dirCount = 0;
            NumFiles fileCount = 0;
            FileSize totalSize = 0;
            uint64_t totalDbSize = 0;
            uint64_t totalHashedBytes = 0;
            double totalHashSeconds = 0.0;
            std::map<ContentKey, uint64_t> contentCounts;
            for (const auto& dir : dirs)
            {
                if (!isPathWithin(rootData.path, dir.path))
                {
                    continue;
                }
                dirCount++;
                fileCount += dir.files.size();
                for (const auto& file : dir.files)
                {
                    totalSize += file.size;
                    Hash128 keyHash = sameFilename ? hashWithFilename(file.hash, keyNameForPath(file.path)) : file.hash;
                    ContentKey key{file.size, keyHash};
                    contentCounts[key]++;
                }
                totalDbSize += dir.dbSize;
                totalHashedBytes += dir.hashedBytes;
                totalHashSeconds += dir.hashSeconds;
            }

            uint64_t redundantFiles = 0;
            uint64_t redundantSize = 0;
            for (const auto& [key, count] : contentCounts)
            {
                if (count > 1)
                {
                    uint64_t extra = count - 1;
                    redundantFiles += extra;
                    redundantSize += extra * key.size;
                }
            }

            std::string percentStr = formatPercentFixed(totalSize == 0 ? 0.0 : (100.0 * totalDbSize / totalSize));
            std::string redundantFilesPct = formatPercentFixed(fileCount == 0 ? 0.0 : (100.0 * redundantFiles / fileCount));
            std::string redundantSizePct = formatPercentFixed(totalSize == 0 ? 0.0 : (100.0 * redundantSize / totalSize));
            double dirdbBytesPerFile = (fileCount == 0) ? 0.0 : (double(totalDbSize) / double(fileCount));
            std::string elapsedStr;
            if (rootData.elapsedSeconds > 0.0)
            {
                elapsedStr = ut1::secondsToString(rootData.elapsedSeconds);
            }
            std::vector<StatLine> stats = {
                {"files:", formatCountInt(fileCount), std::string()},
                {"dirs:", formatCountInt(dirCount), std::string()},
                {"total-size:", ut1::getApproxSizeStr(totalSize, 3, true, false), std::string()},
                {"redundant-files:", formatCountInt(redundantFiles), "(" + redundantFilesPct + ")"},
                {"redundant-size:", ut1::getApproxSizeStr(redundantSize, 3, true, false), "(" + redundantSizePct + ")"},
                {"dirdb-size:", ut1::getApproxSizeStr(totalDbSize, 3, true, false), "(" + percentStr + ")"},
                {"dirdb-bytes-per-file:", ut1::getApproxSizeStr(dirdbBytesPerFile, 1, true, true), std::string()}
            };
            if (totalHashedBytes > 0 && totalHashSeconds > 0.0)
            {
                double rateMb = (double(totalHashedBytes) / totalHashSeconds / (1024.0 * 1024.0));
                std::ostringstream rateOs;
                rateOs << std::fixed << std::setprecision(1) << rateMb << " MB/s";
                stats.push_back({"hash-size:", ut1::getApproxSizeStr(totalHashedBytes, 3, true, false), std::string()});
                stats.push_back({"hash-rate:", rateOs.str(), std::string()});
            }
            if (!elapsedStr.empty())
            {
                stats.push_back({"elapsed:", elapsedStr, std::string()});
            }

            std::cout << rootData.path.string() << "\n";
            printStatList(stats);
        }
    }

    /// List all files with stored metadata.
    void listFiles() const
    {
        size_t hashLen = getUniqueHashHexLen();
        std::vector<FileEntry> refs;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                refs.push_back(FileEntry{
                    (dir.path / file.path).string(),
                    file.size,
                    file.hash,
                    file.inode,
                    file.date,
                    file.numLinks
                });
            }
        }
        printListRows(refs, clVerbose > 1, hashLen);
    }

    /// Print a size histogram over all files.
    void printSizeHistogram(uint64_t batchSize, uint64_t maxSizeLimit, bool hasMaxSize) const
    {
        if (batchSize == 0)
        {
            throw std::runtime_error("size-histogram batch size must be greater than 0.");
        }

        struct Bucket
        {
            uint64_t count{};
            uint64_t totalSize{};
        };

        std::map<uint64_t, Bucket> buckets;
        uint64_t maxSize = 0;
        bool hasFiles = false;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                if (hasMaxSize && file.size > maxSizeLimit)
                {
                    continue;
                }
                uint64_t start = (file.size / batchSize) * batchSize;
                Bucket& bucket = buckets[start];
                bucket.count++;
                bucket.totalSize += file.size;
                if (!hasFiles || file.size > maxSize)
                {
                    maxSize = file.size;
                    hasFiles = true;
                }
            }
        }

        size_t widthStart = 0;
        size_t widthEnd = 0;
        size_t widthCount = 0;
        size_t widthTotal = 0;
        size_t totalDecimalPos = 0;
        size_t totalSuffixWidth = 0;
        std::vector<std::string> bucketTotalStrings;
        std::vector<uint64_t> bucketTotals;
        uint64_t maxBucketTotal = 0;
        bool showEnd = clVerbose > 0;
        size_t widthStartNum = 0;
        size_t widthEndNum = 0;
        uint64_t unitFactor = 1;
        std::string unitLabel = splitSizeStr(ut1::getPreciseSizeStr(batchSize, &unitFactor)).second;

        uint64_t maxStart = hasFiles ? (maxSize / batchSize) * batchSize : 0;
        for (uint64_t start = 0; start <= maxStart; start += batchSize)
        {
            std::string startNum = ut1::toStr(start / unitFactor);
            std::string endNum = ut1::toStr((start + batchSize) / unitFactor);
            widthStartNum = std::max(widthStartNum, startNum.size());
            if (showEnd)
            {
                widthEndNum = std::max(widthEndNum, endNum.size());
            }
        }

        std::string unitSuffix = std::string(" ") + unitLabel;
        widthStart = widthStartNum + unitSuffix.size();
        if (showEnd)
        {
            widthEnd = widthEndNum + unitSuffix.size();
        }

        for (uint64_t start = 0; start <= maxStart; start += batchSize)
        {
            const auto it = buckets.find(start);
            const Bucket empty{};
            const Bucket& bucket = (it == buckets.end()) ? empty : it->second;
            widthCount = std::max(widthCount, ut1::toStr(bucket.count).size());
            std::string totalStr = ut1::getApproxSizeStr(bucket.totalSize, 3, true, false);
            auto [numberStr, suffixStr] = splitSizeStr(totalStr);
            totalDecimalPos = std::max(totalDecimalPos, getDecimalPos(numberStr));
            totalSuffixWidth = std::max(totalSuffixWidth, suffixStr.size());
            bucketTotalStrings.push_back(std::move(totalStr));
            bucketTotals.push_back(bucket.totalSize);
            maxBucketTotal = std::max(maxBucketTotal, bucket.totalSize);
        }

        for (const auto& totalStr : bucketTotalStrings)
        {
            auto [numberStr, suffixStr] = splitSizeStr(totalStr);
            size_t numberWidth = numberStr.size() + (totalDecimalPos > getDecimalPos(numberStr)
                ? (totalDecimalPos - getDecimalPos(numberStr))
                : 0);
            size_t totalWidth = numberWidth + 1 + totalSuffixWidth;
            widthTotal = std::max(widthTotal, totalWidth);
        }

        size_t rangeWidth = showEnd ? (widthStart + 2 + widthEnd + 1) : (widthStart + 1);
        size_t baseWidth = rangeWidth + 1 + widthCount + 1 + widthTotal;
        bool showBar = clVerbose > 1;
        size_t barAvailable = (showBar && baseWidth + 1 < 79) ? (79 - baseWidth - 1) : 0;
        size_t bucketIndex = 0;
        for (uint64_t start = 0; start <= maxStart; start += batchSize)
        {
            const auto it = buckets.find(start);
            const Bucket empty{};
            const Bucket& bucket = (it == buckets.end()) ? empty : it->second;
            std::string startStr = formatHistogramBoundary(start, unitFactor, unitLabel, widthStartNum);
            std::string totalStr = (bucketIndex < bucketTotalStrings.size())
                ? bucketTotalStrings[bucketIndex]
                : ut1::getApproxSizeStr(bucket.totalSize, 3, true, false);
            totalStr = formatSizeAligned(totalStr, totalDecimalPos, totalSuffixWidth);
            if (totalStr.size() < widthTotal)
            {
                totalStr = padRight(totalStr, widthTotal);
            }
            std::string rangeLabel;
            if (showEnd)
            {
                std::string endStr = formatHistogramBoundary(start + batchSize, unitFactor, unitLabel, widthEndNum);
                rangeLabel = padRight(startStr, widthStart) + ".." + padRight(endStr, widthEnd) + ":";
            }
            else
            {
                rangeLabel = padRight(startStr, widthStart) + ":";
            }
            std::cout << padRight(rangeLabel, rangeWidth) << " "
                      << std::setw(static_cast<int>(widthCount)) << bucket.count << " "
                      << totalStr;
            if (showBar && barAvailable > 0)
            {
                uint64_t total = (bucketIndex < bucketTotals.size()) ? bucketTotals[bucketIndex] : bucket.totalSize;
                size_t barLen = 0;
                if (maxBucketTotal > 0)
                {
                    barLen = static_cast<size_t>((total * barAvailable) / maxBucketTotal);
                    if (total > 0 && barLen == 0)
                    {
                        barLen = 1;
                    }
                }
                std::cout << " " << std::string(barLen, '#');
            }
            std::cout << "\n";
            bucketIndex++;
        }
    }

    /// Print intersect stats and optional file lists/extractions.
    void printIntersectStats(const std::vector<fs::path>& rootPaths, bool listA, bool listB, bool listBoth,
        const fs::path* extractA, const fs::path* extractB, bool removeCopies, bool dryRun) const
    {
        std::vector<std::map<ContentKey, std::vector<FileEntry>>> rootFiles(rootPaths.size());
        std::map<ContentKey, size_t> rootsWithKey;

        for (const auto& dir : dirs)
        {
            for (size_t i = 0; i < rootPaths.size(); i++)
            {
                if (!isPathWithin(rootPaths[i], dir.path))
                {
                    continue;
                }
                for (const auto& file : dir.files)
                {
                    Hash128 keyHash = sameFilename ? hashWithFilename(file.hash, keyNameForPath(file.path)) : file.hash;
                    ContentKey key{file.size, keyHash};
                    rootFiles[i][key].push_back(FileEntry{(dir.path / file.path).string(), file.size, file.hash, file.inode, file.date, file.numLinks});
                }
            }
        }

        struct BucketStats
        {
            uint64_t files{};
            uint64_t bytes{};
        };
        for (size_t i = 0; i < rootFiles.size(); i++)
        {
            for (const auto& [key, listRefs] : rootFiles[i])
            {
                if (!listRefs.empty())
                {
                    rootsWithKey[key]++;
                }
            }
        }

        uint64_t removedFiles = 0;
        uint64_t removedBytes = 0;
        if (removeCopies)
        {
            std::tie(removedFiles, removedBytes) = removeCopyFiles(rootFiles, dryRun);
        }

        for (size_t i = 0; i < rootFiles.size(); i++)
        {
            BucketStats uniqueStats;
            BucketStats sharedStats;
            for (const auto& [key, listRefs] : rootFiles[i])
            {
                uint64_t count = listRefs.size();
                if (count == 0)
                {
                    continue;
                }
                if (rootsWithKey[key] > 1)
                {
                    sharedStats.files += count;
                    sharedStats.bytes += count * key.size;
                }
                else
                {
                    uniqueStats.files += count;
                    uniqueStats.bytes += count * key.size;
                }
            }

            std::vector<StatLine> stats = {
                {"unique-files:", formatCountInt(uniqueStats.files), std::string()},
                {"unique-size:", ut1::getApproxSizeStr(uniqueStats.bytes, 3, true, false), std::string()},
                {"shared-files:", formatCountInt(sharedStats.files), std::string()},
                {"shared-size:", ut1::getApproxSizeStr(sharedStats.bytes, 3, true, false), std::string()}
            };

            std::cout << rootPaths[i].string() << ":\n";
            printStatList(stats);
        }

        BucketStats totalUnique;
        BucketStats totalShared;
        for (const auto& [key, rootCount] : rootsWithKey)
        {
            size_t totalFiles = 0;
            for (const auto& perRoot : rootFiles)
            {
                auto it = perRoot.find(key);
                if (it != perRoot.end())
                {
                    totalFiles += it->second.size();
                }
            }
            if (rootCount > 1)
            {
                totalShared.files += totalFiles;
                totalShared.bytes += totalFiles * key.size;
            }
            else
            {
                totalUnique.files += totalFiles;
                totalUnique.bytes += totalFiles * key.size;
            }
        }

        uint64_t totalFilesAll = totalUnique.files + totalShared.files;
        uint64_t totalBytesAll = totalUnique.bytes + totalShared.bytes;
        std::string totalUniqueFilesPct = formatPercentFixed(totalFilesAll == 0 ? 0.0 : (100.0 * totalUnique.files / totalFilesAll));
        std::string totalUniqueBytesPct = formatPercentFixed(totalBytesAll == 0 ? 0.0 : (100.0 * totalUnique.bytes / totalBytesAll));
        std::string totalSharedFilesPct = formatPercentFixed(totalFilesAll == 0 ? 0.0 : (100.0 * totalShared.files / totalFilesAll));
        std::string totalSharedBytesPct = formatPercentFixed(totalBytesAll == 0 ? 0.0 : (100.0 * totalShared.bytes / totalBytesAll));
        std::vector<StatLine> totalStats = {
            {"total-files:", formatCountInt(totalFilesAll), std::string()},
            {"total-size:", ut1::getApproxSizeStr(totalBytesAll, 3, true, false), std::string()},
            {"unique-files:", formatCountInt(totalUnique.files), "(" + totalUniqueFilesPct + " of total)"},
            {"unique-size:", ut1::getApproxSizeStr(totalUnique.bytes, 3, true, false), "(" + totalUniqueBytesPct + " of total)"},
            {"shared-files:", formatCountInt(totalShared.files), "(" + totalSharedFilesPct + " of total)"},
            {"shared-size:", ut1::getApproxSizeStr(totalShared.bytes, 3, true, false), "(" + totalSharedBytesPct + " of total)"}
        };
        if (removeCopies)
        {
            std::string removedFilesPct = formatPercentFixed(totalFilesAll == 0 ? 0.0 : (100.0 * removedFiles / totalFilesAll));
            std::string removedBytesPct = formatPercentFixed(totalBytesAll == 0 ? 0.0 : (100.0 * removedBytes / totalBytesAll));
            totalStats.push_back({"removed-files:", formatCountInt(removedFiles), "(" + removedFilesPct + " of total)"});
            totalStats.push_back({"removed-bytes:", ut1::getApproxSizeStr(removedBytes, 3, true, false), "(" + removedBytesPct + " of total)"});
        }

        std::cout << "total:\n";
        printStatList(totalStats);

        if (rootPaths.size() == 2)
        {
            auto& filesA = rootFiles[0];
            auto& filesB = rootFiles[1];
            if (extractA)
            {
                copyIntersectFiles(rootPaths[0], *extractA, filesA, filesB, dryRun);
            }
            if (extractB)
            {
                copyIntersectFiles(rootPaths[1], *extractB, filesB, filesA, dryRun);
            }
        }

        size_t hashLen = 0;
        if (clVerbose > 0 && (listA || listB || listBoth))
        {
            hashLen = getUniqueHashHexLen();
        }

        if (listA && rootPaths.size() == 2)
        {
            std::cout << "only-in-A:\n";
            if (clVerbose > 0)
            {
                std::vector<FileEntry> refs;
                for (const auto& [key, listARefs] : rootFiles[0])
                {
                    if (rootFiles[1].find(key) != rootFiles[1].end())
                    {
                        continue;
                    }
                    refs.insert(refs.end(), listARefs.begin(), listARefs.end());
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listARefs] : rootFiles[0])
                {
                    if (rootFiles[1].find(key) != rootFiles[1].end())
                    {
                        continue;
                    }
                    for (const auto& ref : listARefs)
                    {
                        std::cout << ref.path << "\n";
                    }
                }
            }
        }

        if (listB && rootPaths.size() == 2)
        {
            std::cout << "only-in-B:\n";
            if (clVerbose > 0)
            {
                std::vector<FileEntry> refs;
                for (const auto& [key, listBRefs] : rootFiles[1])
                {
                    if (rootFiles[0].find(key) != rootFiles[0].end())
                    {
                        continue;
                    }
                    refs.insert(refs.end(), listBRefs.begin(), listBRefs.end());
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listBRefs] : rootFiles[1])
                {
                    if (rootFiles[0].find(key) != rootFiles[0].end())
                    {
                        continue;
                    }
                    for (const auto& ref : listBRefs)
                    {
                        std::cout << ref.path << "\n";
                    }
                }
            }
        }

        if (listBoth && rootPaths.size() == 2)
        {
            std::cout << "in-both:\n";
            if (clVerbose > 0)
            {
                std::vector<FileEntry> refs;
                for (const auto& [key, listARefs] : rootFiles[0])
                {
                    auto itB = rootFiles[1].find(key);
                    if (itB == rootFiles[1].end())
                    {
                        continue;
                    }
                    for (const auto& ref : listARefs)
                    {
                        FileEntry labeled = ref;
                        labeled.path = "A: " + labeled.path;
                        refs.push_back(std::move(labeled));
                    }
                    for (const auto& ref : itB->second)
                    {
                        FileEntry labeled = ref;
                        labeled.path = "B: " + labeled.path;
                        refs.push_back(std::move(labeled));
                    }
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listARefs] : rootFiles[0])
                {
                    auto itB = rootFiles[1].find(key);
                    if (itB == rootFiles[1].end())
                    {
                        continue;
                    }
                    for (const auto& ref : listARefs)
                    {
                        std::cout << "A: " << ref.path << "\n";
                    }
                    for (const auto& ref : itB->second)
                    {
                        std::cout << "B: " << ref.path << "\n";
                    }
                }
            }
        }
    }

    /// Print the minimum hash length needed to distinguish distinct contents.
    void printUniqueHashLen() const
    {
        std::vector<Hash128> hashes;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                hashes.push_back(file.hash);
            }
        }
        size_t minBits = getMinUniqueHashBits(std::move(hashes));
        std::cout << "unique-hash-len: " << minBits << "\n";
    }

    struct HardlinkStats
    {
        uint64_t createdLinks{};
        uint64_t removedFiles{};
        uint64_t removedBytes{};
    };

    /// Replace duplicate files with hardlinks to the oldest file.
    HardlinkStats hardlinkCopies(uint64_t minSize, uint64_t maxHardlinks, bool dryRun) const
    {
        std::map<ContentKey, std::vector<FileEntry>> contentFiles;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                if (file.size < minSize)
                {
                    continue;
                }
                Hash128 keyHash = sameFilename ? hashWithFilename(file.hash, keyNameForPath(file.path)) : file.hash;
                ContentKey key{file.size, keyHash};
                contentFiles[key].push_back(FileEntry{
                    (dir.path / file.path).string(),
                    file.size,
                    file.hash,
                    file.inode,
                    file.date,
                    file.numLinks
                });
            }
        }

        HardlinkStats stats;
        std::set<fs::path> touchedDirs;
        for (const auto& [key, files] : contentFiles)
        {
            if (files.size() < 2)
            {
                continue;
            }
            auto oldestIt = std::min_element(files.begin(), files.end(),
                [](const FileEntry& a, const FileEntry& b)
                {
                    if (a.date != b.date)
                    {
                        return a.date < b.date;
                    }
                    return a.path < b.path;
                });
            if (oldestIt == files.end())
            {
                continue;
            }
            const FileEntry& oldest = *oldestIt;
            fs::path oldestPath(oldest.path);
            uint64_t linkCount = oldest.numLinks;
            if (!dryRun)
            {
                std::error_code ec;
                uint64_t currentLinks = fs::hard_link_count(oldestPath, ec);
                if (!ec)
                {
                    linkCount = currentLinks;
                }
                else if (clVerbose)
                {
                    std::cerr << "Warning: Failed to read hardlink count for " << oldestPath.string()
                              << ": " << ec.message() << "\n";
                }
            }
            if (linkCount >= maxHardlinks)
            {
                std::cerr << "Warning: " << oldestPath.string() << " has " << linkCount
                          << " hardlinks (>= " << maxHardlinks << "), skipping.\n";
                continue;
            }

            for (const auto& ref : files)
            {
                if (ref.path == oldest.path)
                {
                    continue;
                }
                if (ref.inode == oldest.inode)
                {
                    continue;
                }
                if (dryRun)
                {
                    std::cout << "Would hardlink " << ref.path << " -> " << oldest.path << "\n";
                }
                else
                {
                    std::string errorMsg;
                    if (!replaceWithHardlink(oldestPath, fs::path(ref.path), &errorMsg))
                    {
                        std::cerr << "Warning: " << errorMsg << "\n";
                        continue;
                    }
                    if (clVerbose)
                    {
                        std::cout << "Hardlinked " << ref.path << " -> " << oldest.path << "\n";
                    }
                    touchedDirs.insert(oldestPath.parent_path());
                    touchedDirs.insert(fs::path(ref.path).parent_path());
                }
                stats.createdLinks++;
                stats.removedFiles++;
                stats.removedBytes += ref.size;
            }
        }

        if (!dryRun)
        {
            for (const auto& dirPath : touchedDirs)
            {
                if (ut1::fsExists(dirPath / ".dirdb"))
                {
                    updateDirDb(dirPath);
                }
            }
        }

        return stats;
    }

    /// Print hardlink operation statistics.
    void printHardlinkStats(const HardlinkStats& stats) const
    {
        std::vector<StatLine> lines = {
            {"hardlinks-created:", formatCountInt(stats.createdLinks), std::string()},
            {"removed-files:", formatCountInt(stats.removedFiles), std::string()},
            {"removed-bytes:", ut1::getApproxSizeStr(stats.removedBytes, 3, true, false), std::string()}
        };
        printStatList(lines);
    }

private:
    /// Extract the basename used for same-filename matching.
    static std::string keyNameForPath(const std::string& path)
    {
        return fs::path(path).filename().string();
    }

    /// Combine a content hash with a filename to form a single hash.
    static Hash128 hashWithFilename(const Hash128& base, const std::string& name)
    {
        HashSha3_128 hasher;
        std::vector<uint8_t> bytes = base.toBytes();
        hasher.update(bytes.data(), bytes.size());
        if (!name.empty())
        {
            hasher.update(reinterpret_cast<const uint8_t*>(name.data()), name.size());
        }
        std::vector<uint8_t> digest = hasher.finalize();
        if (digest.size() < 16)
        {
            return base;
        }
        return Hash128::fromBytes(digest);
    }

    struct ContentKey
    {
        uint64_t size{};
        Hash128 hash{};
        /// Order by size then hash.
        bool operator<(const ContentKey& other) const
        {
            if (size != other.size)
            {
                return size < other.size;
            }
            return hash < other.hash;
        }
    };

    struct StatLine
    {
        std::string label;
        std::string value;
        std::string extra;
    };

    /// Print rows for file listings with aligned columns.
    static void printListRows(const std::vector<FileEntry>& refs, bool showInodeLinks, size_t hashLen)
    {
        struct Row
        {
            std::string size;
            std::string hash;
            std::string inode;
            std::string date;
            std::string numLinks;
            std::string name;
        };

        std::vector<Row> rows;
        size_t widthSize = 0;
        size_t widthHash = 0;
        size_t widthInode = 0;
        size_t widthDate = 0;
        size_t widthLinks = 0;

        for (const auto& ref : refs)
        {
            Row row;
            row.size = ut1::toStr(ref.size);
            std::string hex = ref.hash.toHex();
            row.hash = hex.substr(0, std::min(hashLen, hex.size()));
            row.inode = ut1::toStr(ref.inode);
            row.date = formatFileTime(ref.date);
            row.numLinks = ut1::toStr(ref.numLinks);
            row.name = ref.path;

            widthSize = std::max(widthSize, row.size.size());
            widthHash = std::max(widthHash, row.hash.size());
            if (showInodeLinks)
            {
                widthInode = std::max(widthInode, row.inode.size());
                widthLinks = std::max(widthLinks, row.numLinks.size());
            }
            widthDate = std::max(widthDate, row.date.size());

            rows.push_back(std::move(row));
        }

        for (const auto& row : rows)
        {
            std::cout << std::setw(static_cast<int>(widthSize)) << row.size << " "
                      << std::setw(static_cast<int>(widthHash)) << row.hash << " ";
            if (showInodeLinks)
            {
                std::cout << std::setw(static_cast<int>(widthInode)) << row.inode << " ";
            }
            std::cout << std::setw(static_cast<int>(widthDate)) << row.date << " ";
            if (showInodeLinks)
            {
                std::cout << std::setw(static_cast<int>(widthLinks)) << row.numLinks << " ";
            }
            std::cout << row.name << "\n";
        }
    }

    /// Print aligned statistics lines.
    static void printStatList(const std::vector<StatLine>& lines)
    {
        size_t labelWidth = 0;
        size_t maxDecimalPos = 0;
        size_t maxExtraDecimalPos = 0;
        for (const auto& line : lines)
        {
            labelWidth = std::max(labelWidth, line.label.size());
            maxDecimalPos = std::max(maxDecimalPos, getStatDecimalPos(line.value));
            if (!line.extra.empty())
            {
                maxExtraDecimalPos = std::max(maxExtraDecimalPos, getStatDecimalPos(line.extra));
            }
        }
        size_t decimalCol = labelWidth + 1 + maxDecimalPos;

        size_t maxValueWidth = 0;
        size_t maxExtraWidth = 0;
        std::vector<std::string> alignedValues;
        std::vector<std::string> alignedExtras;
        alignedValues.reserve(lines.size());
        alignedExtras.reserve(lines.size());
        for (const auto& line : lines)
        {
            std::string valueAligned = formatAlignedStatValue(line.value, labelWidth, decimalCol);
            maxValueWidth = std::max(maxValueWidth, valueAligned.size());
            alignedValues.push_back(std::move(valueAligned));
            if (!line.extra.empty())
            {
                size_t extraCol = labelWidth + 1 + maxExtraDecimalPos;
                std::string extraAligned = formatAlignedStatValue(line.extra, labelWidth, extraCol);
                maxExtraWidth = std::max(maxExtraWidth, extraAligned.size());
                alignedExtras.push_back(std::move(extraAligned));
            }
            else
            {
                alignedExtras.emplace_back();
            }
        }

        for (size_t i = 0; i < lines.size(); i++)
        {
            const auto& line = lines[i];
            const std::string& valueAligned = alignedValues[i];
            std::string lineOut = line.label + std::string(labelWidth - line.label.size(), ' ') + " " + valueAligned;
            if (!line.extra.empty())
            {
                lineOut += std::string(maxValueWidth - valueAligned.size(), ' ');
                const std::string& extraAligned = alignedExtras[i];
                lineOut += " " + extraAligned;
            }
            std::cout << lineOut << "\n";
        }
    }

    /// Copy files that exist only in the source root.
    static void copyIntersectFiles(const fs::path& rootSrc, const fs::path& destRoot,
        const std::map<ContentKey, std::vector<FileEntry>>& filesSrc,
        const std::map<ContentKey, std::vector<FileEntry>>& filesOther,
        bool dryRun)
    {
        if (fs::exists(destRoot))
        {
            throw std::runtime_error("Destination exists: " + destRoot.string());
        }
        if (!dryRun)
        {
            fs::create_directories(destRoot);
        }

        for (const auto& [key, listRefs] : filesSrc)
        {
            if (filesOther.find(key) != filesOther.end())
            {
                continue;
            }
            for (const auto& ref : listRefs)
            {
                fs::path srcPath(ref.path);
                std::error_code ec;
                fs::path rel = fs::relative(srcPath, rootSrc, ec);
                if (ec)
                {
                    throw std::runtime_error("Failed to compute relative path for " + srcPath.string());
                }
                fs::path destPath = destRoot / rel;
                if (dryRun)
                {
                    std::cout << "Would copy " << srcPath.string() << " -> " << destPath.string() << "\n";
                    continue;
                }
                fs::create_directories(destPath.parent_path());
                fs::copy_file(srcPath, destPath, fs::copy_options::none, ec);
                if (ec)
                {
                    throw std::runtime_error("Failed to copy to " + destPath.string());
                }
            }
        }
    }

    /// Remove duplicate files from later roots, keeping earliest roots.
    static std::pair<uint64_t, uint64_t> removeCopyFiles(const std::vector<std::map<ContentKey, std::vector<FileEntry>>>& rootFiles, bool dryRun)
    {
        std::map<ContentKey, size_t> firstRoot;
        uint64_t removedFiles = 0;
        uint64_t removedBytes = 0;
        std::set<fs::path> touchedDirs;
        for (size_t i = 0; i < rootFiles.size(); i++)
        {
            for (const auto& [key, listRefs] : rootFiles[i])
            {
                if (listRefs.empty())
                {
                    continue;
                }
                auto it = firstRoot.find(key);
                if (it == firstRoot.end())
                {
                    firstRoot.emplace(key, i);
                    continue;
                }
                if (i > it->second)
                {
                    for (const auto& ref : listRefs)
                    {
                        if (dryRun || clVerbose)
                        {
                            std::cout << (dryRun ? "Would remove " : "Removed ") << ref.path << "\n";
                        }
                        removedFiles++;
                        removedBytes += ref.size;
                        if (dryRun)
                        {
                            continue;
                        }
                        std::error_code ec;
                        fs::remove(ref.path, ec);
                        if (ec)
                        {
                            throw std::runtime_error("Failed to remove " + ref.path);
                        }
                        touchedDirs.insert(fs::path(ref.path).parent_path());
                    }
                }
            }
        }
        if (!dryRun)
        {
            for (const auto& dirPath : touchedDirs)
            {
                if (ut1::fsExists(dirPath / ".dirdb"))
                {
                    updateDirDb(dirPath);
                }
            }
        }
        return {removedFiles, removedBytes};
    }

    /// Replace a file with a hardlink to the source using a temporary path.
    static bool replaceWithHardlink(const fs::path& source, const fs::path& target, std::string* errorMsg)
    {
        std::error_code ec;
        fs::path temp;
        for (int i = 0; i < 100; i++)
        {
            temp = target;
            temp += ".treeop_link_tmp";
            if (i > 0)
            {
                temp += ut1::toStr(i);
            }
            if (!fs::exists(temp, ec))
            {
                break;
            }
        }
        if (fs::exists(temp, ec))
        {
            if (errorMsg)
            {
                *errorMsg = "No temporary path available for " + target.string();
            }
            return false;
        }

        fs::create_hard_link(source, temp, ec);
        if (ec)
        {
            if (errorMsg)
            {
                *errorMsg = "Failed to create hardlink for " + target.string() + ": " + ec.message();
            }
            return false;
        }

        fs::rename(temp, target, ec);
        if (ec)
        {
            std::error_code rmEc;
            fs::remove(target, rmEc);
            fs::rename(temp, target, ec);
        }
        if (ec)
        {
            std::error_code rmEc;
            fs::remove(temp, rmEc);
            if (errorMsg)
            {
                *errorMsg = "Failed to replace " + target.string() + ": " + ec.message();
            }
            return false;
        }
        return true;
    }

    /// Walk a directory tree and load or create .dirdb files.
    void processDirTree(const fs::path& root, bool forceCreate, bool update)
    {
        addDir(loadOrCreateDirDb(root, forceCreate, update));

        std::error_code ec;
        fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec);
        fs::recursive_directory_iterator end;
        while (it != end)
        {
            if (ec)
            {
                if (clVerbose)
                {
                    if (it != end)
                    {
                        std::cerr << "Skipping entry due to error: " << it->path() << "\n";
                    }
                }
                ec.clear();
                it.increment(ec);
                continue;
            }
            if (ut1::getFileType(*it, false) == ut1::FT_DIR)
            {
                addDir(loadOrCreateDirDb(it->path(), forceCreate, update));
            }
            it.increment(ec);
        }
    }

    /// Compute the minimal hex prefix length to distinguish all hashes.
    size_t getUniqueHashHexLen() const
    {
        std::vector<Hash128> hashes;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                hashes.push_back(file.hash);
            }
        }
        size_t minBits = getMinUniqueHashBits(std::move(hashes));
        size_t nibbles = (minBits + 3) / 4;
        if (nibbles < 4)
        {
            nibbles = 4;
        }
        if (nibbles > 32)
        {
            nibbles = 32;
        }
        return nibbles;
    }

    /// Compute the minimum number of hash bits needed to distinguish values.
    static size_t getMinUniqueHashBits(std::vector<Hash128> hashes)
    {
        if (hashes.size() <= 1)
        {
            return 0;
        }
        std::sort(hashes.begin(), hashes.end());
        hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
        if (hashes.size() <= 1)
        {
            return 0;
        }
        // After sorting, the longest common prefix between any two distinct hashes
        // must occur between neighboring entries, so only adjacent pairs are needed.
        size_t maxCommonPrefix = 0;
        for (size_t i = 1; i < hashes.size(); i++)
        {
            uint64_t hiXor = hashes[i].hi ^ hashes[i - 1].hi;
            size_t common = 0;
            if (hiXor == 0)
            {
                uint64_t loXor = hashes[i].lo ^ hashes[i - 1].lo;
                common = 64 + std::countl_zero(loXor);
            }
            else
            {
                common = std::countl_zero(hiXor);
            }
            if (common > maxCommonPrefix)
            {
                maxCommonPrefix = common;
            }
        }
        return std::min<size_t>(128, maxCommonPrefix + 1);
    }

    /// Check if a path is within a root path.
    static bool isPathWithin(const fs::path& root, const fs::path& path)
    {
        auto rootIt = root.begin();
        auto pathIt = path.begin();
        for (; rootIt != root.end() && pathIt != path.end(); ++rootIt, ++pathIt)
        {
            if (*rootIt != *pathIt)
            {
                return false;
            }
        }
        return rootIt == root.end();
    }

    /// Format a percentage with one decimal place.
    static std::string formatPercentFixed(double percent)
    {
        std::ostringstream os;
        os << std::fixed << std::setprecision(1) << percent << "%";
        return os.str();
    }

    /// Format an integer count as a string.
    static std::string formatCountInt(uint64_t count)
    {
        return ut1::toStr(count);
    }

    /// Return the decimal point column within a numeric string.
    static size_t getDecimalPos(const std::string& value)
    {
        size_t pos = value.find('.');
        if (pos == std::string::npos)
        {
            return value.size();
        }
        return pos;
    }

    /// Return decimal position for stat values with optional suffix.
    static size_t getStatDecimalPos(const std::string& value)
    {
        size_t end = value.find(' ');
        std::string_view number = (end == std::string::npos)
            ? std::string_view(value)
            : std::string_view(value).substr(0, end);
        size_t pos = number.find('.');
        return (pos == std::string::npos) ? number.size() : pos;
    }

    /// Align stat values on their decimal point.
    static std::string formatAlignedStatValue(const std::string& value, size_t labelWidth, size_t decimalCol)
    {
        size_t decimalPos = getStatDecimalPos(value);
        size_t currentCol = labelWidth + 1 + decimalPos;
        size_t padding = (decimalCol > currentCol) ? (decimalCol - currentCol) : 0;
        return std::string(padding, ' ') + value;
    }

    /// Pad a string on the right to a width.
    static std::string padRight(const std::string& value, size_t width)
    {
        if (value.size() >= width)
        {
            return value;
        }
        return value + std::string(width - value.size(), ' ');
    }

    /// Pad a string on the left to a width.
    static std::string padLeft(const std::string& value, size_t width)
    {
        if (value.size() >= width)
        {
            return value;
        }
        return std::string(width - value.size(), ' ') + value;
    }

    /// Align a numeric string to a given decimal column.
    static std::string alignDecimalTo(const std::string& value, size_t decimalPos)
    {
        size_t pos = getDecimalPos(value);
        if (pos >= decimalPos)
        {
            return value;
        }
        return std::string(decimalPos - pos, ' ') + value;
    }

    /// Split a size string into number and suffix.
    static std::pair<std::string, std::string> splitSizeStr(const std::string& value)
    {
        size_t sep = value.rfind(' ');
        if (sep == std::string::npos)
        {
            return {value, std::string()};
        }
        return {value.substr(0, sep), value.substr(sep + 1)};
    }

    /// Align a size string on its decimal and suffix columns.
    static std::string formatSizeAligned(const std::string& value, size_t decimalPos, size_t suffixWidth)
    {
        auto [numberStr, suffixStr] = splitSizeStr(value);
        numberStr = alignDecimalTo(numberStr, decimalPos);
        suffixStr = padRight(suffixStr, suffixWidth);
        if (suffixWidth == 0)
        {
            return numberStr;
        }
        return numberStr + " " + suffixStr;
    }

    /// Format one histogram boundary with the selected unit.
    static std::string formatHistogramBoundary(uint64_t value, uint64_t unitFactor, const std::string& unitLabel, size_t numberWidth)
    {
        std::string number = ut1::toStr(value / unitFactor);
        return padLeft(number, numberWidth) + " " + unitLabel;
    }

    /// Format FILETIME ticks (100ns since 1601-01-01 UTC) into a UTC timestamp.
    static std::string formatFileTime(uint64_t fileTime)
    {
        if (fileTime == 0)
        {
            return "0000-00-00 00:00:00";
        }
        uint64_t seconds = fileTime / 10000000ULL;
        if (seconds < kWindowsToUnixEpoch)
        {
            return "0000-00-00 00:00:00";
        }
        time_t unixSeconds = static_cast<time_t>(seconds - kWindowsToUnixEpoch);
        std::tm tm{};
#ifdef _WIN32
        gmtime_s(&tm, &unixSeconds);
#else
        gmtime_r(&unixSeconds, &tm);
#endif
        std::ostringstream os;
        os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (clVerbose >= 3)
        {
            uint64_t micro = (fileTime / 10ULL) % 1000000ULL;
            os << "." << std::setw(6) << std::setfill('0') << micro;
        }
        return os.str();
    }

    std::vector<RootData> roots;
    std::vector<DirDbData> dirs;
    bool sameFilename{};
};

struct ReadBenchStats
{
    uint64_t files{};
    uint64_t bytes{};
    uint64_t dirs{};
    double elapsed{};
};

/// Read all files under the given roots to measure read performance.
static ReadBenchStats runReadBench(const std::vector<fs::path>& roots)
{
    ReadBenchStats stats;
    std::vector<uint8_t> buffer(gBufSize);
    double start = ut1::getTimeSec();

    for (const auto& root : roots)
    {
        if (gProgress)
        {
            gProgress->onDirStart(root);
        }
        stats.dirs++;
        std::error_code ec;
        fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec);
        fs::recursive_directory_iterator end;
        while (it != end)
        {
            if (ec)
            {
                if (clVerbose)
                {
                    if (it != end)
                    {
                        std::cerr << "Skipping entry due to error: " << it->path() << "\n";
                    }
                }
                ec.clear();
                it.increment(ec);
                continue;
            }
            if (it->path().filename() == ".dirdb")
            {
                it.increment(ec);
                continue;
            }
            if (ut1::getFileType(*it, false) == ut1::FT_DIR)
            {
                if (gProgress)
                {
                    gProgress->onDirStart(it->path());
                    gProgress->onDirDone();
                }
                stats.dirs++;
                it.increment(ec);
                continue;
            }
            if (ut1::getFileType(*it, false) != ut1::FT_REGULAR)
            {
                it.increment(ec);
                continue;
            }

            uint64_t size = 0;
            try
            {
                size = static_cast<uint64_t>(it->file_size());
            }
            catch (const std::exception&)
            {
                throw std::runtime_error("Error while reading file size: " + it->path().string());
            }

            if (clVerbose)
            {
                std::cout << "Reading " << it->path().string() << "\n";
            }

            std::ifstream is(it->path(), std::ios::binary);
            if (!is)
            {
                throw std::runtime_error("Error while opening file for reading: " + it->path().string());
            }
            if (gProgress)
            {
                gProgress->onHashStart(it->path(), size);
            }
            while (is)
            {
                is.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
                std::streamsize count = is.gcount();
                if (count > 0)
                {
                    stats.bytes += static_cast<uint64_t>(count);
                    if (gProgress)
                    {
                        gProgress->onHashProgress(static_cast<uint64_t>(count));
                    }
                }
            }
            if (gProgress)
            {
                gProgress->onHashEnd();
                gProgress->onFileProcessed(size);
            }
            stats.files++;
            it.increment(ec);
        }
        if (gProgress)
        {
            gProgress->onDirDone();
        }
    }

    stats.elapsed = ut1::getTimeSec() - start;
    return stats;
}

/// Normalize a path for consistent comparisons.
static fs::path normalizePath(const fs::path& path)
{
    std::error_code ec;
    fs::path abs = fs::absolute(path, ec);
    if (ec)
    {
        fs::path normalized = path.lexically_normal();
        if (normalized.has_filename() == false && normalized != normalized.root_path())
        {
            normalized = normalized.parent_path();
        }
        return normalized;
    }
    fs::path normalized = abs.lexically_normal();
    if (normalized.has_filename() == false && normalized != normalized.root_path())
    {
        normalized = normalized.parent_path();
    }
    return normalized;
}

/// Create a little-endian 8-byte tag value.
static uint64_t makeTag(const char* tag)
{
    uint64_t value = 0;
    for (size_t i = 0; i < 8 && tag[i]; i++)
    {
        value |= (static_cast<uint64_t>(static_cast<unsigned char>(tag[i])) << (8 * i));
    }
    return value;
}

/// Append a uint64_t in little-endian order.
static void appendU64Le(std::vector<uint8_t>& out, uint64_t value)
{
    for (size_t i = 0; i < 8; i++)
    {
        out.push_back(static_cast<uint8_t>(value & 0xff));
        value >>= 8;
    }
}

/// Read a uint64_t from a byte buffer in little-endian order.
static uint64_t readU64Le(const uint8_t* data, size_t size, size_t& offset, const char* what)
{
    if (offset + 8 > size)
    {
        throw std::runtime_error(std::string("Unexpected end of .dirdb while reading ") + what);
    }
    uint64_t value = 0;
    for (size_t i = 0; i < 8; i++)
    {
        value |= (static_cast<uint64_t>(data[offset + i]) << (8 * i));
    }
    offset += 8;
    return value;
}

/// Append a length-prefixed string to a byte buffer.
static void appendLengthString(std::vector<uint8_t>& out, const std::string& s)
{
    size_t len = s.size();
    if (len <= 0xfc)
    {
        out.push_back(static_cast<uint8_t>(len));
    }
    else if (len <= 0xffff)
    {
        out.push_back(0xff);
        out.push_back(static_cast<uint8_t>(len & 0xff));
        out.push_back(static_cast<uint8_t>((len >> 8) & 0xff));
    }
    else if (len <= 0xffffffffULL)
    {
        out.push_back(0xfe);
        for (size_t i = 0; i < 4; i++)
        {
            out.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xff));
        }
    }
    else
    {
        out.push_back(0xfd);
        for (size_t i = 0; i < 8; i++)
        {
            out.push_back(static_cast<uint8_t>((len >> (8 * i)) & 0xff));
        }
    }
    out.insert(out.end(), s.begin(), s.end());
}

/// Read a length-prefixed string from a byte buffer.
static std::string readLengthStringAt(const std::vector<uint8_t>& data, size_t offset)
{
    if (offset >= data.size())
    {
        throw std::runtime_error("Invalid string offset in .dirdb");
    }
    size_t pos = offset;
    uint8_t prefix = data[pos++];
    uint64_t len = 0;
    if (prefix <= 0xfc)
    {
        len = prefix;
    }
    else if (prefix == 0xff)
    {
        if (pos + 2 > data.size())
        {
            throw std::runtime_error("Invalid 2-byte length in .dirdb");
        }
        len = data[pos] | (static_cast<uint64_t>(data[pos + 1]) << 8);
        pos += 2;
    }
    else if (prefix == 0xfe)
    {
        if (pos + 4 > data.size())
        {
            throw std::runtime_error("Invalid 4-byte length in .dirdb");
        }
        len = 0;
        for (size_t i = 0; i < 4; i++)
        {
            len |= (static_cast<uint64_t>(data[pos + i]) << (8 * i));
        }
        pos += 4;
    }
    else if (prefix == 0xfd)
    {
        if (pos + 8 > data.size())
        {
            throw std::runtime_error("Invalid 8-byte length in .dirdb");
        }
        len = 0;
        for (size_t i = 0; i < 8; i++)
        {
            len |= (static_cast<uint64_t>(data[pos + i]) << (8 * i));
        }
        pos += 8;
    }
    else
    {
        throw std::runtime_error("Invalid string length prefix in .dirdb");
    }

    if (pos + len > data.size())
    {
        throw std::runtime_error("Invalid string length in .dirdb");
    }
    return std::string(reinterpret_cast<const char*>(data.data() + pos), static_cast<size_t>(len));
}

static uint64_t fileTimeFromTimespec(const timespec& ts)
{
    if (ts.tv_sec < 0)
    {
        return 0;
    }
    uint64_t sec = static_cast<uint64_t>(ts.tv_sec);
    uint64_t ft = (sec + kWindowsToUnixEpoch) * 10000000ULL;
    ft += static_cast<uint64_t>(ts.tv_nsec) / 100ULL;
    return ft;
}

/// Hash a file into a 128-bit value and optionally report time spent.
static Hash128 hashFile128(const fs::path& path, uint64_t fileSize, double* secondsOut)
{
    HashSha3_128 hasher;
    std::ifstream is(path, std::ios::binary);
    if (!is)
    {
        throw std::runtime_error("Error while opening file for hashing: " + path.string());
    }
    if (gProgress)
    {
        gProgress->onHashStart(path, fileSize);
    }
    std::vector<uint8_t> buffer(gBufSize);
    double start = ut1::getTimeSec();
    while (is)
    {
        is.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
        std::streamsize count = is.gcount();
        if (count > 0)
        {
            hasher.update(buffer.data(), static_cast<size_t>(count));
            if (gProgress)
            {
                gProgress->onHashProgress(static_cast<uint64_t>(count));
            }
        }
    }
    double end = ut1::getTimeSec();
    if (secondsOut)
    {
        *secondsOut = end - start;
    }
    if (gProgress)
    {
        gProgress->onHashEnd();
    }
    std::vector<uint8_t> digest = hasher.finalize();
    if (digest.size() < 16)
    {
        throw std::runtime_error("Unexpected hash size while hashing: " + path.string());
    }
    return Hash128::fromBytes(digest);
}

/// Read a .dirdb file for a directory and return its contents.
static DirDbData readDirDb(const fs::path& dirPath, bool reportProgress = true)
{
    fs::path dbPath = dirPath / ".dirdb";
    std::string raw = ut1::readFile(dbPath.string());
    const uint8_t* data = reinterpret_cast<const uint8_t*>(raw.data());
    size_t size = raw.size();
    size_t pos = 0;

    uint64_t tag = readU64Le(data, size, pos, "DirDB tag");
    if (tag != makeTag("DirDB"))
    {
        throw std::runtime_error("Invalid .dirdb tag in " + dbPath.string());
    }
    uint64_t version = readU64Le(data, size, pos, "version");
    if (version != kDirDbVersion)
    {
        throw std::runtime_error("Unsupported .dirdb version in " + dbPath.string());
    }

    tag = readU64Le(data, size, pos, "TOC tag");
    if (tag != makeTag("TOC"))
    {
        throw std::runtime_error("Missing TOC tag in " + dbPath.string());
    }
    uint64_t tocCount = readU64Le(data, size, pos, "TOC count");
    uint64_t tocEntrySize = readU64Le(data, size, pos, "TOC entry size");
    if (tocEntrySize < 16)
    {
        throw std::runtime_error("Unsupported TOC entry size in " + dbPath.string());
    }
    struct TocEntry { uint64_t size; uint64_t fileIndex; };
    std::vector<TocEntry> tocEntries;
    for (uint64_t i = 0; i < tocCount; i++)
    {
        size_t entryStart = pos;
        TocEntry entry{};
        entry.size = readU64Le(data, size, pos, "TOC size");
        entry.fileIndex = readU64Le(data, size, pos, "TOC fileIndex");
        size_t expectedEnd = entryStart + static_cast<size_t>(tocEntrySize);
        if (expectedEnd > size)
        {
            throw std::runtime_error("Unexpected end of TOC in " + dbPath.string());
        }
        pos = expectedEnd;
        tocEntries.push_back(entry);
    }

    tag = readU64Le(data, size, pos, "FILES tag");
    if (tag != makeTag("FILES"))
    {
        throw std::runtime_error("Missing FILES tag in " + dbPath.string());
    }
    uint64_t fileCount = readU64Le(data, size, pos, "file count");
    uint64_t fileEntrySize = readU64Le(data, size, pos, "file entry size");
    if (fileEntrySize < 48)
    {
        throw std::runtime_error("Unsupported file entry size in " + dbPath.string());
    }
    struct RawFileEntry
    {
        uint64_t nameIndex;
        Hash128 hash;
        uint64_t inode;
        uint64_t date; // FILETIME ticks (100ns since 1601-01-01 UTC).
        uint64_t numLinks;
    };
    std::vector<RawFileEntry> rawEntries;
    for (uint64_t i = 0; i < fileCount; i++)
    {
        size_t entryStart = pos;
        RawFileEntry entry{};
        entry.nameIndex = readU64Le(data, size, pos, "nameIndex");
        entry.hash.lo = readU64Le(data, size, pos, "hashLo");
        entry.hash.hi = readU64Le(data, size, pos, "hashHi");
        entry.inode = readU64Le(data, size, pos, "inodeNumber");
        entry.date = readU64Le(data, size, pos, "date");
        entry.numLinks = readU64Le(data, size, pos, "numLinks");
        size_t expectedEnd = entryStart + static_cast<size_t>(fileEntrySize);
        if (expectedEnd > size)
        {
            throw std::runtime_error("Unexpected end of file entries in " + dbPath.string());
        }
        pos = expectedEnd;
        rawEntries.push_back(entry);
    }

    tag = readU64Le(data, size, pos, "STRINGS tag");
    if (tag != makeTag("STRINGS"))
    {
        throw std::runtime_error("Missing STRINGS tag in " + dbPath.string());
    }
    uint64_t stringsSize = readU64Le(data, size, pos, "strings size");
    if (pos + stringsSize > size)
    {
        throw std::runtime_error("Invalid STRINGS size in " + dbPath.string());
    }
    std::vector<uint8_t> strings(data + pos, data + pos + stringsSize);
    pos += stringsSize;

    std::vector<FileSize> sizes(static_cast<size_t>(fileCount), 0);
    if (!rawEntries.empty() && tocEntries.empty())
    {
        throw std::runtime_error("Missing TOC entries in " + dbPath.string());
    }
    for (size_t i = 0; i < tocEntries.size(); i++)
    {
        size_t start = static_cast<size_t>(tocEntries[i].fileIndex);
        size_t end = (i + 1 < tocEntries.size())
            ? static_cast<size_t>(tocEntries[i + 1].fileIndex)
            : static_cast<size_t>(fileCount);
        if (start > end || end > rawEntries.size())
        {
            throw std::runtime_error("Invalid TOC index in " + dbPath.string());
        }
        for (size_t j = start; j < end; j++)
        {
            sizes[j] = tocEntries[i].size;
        }
    }

    DirDbData dirData;
    dirData.path = normalizePath(dirPath);
    dirData.dbSize = static_cast<uint64_t>(ut1::getFileSize(dbPath.string()));
    dirData.hashedBytes = 0;
    dirData.hashSeconds = 0.0;
    for (size_t i = 0; i < rawEntries.size(); i++)
    {
        const auto& rawEntry = rawEntries[i];
        if (rawEntry.nameIndex >= strings.size())
        {
            throw std::runtime_error("Invalid name index in " + dbPath.string());
        }
        FileEntry entry;
        entry.path = readLengthStringAt(strings, static_cast<size_t>(rawEntry.nameIndex));
        entry.size = sizes[i];
        entry.hash = rawEntry.hash;
        entry.inode = rawEntry.inode;
        entry.date = rawEntry.date;
        entry.numLinks = rawEntry.numLinks;
        dirData.files.push_back(std::move(entry));
    }

    if (gProgress && reportProgress)
    {
        uint64_t totalBytes = 0;
        for (const auto& file : dirData.files)
        {
            totalBytes += file.size;
        }
        gProgress->onDirStart(dirPath);
        gProgress->addDirSummary(dirData.files.size(), totalBytes);
    }

    return dirData;
}

struct HashReuseKey
{
    uint64_t inode{};
    uint64_t size{};
    uint64_t date{}; // FILETIME ticks (100ns since 1601-01-01 UTC).

    /// Compare cache keys for reuse.
    bool operator==(const HashReuseKey& other) const
    {
        return inode == other.inode && size == other.size && date == other.date;
    }
};

struct HashReuseKeyHasher
{
    /// Hash a cache key for unordered_map lookups.
    size_t operator()(const HashReuseKey& key) const
    {
        size_t h1 = std::hash<uint64_t>{}(key.inode);
        size_t h2 = std::hash<uint64_t>{}(key.size);
        size_t h3 = std::hash<uint64_t>{}(key.date);
        size_t h = h1;
        h ^= h2 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= h3 + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return h;
    }
};

/// Scan a directory and build a new .dirdb file, reusing hashes when possible.
static DirDbData buildDirDb(const fs::path& dirPath, const std::unordered_map<HashReuseKey, FileEntry, HashReuseKeyHasher>* cache)
{
    if (clVerbose > 0)
    {
        std::cout << "Scanning " << dirPath.string() << "\n";
    }
    if (gProgress)
    {
        gProgress->onDirStart(dirPath);
    }

    std::vector<FileEntry> entries;
    uint64_t hashedBytes = 0;
    double hashSeconds = 0.0;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dirPath, fs::directory_options::skip_permission_denied, ec))
    {
        if (ec)
        {
            throw std::runtime_error("Error while scanning directory: " + dirPath.string());
        }
        if (entry.path().filename() == ".dirdb")
        {
            continue;
        }
        if (ut1::getFileType(entry, false) != ut1::FT_REGULAR)
        {
            continue;
        }
        uint64_t size = entry.file_size();
        if (gProgress)
        {
            gProgress->onFileProcessed(size);
        }
        ut1::StatInfo statInfo = ut1::getStat(entry, false);
        uint64_t date = fileTimeFromTimespec(statInfo.getMTimeSpec());
        Hash128 hash{};
        bool reusedHash = false;
        if (cache)
        {
            HashReuseKey key{static_cast<uint64_t>(statInfo.getIno()), size, date};
            auto it = cache->find(key);
            if (it != cache->end())
            {
                const auto& cached = it->second;
                hash = cached.hash;
                reusedHash = true;
            }
        }
        if (!reusedHash)
        {
            double seconds = 0.0;
            hash = hashFile128(entry.path(), size, &seconds);
            hashedBytes += size;
            hashSeconds += seconds;
        }
        FileEntry scan;
        scan.path = entry.path().filename().string();
        scan.size = size;
        scan.hash = hash;
        scan.inode = static_cast<uint64_t>(statInfo.getIno());
        scan.date = date;
        scan.numLinks = static_cast<uint64_t>(statInfo.statData.st_nlink);
        entries.push_back(std::move(scan));
    }
    if (ec)
    {
        throw std::runtime_error("Error while scanning directory: " + dirPath.string());
    }
    if (gProgress)
    {
        gProgress->onDirDone();
    }

    std::sort(entries.begin(), entries.end(), [](const FileEntry& a, const FileEntry& b)
    {
        if (a.size != b.size)
        {
            return a.size < b.size;
        }
        return a.path < b.path;
    });

    struct TocEntry { uint64_t size; uint64_t fileIndex; };
    std::vector<TocEntry> tocEntries;
    for (size_t i = 0; i < entries.size(); i++)
    {
        if (i == 0 || entries[i].size != entries[i - 1].size)
        {
            TocEntry tocEntry{};
            tocEntry.size = entries[i].size;
            tocEntry.fileIndex = i;
            tocEntries.push_back(tocEntry);
        }
    }

    std::vector<uint8_t> stringData;
    struct RawFileEntry
    {
        uint64_t nameIndex;
        Hash128 hash;
        uint64_t inode;
        uint64_t date; // FILETIME ticks (100ns since 1601-01-01 UTC).
        uint64_t numLinks;
    };
    std::vector<RawFileEntry> rawEntries;
    for (const auto& entry : entries)
    {
        RawFileEntry raw{};
        raw.nameIndex = stringData.size();
        appendLengthString(stringData, entry.path);
        raw.hash = entry.hash;
        raw.inode = entry.inode;
        raw.date = entry.date;
        raw.numLinks = entry.numLinks;
        rawEntries.push_back(raw);
    }

    std::vector<uint8_t> out;
    appendU64Le(out, makeTag("DirDB"));
    appendU64Le(out, kDirDbVersion);
    appendU64Le(out, makeTag("TOC"));
    appendU64Le(out, tocEntries.size());
    appendU64Le(out, 16);
    for (const auto& toc : tocEntries)
    {
        appendU64Le(out, toc.size);
        appendU64Le(out, toc.fileIndex);
    }
    appendU64Le(out, makeTag("FILES"));
    appendU64Le(out, rawEntries.size());
    appendU64Le(out, 48);
    for (const auto& raw : rawEntries)
    {
        appendU64Le(out, raw.nameIndex);
        appendU64Le(out, raw.hash.lo);
        appendU64Le(out, raw.hash.hi);
        appendU64Le(out, raw.inode);
        appendU64Le(out, raw.date);
        appendU64Le(out, raw.numLinks);
    }
    appendU64Le(out, makeTag("STRINGS"));
    appendU64Le(out, stringData.size());
    out.insert(out.end(), stringData.begin(), stringData.end());

    fs::path dbPath = dirPath / ".dirdb";
    std::string raw(reinterpret_cast<const char*>(out.data()), out.size());
    ut1::writeFile(dbPath.string(), raw);

    DirDbData dirData;
    dirData.path = normalizePath(dirPath);
    dirData.dbSize = static_cast<uint64_t>(ut1::getFileSize(dbPath.string()));
    dirData.hashedBytes = hashedBytes;
    dirData.hashSeconds = hashSeconds;
    for (const auto& entry : entries)
    {
        FileEntry file;
        file.path = entry.path;
        file.size = entry.size;
        file.hash = entry.hash;
        file.inode = entry.inode;
        file.date = entry.date;
        file.numLinks = entry.numLinks;
        dirData.files.push_back(std::move(file));
    }
    return dirData;
}

/// Create a new .dirdb file for a directory.
static DirDbData createDirDb(const fs::path& dirPath)
{
    return buildDirDb(dirPath, nullptr);
}

/// Update an existing .dirdb by reusing cached hashes where possible.
static DirDbData updateDirDb(const fs::path& dirPath)
{
    DirDbData existing = readDirDb(dirPath, false);
    std::unordered_map<HashReuseKey, FileEntry, HashReuseKeyHasher> cache;
    for (const auto& entry : existing.files)
    {
        HashReuseKey key{entry.inode, entry.size, entry.date};
        cache.emplace(std::move(key), entry);
    }
    return buildDirDb(dirPath, &cache);
}

/// Load, create, or update a .dirdb file depending on flags.
static DirDbData loadOrCreateDirDb(const fs::path& dirPath, bool forceCreate, bool update)
{
    fs::path dbPath = dirPath / ".dirdb";
    if (update)
    {
        if (ut1::fsExists(dbPath))
        {
            return updateDirDb(dirPath);
        }
        return createDirDb(dirPath);
    }
    if (!forceCreate && ut1::fsExists(dbPath))
    {
        return readDirDb(dirPath);
    }
    return createDirDb(dirPath);
}

/// Recursively remove .dirdb files under a root directory.
static void removeDirDbTree(const fs::path& root, bool dryRun)
{
    auto removeIfExists = [](const fs::path& dirPath, bool dryRunFlag)
    {
        fs::path dbPath = dirPath / ".dirdb";
        if (ut1::fsExists(dbPath))
        {
            if (dryRunFlag || clVerbose)
            {
                std::cout << (dryRunFlag ? "Would remove " : "Removed ") << dbPath.string() << "\n";
            }
            if (dryRunFlag)
            {
                return;
            }
            std::error_code ec;
            fs::remove(dbPath, ec);
            if (ec)
            {
                throw std::runtime_error("Failed to remove " + dbPath.string());
            }
        }
    };

    removeIfExists(root, dryRun);

    std::error_code ec;
    fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    while (it != end)
    {
        if (ec)
        {
            if (clVerbose && it != end)
            {
                std::cerr << "Skipping entry due to error: " << it->path() << "\n";
            }
            ec.clear();
            it.increment(ec);
            continue;
        }
        if (ut1::getFileType(*it, false) == ut1::FT_DIR)
        {
            removeIfExists(it->path(), dryRun);
        }
        it.increment(ec);
    }
}

/// Main.
/// Entry point for treeop.
int main(int argc, char *argv[])
{
    // Run unit tests and exit if enabled at compile time.
    UNIT_TEST_RUN();

    // Command line options.
    const char *usage = "Operations on huge directory trees.\n"
                        "\n"
                        "Usage: $programName [OPTIONS] DIR...\n"
                        "\n"
                        "All sizes may be specified with kMGTPE suffixes indicating powers of 1024.";
    ut1::CommandLineParser cl("treeop", usage,
        "\n$programName version $version *** Copyright (c) 2026 Johannes Overmann *** https://github.com/jovermann/treeop",
        "0.1.1");

    cl.addHeader("\nOptions:\n");
    cl.addOption('i', "intersect", "Determine intersections of two or more dirs. Print unique/shared statistics per dir.");
    cl.addOption('s', "stats", "Print statistics about each dir (number of files and total size etc).");
    cl.addOption('l', "list-files", "List all files with stored meta-data.");
    cl.addOption(' ', "list-a", "List files only in A when used with --intersect.");
    cl.addOption(' ', "list-b", "List files only in B when used with --intersect.");
    cl.addOption(' ', "list-both", "List files in both A and B when used with --intersect.");
    cl.addOption(' ', "extract-a", "Extract files only in A into DIR when used with --intersect.", "DIR", "");
    cl.addOption(' ', "extract-b", "Extract files only in B into DIR when used with --intersect.", "DIR", "");
    cl.addOption(' ', "remove-copies", "Delete files from later roots when content exists in earlier roots (with --intersect).");
    cl.addOption(' ', "same-filename", "Treat files as identical only if content and filename match.");
    cl.addOption(' ', "hardlink-copies", "Replace duplicate files with hardlinks to the oldest file.");
    cl.addOption(' ', "readbench", "Read all files to measure filesystem read performance.");
    cl.addOption(' ', "bufsize", "Buffer size for reading (readbench and hashing).", "N", "1M");
    cl.addOption(' ', "min-size", "Minimum file size to hardlink when using --hardlink-copies.", "N", "0");
    cl.addOption(' ', "max-hardlinks", "Maximum allowed hardlink count for the oldest file (with --hardlink-copies).", "N", "60000");
    cl.addOption('d', "dry-run", "Show what would change, but do not modify files.");
    cl.addOption(' ', "new-dirdb", "Force creation of new .dirdb files (overwrite existing).");
    cl.addOption('u', "update-dirdb", "Update .dirdb files, reusing hashes when inode/size/mtime match.");
    cl.addOption(' ', "remove-dirdb", "Recursively remove all .dirdb files under specified dirs.");
    cl.addOption(' ', "get-unique-hash-len", "Calculate the minimum hash length in bits that makes all file contents unique.");
    cl.addOption(' ', "size-histogram", "Print size histogram for all files in all dirs where N in the batch size in bytes.", "N", "0");
    cl.addOption(' ', "max-size", "Maximum file size to include in size histogram.", "N", "0");
    cl.addOption('p', "progress", "Print progress once per second.");
    cl.addOption('W', "width", "Max width for progress line.", "N", "199");
    cl.addOption('v', "verbose", "Increase verbosity. Specify multiple times to be more verbose.");

    // Parse command line options.
    cl.parse(argc, argv);
    clVerbose = cl.getCount("verbose");
    gBufSize = ut1::strToU64(cl.getStr("bufsize"));
    if (gBufSize == 0)
    {
        cl.error("--bufsize must be greater than 0.");
    }
    unsigned progressCount = cl.getCount("progress");
    ProgressTracker progress(cl.getUInt("width"), progressCount > 1);
    if (progressCount > 0)
    {
        gProgress = &progress;
    }

    // Implicit options.
    if (!(cl("list-files") || cl("size-histogram") || cl("remove-dirdb") || cl("intersect") || cl("list-a") || cl("list-b") || cl("list-both") || cl("extract-a") || cl("extract-b") || cl("remove-copies") || cl("hardlink-copies") || cl("readbench") || cl("get-unique-hash-len")))
    {
        cl.setOption("stats");
    }

    try
    {
        if (cl.getArgs().empty())
        {
            cl.error("Please specify at least one directory.");
        }

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

        if (cl("new-dirdb") && cl("update-dirdb"))
        {
            cl.error("Cannot combine --new-dirdb with --update-dirdb.");
        }
        if ((cl("list-a") || cl("list-b") || cl("list-both")) && !cl("intersect"))
        {
            cl.error("--list-a/--list-b/--list-both require --intersect.");
        }
        if ((cl("extract-a") || cl("extract-b")) && !cl("intersect"))
        {
            cl.error("--extract-a/--extract-b require --intersect.");
        }
        if (cl("remove-copies") && !cl("intersect"))
        {
            cl.error("--remove-copies requires --intersect.");
        }
        if (cl("dry-run") && !(cl("remove-copies") || cl("extract-a") || cl("extract-b") || cl("remove-dirdb") || cl("hardlink-copies")))
        {
            cl.error("--dry-run requires --remove-copies, --extract-a/--extract-b, --remove-dirdb, or --hardlink-copies.");
        }

        if (cl("remove-dirdb"))
        {
            for (const std::string& path : cl.getArgs())
            {
                removeDirDbTree(normalizePath(path), cl("dry-run"));
            }
        }
        else
        {
            std::vector<fs::path> normalizedRoots;
            for (const std::string& path : cl.getArgs())
            {
                normalizedRoots.push_back(normalizePath(path));
            }
            if (cl("readbench"))
            {
                bool otherOps = cl("stats") || cl("list-files") || cl("size-histogram") || cl("remove-dirdb")
                    || cl("intersect") || cl("update-dirdb") || cl("list-a") || cl("list-b") || cl("list-both")
                    || cl("extract-a") || cl("extract-b") || cl("remove-copies") || cl("hardlink-copies")
                    || cl("get-unique-hash-len") || cl("new-dirdb");
                if (otherOps)
                {
                    cl.error("--readbench cannot be combined with other operations.");
                }

                ReadBenchStats stats = runReadBench(normalizedRoots);
                if (gProgress)
                {
                    gProgress->finish();
                }
                double rate = (stats.elapsed > 0.0) ? (double(stats.bytes) / stats.elapsed) : 0.0;
                std::cout << "total-files: " << stats.files << "\n";
                std::cout << "total-dirs: " << stats.dirs << "\n";
                std::cout << "total-size: " << ut1::getApproxSizeStr(stats.bytes, 3, true, false) << "\n";
                std::cout << "bufsize: " << ut1::getPreciseSizeStr(static_cast<size_t>(gBufSize)) << "\n";
                std::cout << "read-rate: " << ut1::getApproxSizeStr(rate, 1, true, true) << "/s\n";
                std::cout << "elapsed: " << ut1::secondsToString(stats.elapsed) << "\n";
                return 0;
            }

            MainDb mainDb(normalizedRoots, cl("same-filename"));

            // Recursively walk all dirs specified on the command line and either read existing .dirdb files or create missing .dirdb files.
            mainDb.processRoots(cl("new-dirdb"), cl("update-dirdb"));
            if (gProgress)
            {
                gProgress->finish();
            }

            if (cl("hardlink-copies"))
            {
                uint64_t minSize = ut1::strToU64(cl.getStr("min-size"));
                uint64_t maxHardlinks = ut1::strToU64(cl.getStr("max-hardlinks"));
                auto stats = mainDb.hardlinkCopies(minSize, maxHardlinks, cl("dry-run"));
                std::cout << "hardlink-copies:\n";
                mainDb.printHardlinkStats(stats);
            }

            if (cl("intersect"))
            {
                if (normalizedRoots.size() < 2)
                {
                    cl.error("--intersect requires at least two directories.");
                }
                if ((cl("list-a") || cl("list-b") || cl("list-both") || cl("extract-a") || cl("extract-b")) && normalizedRoots.size() != 2)
                {
                    cl.error("--list-a/--list-b/--list-both/--extract-a/--extract-b require exactly two directories.");
                }
                std::optional<fs::path> extractA;
                std::optional<fs::path> extractB;
                if (cl("extract-a"))
                {
                    extractA = normalizePath(cl.getStr("extract-a"));
                }
                if (cl("extract-b"))
                {
                    extractB = normalizePath(cl.getStr("extract-b"));
                }
                mainDb.printIntersectStats(
                    normalizedRoots,
                    cl("list-a"),
                    cl("list-b"),
                    cl("list-both"),
                    extractA ? &*extractA : nullptr,
                    extractB ? &*extractB : nullptr,
                    cl("remove-copies"),
                    cl("dry-run"));
            }
            else
            {
                if (cl("stats"))
                {
                    mainDb.printStats();
                }

                if (cl("size-histogram"))
                {
                    uint64_t batchSize = ut1::strToU64(cl.getStr("size-histogram"));
                    uint64_t maxSize = ut1::strToU64(cl.getStr("max-size"));
                    bool hasMaxSize = cl.getStr("max-size") != "0";
                    mainDb.printSizeHistogram(batchSize, maxSize, hasMaxSize);
                }

                if (cl("list-files"))
                {
                    mainDb.listFiles();
                }
                if (cl("get-unique-hash-len"))
                {
                    mainDb.printUniqueHashLen();
                }
            }
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
