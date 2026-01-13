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
#include <iostream>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>
#include <unordered_map>
#include <bit>
#include <optional>
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

struct Hash128
{
    uint64_t hi{};
    uint64_t lo{};
    bool operator<(const Hash128& other) const
    {
        if (hi != other.hi)
        {
            return hi < other.hi;
        }
        return lo < other.lo;
    }
    bool operator==(const Hash128& other) const
    {
        return hi == other.hi && lo == other.lo;
    }
    std::string toHex() const
    {
        std::ostringstream os;
        os << std::hex << std::setw(16) << std::setfill('0') << lo
           << std::setw(16) << std::setfill('0') << hi;
        return os.str();
    }
};

class ProgressTracker
{
public:
    explicit ProgressTracker(size_t maxWidth_ = 199, bool linefeed_ = false)
        : startTime(ut1::getTimeSec()), lastPrintTime(startTime), maxWidth(maxWidth_), linefeed(linefeed_) {}

    void onDirStart(const fs::path& dirPath)
    {
        if (!hashing)
        {
            currentDir = dirPath.string();
        }
        tick();
    }

    void onDirDone()
    {
        dirs++;
        tick();
    }

    void addDirSummary(uint64_t fileCount, uint64_t totalBytes)
    {
        dirs++;
        this->files += fileCount;
        this->bytes += totalBytes;
        tick();
    }

    void onFileProcessed(uint64_t size)
    {
        files++;
        bytes += size;
        tick();
    }

    void onHashStart(const fs::path& filePath, uint64_t fileSize)
    {
        hashing = true;
        currentFile = filePath.string();
        currentFileSize = fileSize;
        currentFileDone = 0;
        tick();
    }

    void onHashProgress(uint64_t bytesRead)
    {
        hashedBytes += bytesRead;
        currentFileDone += bytesRead;
        tick();
    }

    void onHashEnd()
    {
        hashing = false;
        currentFile.clear();
        currentFileSize = 0;
        currentFileDone = 0;
        tick();
    }

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

    void printLine(double now)
    {
        double elapsed = now - startTime;
        double rate = (elapsed > 0.0) ? (double(hashedBytes) / elapsed) : 0.0;
        std::string rateStr = formatRateMb(rate);
        std::string sizeStr = formatCompactSize(bytes);
        std::string prefix = "F:" + ut1::toStr(files) +
            " D:" + ut1::toStr(dirs) +
            " B:" + sizeStr +
            " H:" + rateStr;

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
    }

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

    static std::string formatRateMb(double bytesPerSec)
    {
        double mbPerSec = bytesPerSec / (1024.0 * 1024.0);
        std::ostringstream os;
        os << std::fixed << std::setprecision(1) << mbPerSec << "MB/s";
        return os.str();
    }

    static std::string formatCompactSize(uint64_t bytes)
    {
        static constexpr const char* units[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB"};
        double value = static_cast<double>(bytes);
        size_t unitIndex = 0;
        uint64_t whole = bytes;
        while (whole >= 1024 && unitIndex + 1 < std::size(units))
        {
            whole >>= 10;
            value /= 1024.0;
            unitIndex++;
        }
        std::ostringstream os;
        os << std::fixed << std::setprecision(1) << value << " " << units[unitIndex];
        return os.str();
    }

    uint64_t dirs = 0;
    uint64_t files = 0;
    uint64_t bytes = 0;
    uint64_t hashedBytes = 0;
    double startTime = 0.0;
    double lastPrintTime = 0.0;
    std::string currentDir;
    std::string currentFile;
    uint64_t currentFileSize = 0;
    uint64_t currentFileDone = 0;
    bool hashing = false;
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

struct DirDbFileEntry
{
    std::string name;
    FileSize size{};
    Hash128 hash{};
    uint64_t inodeNumber{};
    uint64_t date{};
    uint64_t numLinks{};
};

struct DirDbData
{
    fs::path path;
    std::vector<DirDbFileEntry> files;
    uint64_t dbSize{};
    uint64_t hashedBytes{};
    double hashSeconds{};
};

class MainDb
{
public:
    struct RootData
    {
        fs::path path;
        double elapsedSeconds{};
    };

    explicit MainDb(std::vector<fs::path> rootDirs)
    {
        for (auto& path : rootDirs)
        {
            roots.push_back(RootData{std::move(path), 0.0});
        }
    }

    void addDir(DirDbData dir)
    {
        dirs.push_back(std::move(dir));
    }

    void setRootElapsed(const fs::path& root, double seconds)
    {
        for (auto& data : roots)
        {
            if (data.path == root)
            {
                data.elapsedSeconds = seconds;
                break;
            }
        }
    }

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
                    ContentKey key{file.size, file.hash};
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
                {"total-size:", formatSizeFixed(totalSize), std::string()},
                {"redundant-files:", formatCountInt(redundantFiles), "(" + redundantFilesPct + ")"},
                {"redundant-size:", formatSizeFixed(redundantSize), "(" + redundantSizePct + ")"},
                {"dirdb-size:", formatSizeFixed(totalDbSize), "(" + percentStr + ")"},
                {"dirdb-bytes-per-file:", formatSizeFixed(dirdbBytesPerFile, 1), std::string()}
            };
            if (totalHashedBytes > 0 && totalHashSeconds > 0.0)
            {
                double rateMb = (double(totalHashedBytes) / totalHashSeconds / (1024.0 * 1024.0));
                std::ostringstream rateOs;
                rateOs << std::fixed << std::setprecision(1) << rateMb << " MB/s";
                stats.push_back({"hash-size:", formatSizeFixed(totalHashedBytes), std::string()});
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

    void listFiles() const
    {
        size_t hashLen = getUniqueHashHexLen();
        std::vector<FileRef> refs;
        for (const auto& dir : dirs)
        {
            for (const auto& file : dir.files)
            {
                refs.push_back(FileRef{
                    (dir.path / file.name).string(),
                    file.size,
                    file.hash,
                    file.inodeNumber,
                    file.date,
                    file.numLinks
                });
            }
        }
        printListRows(refs, clVerbose > 1, hashLen);
    }

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
        HistogramUnit unit = getHistogramUnit(batchSize);

        uint64_t maxStart = hasFiles ? (maxSize / batchSize) * batchSize : 0;
        for (uint64_t start = 0; start <= maxStart; start += batchSize)
        {
            std::string startNum = ut1::toStr(start / unit.factor);
            std::string endNum = ut1::toStr((start + batchSize) / unit.factor);
            widthStartNum = std::max(widthStartNum, startNum.size());
            if (showEnd)
            {
                widthEndNum = std::max(widthEndNum, endNum.size());
            }
        }

        std::string unitSuffix = std::string(" ") + unit.label;
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
            std::string totalStr = formatSizeFixed(bucket.totalSize);
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
            std::string startStr = formatHistogramBoundary(start, unit, widthStartNum);
            std::string totalStr = (bucketIndex < bucketTotalStrings.size())
                ? bucketTotalStrings[bucketIndex]
                : formatSizeFixed(bucket.totalSize);
            totalStr = formatSizeAligned(totalStr, totalDecimalPos, totalSuffixWidth);
            if (totalStr.size() < widthTotal)
            {
                totalStr = padRight(totalStr, widthTotal);
            }
            std::string rangeLabel;
            if (showEnd)
            {
                std::string endStr = formatHistogramBoundary(start + batchSize, unit, widthEndNum);
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

    void printIntersectStats(const fs::path& rootA, const fs::path& rootB, bool listA, bool listB, bool listBoth,
        const fs::path* extractA, const fs::path* extractB) const
    {
        std::map<ContentKey, std::vector<FileRef>> filesA;
        std::map<ContentKey, std::vector<FileRef>> filesB;

        for (const auto& dir : dirs)
        {
            if (isPathWithin(rootA, dir.path))
            {
                for (const auto& file : dir.files)
                {
                    ContentKey key{file.size, file.hash};
                    filesA[key].push_back(FileRef{(dir.path / file.name).string(), file.size, file.hash, file.inodeNumber, file.date, file.numLinks});
                }
            }
            if (isPathWithin(rootB, dir.path))
            {
                for (const auto& file : dir.files)
                {
                    ContentKey key{file.size, file.hash};
                    filesB[key].push_back(FileRef{(dir.path / file.name).string(), file.size, file.hash, file.inodeNumber, file.date, file.numLinks});
                }
            }
        }

        struct BucketStats
        {
            uint64_t files{};
            uint64_t bytes{};
        };
        BucketStats onlyA;
        BucketStats bothA;
        BucketStats bothB;
        BucketStats onlyB;

        for (const auto& [key, listARefs] : filesA)
        {
            uint64_t countA = listARefs.size();
            uint64_t countB = 0;
            auto itB = filesB.find(key);
            if (itB != filesB.end())
            {
                countB = itB->second.size();
            }
            if (countB > 0)
            {
                bothA.files += countA;
                bothA.bytes += countA * key.size;
            }
            else if (countA > 0)
            {
                onlyA.files += countA;
                onlyA.bytes += countA * key.size;
            }
        }

        for (const auto& [key, listBRefs] : filesB)
        {
            uint64_t countB = listBRefs.size();
            uint64_t countA = 0;
            auto itA = filesA.find(key);
            if (itA != filesA.end())
            {
                countA = itA->second.size();
            }
            if (countA > 0)
            {
                bothB.files += countB;
                bothB.bytes += countB * key.size;
            }
            else if (countB > 0)
            {
                onlyB.files += countB;
                onlyB.bytes += countB * key.size;
            }
        }

        uint64_t totalFilesA = onlyA.files + bothA.files;
        uint64_t totalBytesA = onlyA.bytes + bothA.bytes;
        uint64_t totalFilesB = onlyB.files + bothB.files;
        uint64_t totalBytesB = onlyB.bytes + bothB.bytes;

        std::string onlyAFilesStr = formatCountInt(onlyA.files);
        std::string onlyASizeStr = formatSizeFixed(onlyA.bytes);
        std::string bothAFilesStr = formatCountInt(bothA.files);
        std::string bothASizeStr = formatSizeFixed(bothA.bytes);
        std::string bothBFilesStr = formatCountInt(bothB.files);
        std::string bothBSizeStr = formatSizeFixed(bothB.bytes);
        std::string onlyBFilesStr = formatCountInt(onlyB.files);
        std::string onlyBSizeStr = formatSizeFixed(onlyB.bytes);

        std::string onlyAFilesPct = formatPercentFixed(totalFilesA == 0 ? 0.0 : (100.0 * onlyA.files / totalFilesA));
        std::string onlyASizePct = formatPercentFixed(totalBytesA == 0 ? 0.0 : (100.0 * onlyA.bytes / totalBytesA));
        std::string bothAFilesPct = formatPercentFixed(totalFilesA == 0 ? 0.0 : (100.0 * bothA.files / totalFilesA));
        std::string bothASizePct = formatPercentFixed(totalBytesA == 0 ? 0.0 : (100.0 * bothA.bytes / totalBytesA));
        std::string bothBFilesPct = formatPercentFixed(totalFilesB == 0 ? 0.0 : (100.0 * bothB.files / totalFilesB));
        std::string bothBSizePct = formatPercentFixed(totalBytesB == 0 ? 0.0 : (100.0 * bothB.bytes / totalBytesB));
        std::string onlyBFilesPct = formatPercentFixed(totalFilesB == 0 ? 0.0 : (100.0 * onlyB.files / totalFilesB));
        std::string onlyBSizePct = formatPercentFixed(totalBytesB == 0 ? 0.0 : (100.0 * onlyB.bytes / totalBytesB));

        if (extractA)
        {
            copyIntersectFiles(rootA, *extractA, filesA, filesB);
        }
        if (extractB)
        {
            copyIntersectFiles(rootB, *extractB, filesB, filesA);
        }

        std::vector<StatLine> stats = {
            {"only-A-files:", onlyAFilesStr, "(" + onlyAFilesPct + " of A)"},
            {"only-A-size:", onlyASizeStr, "(" + onlyASizePct + " of A)"},
            {"both-A-files:", bothAFilesStr, "(" + bothAFilesPct + " of A)"},
            {"both-A-size:", bothASizeStr, "(" + bothASizePct + " of A)"},
            {"both-B-files:", bothBFilesStr, "(" + bothBFilesPct + " of B)"},
            {"both-B-size:", bothBSizeStr, "(" + bothBSizePct + " of B)"},
            {"only-B-files:", onlyBFilesStr, "(" + onlyBFilesPct + " of B)"},
            {"only-B-size:", onlyBSizeStr, "(" + onlyBSizePct + " of B)"}
        };

        std::cout << "A: " << rootA.string() << "\n"
                  << "B: " << rootB.string() << "\n";
        printStatList(stats);

        size_t hashLen = 0;
        if (clVerbose > 0 && (listA || listB || listBoth))
        {
            hashLen = getUniqueHashHexLen();
        }

        if (listA)
        {
            std::cout << "only-in-A:\n";
            if (clVerbose > 0)
            {
                std::vector<FileRef> refs;
                for (const auto& [key, listARefs] : filesA)
                {
                    if (filesB.find(key) != filesB.end())
                    {
                        continue;
                    }
                    refs.insert(refs.end(), listARefs.begin(), listARefs.end());
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listARefs] : filesA)
                {
                    if (filesB.find(key) != filesB.end())
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

        if (listB)
        {
            std::cout << "only-in-B:\n";
            if (clVerbose > 0)
            {
                std::vector<FileRef> refs;
                for (const auto& [key, listBRefs] : filesB)
                {
                    if (filesA.find(key) != filesA.end())
                    {
                        continue;
                    }
                    refs.insert(refs.end(), listBRefs.begin(), listBRefs.end());
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listBRefs] : filesB)
                {
                    if (filesA.find(key) != filesA.end())
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

        if (listBoth)
        {
            std::cout << "in-both:\n";
            if (clVerbose > 0)
            {
                std::vector<FileRef> refs;
                for (const auto& [key, listARefs] : filesA)
                {
                    auto itB = filesB.find(key);
                    if (itB == filesB.end())
                    {
                        continue;
                    }
                    for (const auto& ref : listARefs)
                    {
                        FileRef labeled = ref;
                        labeled.path = "A: " + labeled.path;
                        refs.push_back(std::move(labeled));
                    }
                    for (const auto& ref : itB->second)
                    {
                        FileRef labeled = ref;
                        labeled.path = "B: " + labeled.path;
                        refs.push_back(std::move(labeled));
                    }
                }
                printListRows(refs, clVerbose > 1, hashLen);
            }
            else
            {
                for (const auto& [key, listARefs] : filesA)
                {
                    auto itB = filesB.find(key);
                    if (itB == filesB.end())
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

private:
    struct FileRef
    {
        std::string path;
        uint64_t size{};
        Hash128 hash{};
        uint64_t inode{};
        uint64_t date{};
        uint64_t numLinks{};
    };

    struct ContentKey
    {
        uint64_t size{};
        Hash128 hash{};
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

    static void printListRows(const std::vector<FileRef>& refs, bool showInodeLinks, size_t hashLen)
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

    static void copyIntersectFiles(const fs::path& rootSrc, const fs::path& destRoot,
        const std::map<ContentKey, std::vector<FileRef>>& filesSrc,
        const std::map<ContentKey, std::vector<FileRef>>& filesOther)
    {
        if (fs::exists(destRoot))
        {
            throw std::runtime_error("Destination exists: " + destRoot.string());
        }
        fs::create_directories(destRoot);

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
                fs::create_directories(destPath.parent_path());
                fs::copy_file(srcPath, destPath, fs::copy_options::none, ec);
                if (ec)
                {
                    throw std::runtime_error("Failed to copy to " + destPath.string());
                }
            }
        }
    }

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

    static std::string formatSizeFixed(uint64_t bytes, unsigned precision = 3)
    {
        static constexpr const char* units[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB"};
        if (bytes == 0)
        {
            return "0";
        }
        double value = static_cast<double>(bytes);
        size_t unitIndex = 0;
        uint64_t whole = bytes;
        while (whole >= 1024 && unitIndex + 1 < std::size(units))
        {
            whole >>= 10;
            value /= 1024.0;
            unitIndex++;
        }
        std::ostringstream os;
        if (unitIndex == 0)
        {
            os << bytes << " " << units[unitIndex];
        }
        else
        {
            os << std::fixed << std::setprecision(precision) << value << " " << units[unitIndex];
        }
        return os.str();
    }

    static std::string formatSizeFixed(double bytes, unsigned precision = 3)
    {
        if (bytes <= 0.0)
        {
            return "0";
        }
        uint64_t whole = static_cast<uint64_t>(bytes);
        static constexpr const char* units[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB"};
        double value = bytes;
        size_t unitIndex = 0;
        while (whole >= 1024 && unitIndex + 1 < std::size(units))
        {
            whole >>= 10;
            value /= 1024.0;
            unitIndex++;
        }
        std::ostringstream os;
        os << std::fixed << std::setprecision(precision) << value << " " << units[unitIndex];
        return os.str();
    }

    static std::string formatPercentFixed(double percent)
    {
        std::ostringstream os;
        os << std::fixed << std::setprecision(1) << percent << "%";
        return os.str();
    }

    static std::string formatCountInt(uint64_t count)
    {
        return ut1::toStr(count);
    }

    static size_t getDecimalPos(const std::string& value)
    {
        size_t pos = value.find('.');
        if (pos == std::string::npos)
        {
            return value.size();
        }
        return pos;
    }

    static size_t getStatDecimalPos(const std::string& value)
    {
        size_t end = value.find(' ');
        std::string_view number = (end == std::string::npos)
            ? std::string_view(value)
            : std::string_view(value).substr(0, end);
        size_t pos = number.find('.');
        return (pos == std::string::npos) ? number.size() : pos;
    }

    static std::string formatAlignedStatValue(const std::string& value, size_t labelWidth, size_t decimalCol)
    {
        size_t decimalPos = getStatDecimalPos(value);
        size_t currentCol = labelWidth + 1 + decimalPos;
        size_t padding = (decimalCol > currentCol) ? (decimalCol - currentCol) : 0;
        return std::string(padding, ' ') + value;
    }

    static std::string padRight(const std::string& value, size_t width)
    {
        if (value.size() >= width)
        {
            return value;
        }
        return value + std::string(width - value.size(), ' ');
    }

    static std::string padLeft(const std::string& value, size_t width)
    {
        if (value.size() >= width)
        {
            return value;
        }
        return std::string(width - value.size(), ' ') + value;
    }

    static std::string alignDecimalTo(const std::string& value, size_t decimalPos)
    {
        size_t pos = getDecimalPos(value);
        if (pos >= decimalPos)
        {
            return value;
        }
        return std::string(decimalPos - pos, ' ') + value;
    }

    static std::pair<std::string, std::string> splitSizeStr(const std::string& value)
    {
        size_t sep = value.rfind(' ');
        if (sep == std::string::npos)
        {
            return {value, std::string()};
        }
        return {value.substr(0, sep), value.substr(sep + 1)};
    }

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

    struct HistogramUnit
    {
        uint64_t factor;
        const char* label;
    };

    static HistogramUnit getHistogramUnit(uint64_t batchSize)
    {
        static constexpr HistogramUnit units[] = {
            {1ULL, "bytes"},
            {1024ULL, "kB"},
            {1024ULL * 1024ULL, "MB"},
            {1024ULL * 1024ULL * 1024ULL, "GB"},
            {1024ULL * 1024ULL * 1024ULL * 1024ULL, "TB"},
            {1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL, "PB"},
            {1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL, "EB"}
        };
        size_t index = 0;
        uint64_t size = batchSize;
        while (size >= 1024 && index + 1 < std::size(units))
        {
            size >>= 10;
            index++;
        }
        return units[index];
    }

    static std::string formatHistogramBoundary(uint64_t value, const HistogramUnit& unit, size_t numberWidth)
    {
        std::string number = ut1::toStr(value / unit.factor);
        return padLeft(number, numberWidth) + " " + unit.label;
    }

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
        return os.str();
    }

    std::vector<RootData> roots;
    std::vector<DirDbData> dirs;
};

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

static uint64_t makeTag(const char* tag)
{
    uint64_t value = 0;
    for (size_t i = 0; i < 8 && tag[i]; i++)
    {
        value |= (static_cast<uint64_t>(static_cast<unsigned char>(tag[i])) << (8 * i));
    }
    return value;
}

static void appendU64Le(std::vector<uint8_t>& out, uint64_t value)
{
    for (size_t i = 0; i < 8; i++)
    {
        out.push_back(static_cast<uint8_t>(value & 0xff));
        value >>= 8;
    }
}

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
    std::vector<uint8_t> buffer(1024 * 1024);
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
    uint64_t lo = 0;
    uint64_t hi = 0;
    for (size_t i = 0; i < 8; i++)
    {
        lo |= (static_cast<uint64_t>(digest[i]) << (8 * i));
        hi |= (static_cast<uint64_t>(digest[8 + i]) << (8 * i));
    }
    return Hash128{hi, lo};
}

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
        uint64_t inodeNumber;
        uint64_t date;
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
        entry.inodeNumber = readU64Le(data, size, pos, "inodeNumber");
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
        DirDbFileEntry entry;
        entry.name = readLengthStringAt(strings, static_cast<size_t>(rawEntry.nameIndex));
        entry.size = sizes[i];
        entry.hash = rawEntry.hash;
        entry.inodeNumber = rawEntry.inodeNumber;
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
    uint64_t date{};

    bool operator==(const HashReuseKey& other) const
    {
        return inode == other.inode && size == other.size && date == other.date;
    }
};

struct HashReuseKeyHasher
{
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

static DirDbData buildDirDb(const fs::path& dirPath, const std::unordered_map<HashReuseKey, DirDbFileEntry, HashReuseKeyHasher>* cache)
{
    if (clVerbose > 0)
    {
        std::cout << "Scanning " << dirPath.string() << "\n";
    }
    if (gProgress)
    {
        gProgress->onDirStart(dirPath);
    }

    struct ScanEntry
    {
        std::string name;
        FileSize size;
        Hash128 hash;
        uint64_t inodeNumber;
        uint64_t date;
        uint64_t numLinks;
    };

    std::vector<ScanEntry> entries;
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
        ScanEntry scan;
        scan.name = entry.path().filename().string();
        scan.size = size;
        scan.hash = hash;
        scan.inodeNumber = static_cast<uint64_t>(statInfo.getIno());
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

    std::sort(entries.begin(), entries.end(), [](const ScanEntry& a, const ScanEntry& b)
    {
        if (a.size != b.size)
        {
            return a.size < b.size;
        }
        return a.name < b.name;
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
        uint64_t inodeNumber;
        uint64_t date;
        uint64_t numLinks;
    };
    std::vector<RawFileEntry> rawEntries;
    for (const auto& entry : entries)
    {
        RawFileEntry raw{};
        raw.nameIndex = stringData.size();
        appendLengthString(stringData, entry.name);
        raw.hash = entry.hash;
        raw.inodeNumber = entry.inodeNumber;
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
        appendU64Le(out, raw.inodeNumber);
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
        DirDbFileEntry file;
        file.name = entry.name;
        file.size = entry.size;
        file.hash = entry.hash;
        file.inodeNumber = entry.inodeNumber;
        file.date = entry.date;
        file.numLinks = entry.numLinks;
        dirData.files.push_back(std::move(file));
    }
    return dirData;
}

static DirDbData createDirDb(const fs::path& dirPath)
{
    return buildDirDb(dirPath, nullptr);
}

static DirDbData updateDirDb(const fs::path& dirPath)
{
    DirDbData existing = readDirDb(dirPath, false);
    std::unordered_map<HashReuseKey, DirDbFileEntry, HashReuseKeyHasher> cache;
    for (const auto& entry : existing.files)
    {
        HashReuseKey key{entry.inodeNumber, entry.size, entry.date};
        cache.emplace(std::move(key), entry);
    }
    return buildDirDb(dirPath, &cache);
}

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

static void processDirTree(const fs::path& root, MainDb& db, bool forceCreate, bool update)
{
    db.addDir(loadOrCreateDirDb(root, forceCreate, update));

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
            db.addDir(loadOrCreateDirDb(it->path(), forceCreate, update));
        }
        it.increment(ec);
    }
}

static void removeDirDbTree(const fs::path& root)
{
    auto removeIfExists = [](const fs::path& dirPath)
    {
        fs::path dbPath = dirPath / ".dirdb";
        if (ut1::fsExists(dbPath))
        {
            std::error_code ec;
            fs::remove(dbPath, ec);
            if (ec)
            {
                throw std::runtime_error("Failed to remove " + dbPath.string());
            }
            if (clVerbose)
            {
                std::cout << "Removed " << dbPath.string() << "\n";
            }
        }
    };

    removeIfExists(root);

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
            removeIfExists(it->path());
        }
        it.increment(ec);
    }
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
        "0.1.1");

    cl.addHeader("\nOptions:\n");
    cl.addOption('i', "intersect", "Determine the intersection of dir A and dir B. Exactly two dirs must be specified when this option is specified. Print statistics (bytes/files) for files contained only in A, in both A and B or only in B.");
    cl.addOption('s', "stats", "Print statistics about each dir (number of files and total size etc).");
    cl.addOption('l', "list-files", "List all files with stored meta-data.");
    cl.addOption(' ', "list-a", "List files only in A when used with --intersect.");
    cl.addOption(' ', "list-b", "List files only in B when used with --intersect.");
    cl.addOption(' ', "list-both", "List files in both A and B when used with --intersect.");
    cl.addOption(' ', "extract-a", "Extract files only in A into DIR when used with --intersect.", "DIR", "");
    cl.addOption(' ', "extract-b", "Extract files only in B into DIR when used with --intersect.", "DIR", "");
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
    unsigned progressCount = cl.getCount("progress");
    ProgressTracker progress(cl.getUInt("width"), progressCount > 1);
    if (progressCount > 0)
    {
        gProgress = &progress;
    }

    // Implicit options.
    if (!(cl("stats") || cl("list-files") || cl("size-histogram") || cl("remove-dirdb") || cl("intersect") || cl("update-dirdb") || cl("list-a") || cl("list-b") || cl("list-both") || cl("extract-a") || cl("extract-b") || cl("get-unique-hash-len")))
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

        if (cl("remove-dirdb"))
        {
            for (const std::string& path : cl.getArgs())
            {
                removeDirDbTree(normalizePath(path));
            }
        }
        else
        {
            std::vector<fs::path> normalizedRoots;
            for (const std::string& path : cl.getArgs())
            {
                normalizedRoots.push_back(normalizePath(path));
            }
            MainDb mainDb(normalizedRoots);

            // Recursively walk all dirs specified on the command line and either read existing .dirdb files or create missing .dirdb files.
            for (size_t i = 0; i < cl.getArgs().size(); i++)
            {
                double start = ut1::getTimeSec();
                processDirTree(normalizedRoots[i], mainDb, cl("new-dirdb"), cl("update-dirdb"));
                double end = ut1::getTimeSec();
                mainDb.setRootElapsed(normalizedRoots[i], end - start);
            }
            if (gProgress)
            {
                gProgress->finish();
            }

            if (cl("intersect"))
            {
                if (normalizedRoots.size() != 2)
                {
                    cl.error("--intersect requires exactly two directories.");
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
                    normalizedRoots[0],
                    normalizedRoots[1],
                    cl("list-a"),
                    cl("list-b"),
                    cl("list-both"),
                    extractA ? &*extractA : nullptr,
                    extractB ? &*extractB : nullptr);
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
