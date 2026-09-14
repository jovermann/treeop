// Interactive directory-tree grooming tool.

#include "RemoveInteractive.hpp"
#include "MiscUtils.hpp"
#include "Tui.hpp"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cctype>
#include <ctime>
#include <fstream>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <spawn.h>
#include <system_error>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace fs = std::filesystem;

namespace treeop
{
namespace
{

constexpr std::string_view trashName = ".treeop_trash";

struct Node
{
    size_t rootIndex{};
    fs::path path;
    fs::path relativePath;
    std::string name;
    uint64_t size{};
    fs::file_time_type date{};
    bool directory{};
    bool regular{};
    bool symlink{};
    bool linkOk{};
    std::string linkTarget;
    bool root{};
    bool expanded{};
    size_t displayDepth{};
    std::vector<Node> children;
};

struct TrashRecord
{
    size_t rootIndex{};
    fs::path entryDir;
    fs::path payload;
    fs::path originalRelative;
    uint64_t size{};
    fs::file_time_type date{};
    bool directory{};
    std::string id;
    bool symlink{};
    bool linkOk{};
    std::string linkTarget;
};

enum class ViewMode { Tree, Undo, Trash };
enum class SortMode { Name, Size, Date };

std::string safe(const fs::path& path)
{
    return ut1::escapeTerminalText(path.string());
}

std::string formatDate(fs::file_time_type time)
{
    auto systemTime = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        decltype(time)::clock::to_sys(time));
    std::time_t value = std::chrono::system_clock::to_time_t(systemTime);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &value);
#else
    localtime_r(&value, &tm);
#endif
    char buffer[20]{};
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M", &tm);
    return buffer;
}

uint64_t addSaturating(uint64_t a, uint64_t b)
{
    return UINT64_MAX - a < b ? UINT64_MAX : a + b;
}

bool pathWithin(const fs::path& root, const fs::path& path)
{
    auto rootIt = root.begin();
    auto pathIt = path.begin();
    for (; rootIt != root.end() && pathIt != path.end(); ++rootIt, ++pathIt)
    {
        if (*rootIt != *pathIt) return false;
    }
    return rootIt == root.end();
}

class Explorer
{
public:
    explicit Explorer(std::vector<fs::path> roots_)
        : roots(std::move(roots_))
    {
        refresh();
    }

    void run()
    {
        ut1::tui::TerminalRawMode rawMode(STDIN_FILENO, true);
        ut1::tui::enterAlternateScreen();
        struct ScreenGuard
        {
            ~ScreenGuard() { ut1::tui::leaveAlternateScreen(); }
        } screenGuard;

        bool done = false;
        while (!done && !quitRequested)
        {
            reapOpeners();
            rebuildVisible();
            render();
            int key = ut1::tui::readKey();
            if (showDetails)
            {
                if (key == 'i' || key == 27) showDetails = false;
                else if (key == ut1::tui::keyUp && detailScroll) detailScroll--;
                else if (key == ut1::tui::keyDown) detailScroll++;
                else if (key == ut1::tui::keyPageUp) detailScroll = detailScroll > pageSize() ? detailScroll - pageSize() : 0;
                else if (key == ut1::tui::keyPageDown) detailScroll += pageSize();
                else if (key == ut1::tui::keyHome) detailScroll = 0;
                else if (key == ut1::tui::keyEnd) detailScroll = detailLines.size();
                else if (key == 'q' || key == 'Q' || key == 3 || key < 0) done = true;
                continue;
            }
            if (showHelp)
            {
                if (key == '?' || key == 'h' || key == 27) showHelp = false;
                else if (key == ut1::tui::keyUp && helpScroll > 0) helpScroll--;
                else if (key == ut1::tui::keyDown) helpScroll++;
                else if (key == ut1::tui::keyPageUp) helpScroll = helpScroll > pageSize() ? helpScroll - pageSize() : 0;
                else if (key == ut1::tui::keyPageDown) helpScroll += pageSize();
                else if (key == ut1::tui::keyHome) helpScroll = 0;
                else if (key == ut1::tui::keyEnd) helpScroll = helpLines().size();
                else if (key == 'q' || key == 'Q' || key == 3 || key < 0) done = true;
                continue;
            }
            if (confirmEmpty && key != 'E')
            {
                confirmEmpty = false;
                status = "Empty-trash confirmation cancelled";
            }
            switch (key)
            {
            case 'q': case 'Q': case 3: case -1: done = true; break;
            case ut1::tui::keyHome: selected = scroll = 0; break;
            case ut1::tui::keyEnd: selected = itemCount() ? itemCount() - 1 : 0; break;
            case ut1::tui::keyUp: moveSelection(-1); break;
            case ut1::tui::keyDown: moveSelection(1); break;
            case ut1::tui::keyPageUp: moveSelection(-static_cast<int>(pageSize())); break;
            case ut1::tui::keyPageDown: moveSelection(static_cast<int>(pageSize())); break;
            case ut1::tui::keyLeft: collapseOrParent(); break;
            case ut1::tui::keyRight: expandSelected(); break;
            case ' ': toggleSelected(); break;
            case '*': case '\r': case '\n': toggleRecursive(); break;
            case 'i': openDetails(); break;
            case '+': setAllExpanded(true); break;
            case '-': setAllExpanded(false); break;
            case 'd': case 'D': removeSelected(); break;
            case 'u': case 'U': undoSelected(); break;
            case '1': setMode(ViewMode::Tree); break;
            case '2': setMode(ViewMode::Undo); break;
            case '3': setMode(ViewMode::Trash); break;
            case '\t': cycleMode(); break;
            case 's': case 'S': cycleSort(); break;
            case 'o': case 'O': openSelected(); break;
            case 'H': showHidden = !showHidden; status = showHidden ? "Hidden files shown" : "Hidden files hidden (sizes unchanged)"; break;
            case '/': readSearch(); break;
            case 'r': case 'R': refresh(); status = "Refreshed"; break;
            case '?': case 'h': showHelp = true; helpScroll = 0; break;
            case 'E': emptyTrash(); break;
            default: break;
            }
        }
    }

private:
    std::vector<fs::path> roots;
    std::vector<Node> treeNodes;
    std::vector<Node> trashNodes;
    std::vector<TrashRecord> records;
    std::vector<Node*> visible;
    std::vector<size_t> visibleUndo;
    ViewMode mode{ViewMode::Tree};
    SortMode sortMode{SortMode::Name};
    size_t selected{};
    size_t scroll{};
    bool showHidden{true};
    bool quitRequested{};
    bool showHelp{};
    size_t helpScroll{};
    bool showDetails{};
    size_t detailScroll{};
    std::string detailTitle;
    std::vector<std::string> detailLines;
    std::vector<pid_t> openers;
    bool confirmEmpty{};
    std::string search;
    std::string status;
    uint64_t trashBytes{};
    uint64_t idCounter{};

    static bool hidden(const fs::path& path)
    {
        std::string name = path.filename().string();
        return !name.empty() && name[0] == '.';
    }

    Node scanNode(size_t rootIndex, const fs::path& path, const fs::path& relative, bool isRoot)
    {
        Node node;
        node.rootIndex = rootIndex;
        node.path = path;
        node.relativePath = relative;
        node.name = isRoot ? path.string() : path.filename().string();
        node.root = isRoot;
        std::error_code ec;
        fs::file_status fileStatus = fs::symlink_status(path, ec);
        node.directory = !ec && fs::is_directory(fileStatus);
        node.regular = !ec && fs::is_regular_file(fileStatus);
        node.symlink = !ec && fs::is_symlink(fileStatus);
        if (node.symlink)
        {
            node.linkTarget = fs::read_symlink(path, ec).string();
            if (ec) node.linkTarget = "(unreadable target)";
            node.linkOk = fs::exists(fs::status(path, ec));
        }
        node.date = fs::last_write_time(path, ec);
        if (ec)
        {
            node.date = fs::file_time_type{};
            ec.clear();
        }
        if (!node.directory)
        {
            if (fs::is_regular_file(fileStatus))
            {
                node.size = fs::file_size(path, ec);
            }
            return node;
        }

        fs::directory_iterator it(path, fs::directory_options::skip_permission_denied, ec);
        fs::directory_iterator end;
        while (!ec && it != end)
        {
            fs::path childPath = it->path();
            std::string childName = childPath.filename().string();
            if (!(isRoot && childName == trashName))
            {
                Node child = scanNode(rootIndex, childPath, relative / childPath.filename(), false);
                node.size = addSaturating(node.size, child.size);
                node.children.push_back(std::move(child));
            }
            it.increment(ec);
        }
        sortChildren(node);
        return node;
    }

    void sortChildren(Node& node)
    {
        std::sort(node.children.begin(), node.children.end(), [&](const Node& a, const Node& b)
        {
            if (a.directory != b.directory)
            {
                return a.directory > b.directory;
            }
            if (sortMode == SortMode::Size && a.size != b.size)
            {
                return a.size > b.size;
            }
            if (sortMode == SortMode::Date && a.date != b.date)
            {
                return a.date > b.date;
            }
            std::string an = a.name;
            std::string bn = b.name;
            std::transform(an.begin(), an.end(), an.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            std::transform(bn.begin(), bn.end(), bn.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return an < bn;
        });
        for (auto& child : node.children)
        {
            sortChildren(child);
        }
    }

    fs::path entriesDir(size_t rootIndex) const
    {
        return roots[rootIndex] / trashName / "entries";
    }

    bool trashLayoutSafe(size_t rootIndex, bool create, std::error_code& ec) const
    {
        fs::path trash = roots[rootIndex] / trashName;
        fs::path entries = entriesDir(rootIndex);
        ec.clear();
        fs::file_status trashStatus = fs::symlink_status(trash, ec);
        if (ec == std::errc::no_such_file_or_directory) ec.clear();
        if (!ec && fs::exists(trashStatus)
            && (fs::is_symlink(trashStatus) || !fs::is_directory(trashStatus)))
        {
            ec = std::make_error_code(std::errc::not_a_directory);
            return false;
        }
        fs::file_status entriesStatus = fs::symlink_status(entries, ec);
        if (ec == std::errc::no_such_file_or_directory) ec.clear();
        if (!ec && fs::exists(entriesStatus)
            && (fs::is_symlink(entriesStatus) || !fs::is_directory(entriesStatus)))
        {
            ec = std::make_error_code(std::errc::not_a_directory);
            return false;
        }
        if (ec || !create) return !ec;
        fs::create_directories(entries, ec);
        return !ec;
    }

    void loadRecords()
    {
        records.clear();
        trashBytes = 0;
        for (size_t rootIndex = 0; rootIndex < roots.size(); rootIndex++)
        {
            fs::path dir = entriesDir(rootIndex);
            std::error_code ec;
            if (!trashLayoutSafe(rootIndex, false, ec))
            {
                continue;
            }
            fs::directory_iterator it(dir, fs::directory_options::skip_permission_denied, ec);
            fs::directory_iterator end;
            while (!ec && it != end)
            {
                if (it->is_directory(ec))
                {
                    fs::path originalFile = it->path() / "original";
                    fs::path payload = it->path() / "payload";
                    try
                    {
                        fs::path original(ut1::readFile(originalFile.string()));
                        bool safeRelative = !original.empty() && !original.is_absolute();
                        for (const auto& component : original)
                        {
                            if (component == "..") safeRelative = false;
                        }
                        if (safeRelative && ut1::fsExists(payload))
                        {
                            Node payloadNode = scanNode(rootIndex, payload, original, false);
                            records.push_back(TrashRecord{rootIndex, it->path(), payload, original,
                                payloadNode.size, payloadNode.date, payloadNode.directory,
                                it->path().filename().string(), payloadNode.symlink, payloadNode.linkOk, payloadNode.linkTarget});
                            trashBytes = addSaturating(trashBytes, payloadNode.size);
                        }
                    }
                    catch (const std::exception&)
                    {
                        // Ignore incomplete entries; they remain available for manual recovery.
                    }
                }
                it.increment(ec);
            }
        }
        std::sort(records.begin(), records.end(), [](const TrashRecord& a, const TrashRecord& b)
        {
            return a.id > b.id;
        });
    }

    void buildTrashNodes()
    {
        trashNodes.clear();
        for (const auto& record : records)
        {
            Node node = scanNode(record.rootIndex, record.payload, record.originalRelative, false);
            node.name = "[" + roots[record.rootIndex].filename().string() + "] " + record.originalRelative.string();
            node.root = true;
            trashNodes.push_back(std::move(node));
        }
    }

    void refresh()
    {
        std::vector<std::string> expanded;
        collectExpanded(treeNodes, expanded);
        treeNodes.clear();
        for (size_t i = 0; i < roots.size(); i++)
        {
            treeNodes.push_back(scanNode(i, roots[i], {}, true));
            treeNodes.back().expanded = true;
        }
        restoreExpanded(treeNodes, expanded);
        loadRecords();
        buildTrashNodes();
        selected = 0;
        scroll = 0;
    }

    static void collectExpanded(const std::vector<Node>& nodes, std::vector<std::string>& paths)
    {
        for (const auto& node : nodes)
        {
            if (node.expanded)
            {
                paths.push_back(std::to_string(node.rootIndex) + ":" + node.relativePath.string());
            }
            collectExpanded(node.children, paths);
        }
    }

    static void restoreExpanded(std::vector<Node>& nodes, const std::vector<std::string>& paths)
    {
        for (auto& node : nodes)
        {
            std::string key = std::to_string(node.rootIndex) + ":" + node.relativePath.string();
            node.expanded = std::find(paths.begin(), paths.end(), key) != paths.end() || node.root;
            restoreExpanded(node.children, paths);
        }
    }

    bool matchesSearch(const Node& node) const
    {
        if (search.empty())
        {
            return true;
        }
        std::string haystack = node.name;
        std::string needle = search;
        std::transform(haystack.begin(), haystack.end(), haystack.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        std::transform(needle.begin(), needle.end(), needle.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return haystack.find(needle) != std::string::npos;
    }

    void flatten(std::vector<Node>& nodes, size_t depth)
    {
        for (auto& node : nodes)
        {
            if (!showHidden && !node.root && hidden(node.path)) continue;
            node.displayDepth = depth;
            if (matchesSearch(node))
            {
                visible.push_back(&node);
            }
            if (node.directory && node.expanded)
            {
                flatten(node.children, depth + 1);
            }
        }
    }

    void rebuildVisible()
    {
        visible.clear();
        visibleUndo.clear();
        if (mode == ViewMode::Tree)
        {
            flatten(treeNodes, 0);
        }
        else if (mode == ViewMode::Trash)
        {
            flatten(trashNodes, 0);
        }
        else
        {
            for (size_t i = 0; i < records.size(); i++)
            {
                std::string name = records[i].originalRelative.string();
                if (search.empty() || name.find(search) != std::string::npos)
                {
                    visibleUndo.push_back(i);
                }
            }
        }
        size_t count = itemCount();
        if (count == 0)
        {
            selected = scroll = 0;
        }
        else if (selected >= count)
        {
            selected = count - 1;
        }
    }

    size_t itemCount() const
    {
        return mode == ViewMode::Undo ? visibleUndo.size() : visible.size();
    }

    size_t pageSize() const
    {
        size_t height = ut1::tui::terminalHeight();
        size_t reserved = 4;
        return height > reserved ? height - reserved : 1;
    }

    std::string modeName() const
    {
        if (mode == ViewMode::Undo) return "UNDO STACK";
        if (mode == ViewMode::Trash) return "TRASH EXPLORER";
        return "TREE EXPLORER";
    }

    std::string sortName() const
    {
        if (sortMode == SortMode::Size) return "size";
        if (sortMode == SortMode::Date) return "date";
        return "name";
    }

    static const std::vector<std::string>& helpLines()
    {
        static const std::vector<std::string> lines = {
            "Up       Select the previous visible file or directory.",
            "Down     Select the next visible file or directory.",
            "Page Up  Move up by one screen of visible entries.",
            "Page Down Move down by one screen of visible entries.",
            "Home     Jump to the first entry (Pos 1 on German keyboards).",
            "End      Jump to the last entry in the current list.",
            "Space    Toggle expansion of the selected directory; files are unchanged.",
            "Return   Same as *: expand/collapse the directory and all descendants.",
            "Right    Expand the selected directory without collapsing it.",
            "Left     Collapse the directory, or select its visible parent.",
            "*        Expand/collapse the selected directory and all its descendants.",
            "+        Expand every directory in the current tree or trash view.",
            "-        Collapse every directory in the current tree or trash view.",
            "d        Delete: move the selected item to its root's .treeop_trash; roots are protected.",
            "u        Restore the newest removal in tree view, or the selected trash entry.",
            "1        Browse the original command-line directory trees.",
            "2        Browse persistent undo history, newest removal first.",
            "3        Explore trashed items and expand their directory contents.",
            "Tab      Cycle through tree, undo history, and trash explorer views.",
            "s        Cycle name, largest-size-first, and newest-date-first sorting.",
            "o        Open the selected file in its default application (open/xdg-open).",
            "H        Show/hide dotfiles without rescanning; all sizes include hidden files.",
            "i        Show recursive extension statistics, or a safe text/hex file preview.",
            "/        Enter a filename filter; Enter accepts it and Escape clears it.",
            "r        Rescan the filesystem and recalculate sizes, including hidden files.",
            "E        Press twice consecutively to permanently empty trash in ALL roots.",
            "?        Open/close this help; arrow/page keys scroll the help text.",
            "h        Alias for ?: open/close the detailed key reference.",
            "q        Quit without emptying trash; undo history survives program restarts."
        };
        return lines;
    }

    std::string currentPath() const
    {
        if (mode == ViewMode::Undo && selected < visibleUndo.size())
        {
            const auto& record = records[visibleUndo[selected]];
            return safe(roots[record.rootIndex] / record.originalRelative);
        }
        if (selected < visible.size()) return safe(visible[selected]->path);
        return "(no selection)";
    }

    void reapOpeners()
    {
        std::erase_if(openers, [&](pid_t pid)
        {
            int result = 0;
            pid_t waited = waitpid(pid, &result, WNOHANG);
            if (waited == pid && (!WIFEXITED(result) || WEXITSTATUS(result) != 0))
                status = "Default application opener failed";
            return waited == pid || (waited < 0 && errno == ECHILD);
        });
    }

    void openSelected()
    {
        fs::path path;
        if (mode == ViewMode::Undo && selected < visibleUndo.size())
            path = records[visibleUndo[selected]].payload;
        else if (Node* node = selectedNode()) path = node->path;
        else return;
        std::error_code ec;
        if (!fs::is_regular_file(path, ec))
        {
            status = "Select a regular file to open";
            return;
        }
#ifdef __APPLE__
        const char* command = "open";
#elif defined(__linux__)
        const char* command = "xdg-open";
#else
        status = "Opening files is supported on macOS and Linux only";
        return;
#endif
#if defined(__APPLE__) || defined(__linux__)
        std::string argument = fs::absolute(path).string();
        char* args[] = {const_cast<char*>(command), argument.data(), nullptr};
        posix_spawn_file_actions_t actions;
        int error = posix_spawn_file_actions_init(&actions);
        if (!error)
        {
            for (int fd : {STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO})
            {
                error = posix_spawn_file_actions_addopen(&actions, fd, "/dev/null", O_RDWR, 0);
                if (error) break;
            }
            pid_t pid;
            if (!error) error = posix_spawnp(&pid, command, &actions, nullptr, args, environ);
            posix_spawn_file_actions_destroy(&actions);
            if (!error) openers.push_back(pid);
        }
        status = error ? "Cannot launch default application: " + std::error_code(error, std::generic_category()).message()
                       : "Opening " + safe(path);
#endif
    }

    void openDetails()
    {
        std::optional<Node> undoNode;
        Node* node = selectedNode();
        if (mode == ViewMode::Undo && selected < visibleUndo.size())
        {
            const auto& record = records[visibleUndo[selected]];
            undoNode = scanNode(record.rootIndex, record.payload, record.originalRelative, false);
            node = &*undoNode;
        }
        if (!node) return;
        detailLines.clear();
        detailScroll = 0;
        if (node->directory)
        {
            detailTitle = "DIRECTORY STATISTICS";
            struct Total { uint64_t count{}, bytes{}; };
            std::map<std::string, Total> totals;
            Total hiddenTotal, all;
            auto collect = [&](auto&& self, const Node& entry, bool inHidden) -> void
            {
                inHidden = inHidden || hidden(entry.path);
                if (entry.directory)
                {
                    for (const auto& child : entry.children) self(self, child, inHidden);
                }
                else if (entry.regular)
                {
                    std::string extension = entry.path.extension().string();
                    if (extension.empty()) extension = "(no extension)";
                    else if (extension.size() - 1 > 20) extension = "long extensions";
                    else std::transform(extension.begin(), extension.end(), extension.begin(),
                        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                    auto& total = totals[extension];
                    total.count++;
                    total.bytes = addSaturating(total.bytes, entry.size);
                    all.count++;
                    all.bytes = addSaturating(all.bytes, entry.size);
                    if (inHidden)
                    {
                        hiddenTotal.count++;
                        hiddenTotal.bytes = addSaturating(hiddenTotal.bytes, entry.size);
                    }
                }
            };
            collect(collect, *node, hidden(node->path));
            auto summary = [](const std::string& label, const Total& total)
            {
                std::ostringstream line;
                line << "  " << std::left << std::setw(24) << ut1::escapeTerminalText(label)
                     << std::right << std::setw(12) << ut1::formatU64WithUnderscores(total.count) << " files  "
                     << std::setw(16) << ut1::getApproxSizeStr(total.bytes, 2, true, false);
                return line.str();
            };
            detailLines.push_back(" Recursive regular-file totals (all hidden files included; symlinks excluded)");
            detailLines.push_back(summary("TOTAL", all));
            detailLines.push_back(summary("Hidden files (subtotal)", hiddenTotal));
            detailLines.push_back(" Hidden subtotal includes files inside dot-directories; overlaps extension totals.");
            detailLines.push_back("");
            detailLines.push_back(" Extension                       File count                  Total size");
            std::vector<std::pair<std::string, Total>> sorted(totals.begin(), totals.end());
            std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b)
            {
                return a.second.bytes != b.second.bytes ? a.second.bytes > b.second.bytes : a.first < b.first;
            });
            for (const auto& [extension, total] : sorted) detailLines.push_back(summary(extension, total));
        }
        else
        {
            detailTitle = "FILE PREVIEW";
            std::error_code ec;
            if (!fs::is_regular_file(fs::symlink_status(node->path, ec)))
                detailLines.push_back(" Preview unavailable: only regular files are read (no symlinks or special files).");
            else
            {
                std::ifstream input(node->path, std::ios::binary);
                if (!input) detailLines.push_back(" Cannot open file for reading.");
                else
                {
                    constexpr size_t limit = 64 * 1024;
                    std::string bytes(limit + 1, '\0');
                    input.read(bytes.data(), bytes.size());
                    bytes.resize(static_cast<size_t>(input.gcount()));
                    bool truncated = bytes.size() > limit;
                    if (truncated) bytes.resize(limit);
                    size_t controls = 0;
                    bool binary = false;
                    for (unsigned char c : bytes)
                    {
                        if (c == 0) binary = true;
                        if (c < 32 && c != '\n' && c != '\r' && c != '\t' && c != 27) controls++;
                    }
                    binary = binary || (!bytes.empty() && controls * 100 > bytes.size());
                    detailTitle = binary ? "HEX PREVIEW" : "TEXT PREVIEW (SAFE ASCII)";
                    detailLines.push_back(" Showing " + std::to_string(bytes.size()) + " bytes"
                        + (truncated ? " (truncated at 64 KiB)" : ""));
                    detailLines.push_back("");
                    const char* hex = "0123456789abcdef";
                    if (binary)
                    {
                        for (size_t offset = 0; offset < bytes.size(); offset += 16)
                        {
                            std::ostringstream line;
                            line << std::hex << std::setfill('0') << std::setw(8) << offset << "  ";
                            std::string ascii;
                            for (size_t i = 0; i < 16; i++)
                            {
                                if (offset + i < bytes.size())
                                {
                                    unsigned char c = bytes[offset + i];
                                    line << hex[c >> 4] << hex[c & 15] << ' ';
                                    ascii += c >= 32 && c < 127 ? static_cast<char>(c) : '.';
                                }
                                else line << "   ";
                            }
                            line << " |" << ascii << '|';
                            detailLines.push_back(line.str());
                        }
                    }
                    else
                    {
                        std::string line;
                        // Wrap long text lines to keep the entire bounded preview accessible.
                        size_t terminalWidth = ut1::tui::terminalWidth();
                        size_t wrap = std::max<size_t>(4, terminalWidth > 1 ? terminalWidth - 1 : 1);
                        for (unsigned char c : bytes)
                        {
                            if (c == '\n') { detailLines.push_back(line); line.clear(); continue; }
                            std::string token;
                            if (c >= 32 && c < 127) token += static_cast<char>(c);
                            else if (c == '\t') token = "\\t";
                            else if (c == '\r') token = "\\r";
                            else { token = "\\x"; token += hex[c >> 4]; token += hex[c & 15]; }
                            if (line.size() + token.size() > wrap) { detailLines.push_back(line); line.clear(); }
                            line += token;
                        }
                        if (!line.empty()) detailLines.push_back(line);
                    }
                    if (bytes.empty()) detailLines.push_back(" (empty file)");
                    if (input.bad()) detailLines.push_back(" Read error: preview may be incomplete.");
                }
            }
        }
        showDetails = true;
    }

    void render()
    {
        size_t width = ut1::tui::terminalWidth();
        size_t height = pageSize();
        if (selected < scroll) scroll = selected;
        if (selected >= scroll + height) scroll = selected - height + 1;
        ut1::tui::clearScreen();
        std::string title = " treeop explore-interactive  " + (showDetails ? detailTitle : modeName())
            + "  trash: " + ut1::getApproxSizeStr(trashBytes, 2, true, false)
            + "  items: " + ut1::formatU64WithUnderscores(records.size())
            + "  sort: " + sortName();
        if (!search.empty()) title += "  filter: " + ut1::escapeTerminalText(search);
        std::cout << ut1::tui::ansiWhiteOnBlue << ut1::tui::fitTerminalLine(title, width)
                  << ut1::tui::ansiReset << "\n";
        std::cout << ut1::tui::ansiGray
                  << ut1::tui::fitTerminalLine(" Path: " + currentPath(), width)
                  << ut1::tui::ansiReset << "\n";

        const auto& help = helpLines();
        if (showHelp) helpScroll = std::min(helpScroll, help.size() > height ? help.size() - height : 0);
        if (showDetails) detailScroll = std::min(detailScroll, detailLines.size() > height ? detailLines.size() - height : 0);

        for (size_t row = 0; row < height; row++)
        {
            if (showDetails)
            {
                std::string line = detailScroll + row < detailLines.size() ? detailLines[detailScroll + row] : "";
                std::cout << ut1::tui::ansiBrightCyan << ut1::tui::fitTerminalLine(line, width)
                          << ut1::tui::ansiReset << "\n";
                continue;
            }
            if (showHelp)
            {
                std::string line = helpScroll + row < help.size() ? help[helpScroll + row] : "";
                std::cout << ut1::tui::ansiYellow << ut1::tui::fitTerminalLine(line, width)
                          << ut1::tui::ansiReset << "\n";
                continue;
            }
            size_t index = scroll + row;
            if (index >= itemCount())
            {
                std::cout << "\n";
                continue;
            }
            bool active = index == selected;
            std::string line;
            bool directory = false;
            if (mode == ViewMode::Undo)
            {
                const auto& record = records[visibleUndo[index]];
                directory = record.directory;
                line = "  " + std::string(directory ? "▣ " : "• ")
                    + "[" + ut1::escapeTerminalText(roots[record.rootIndex].filename().string()) + "] "
                    + ut1::escapeTerminalText(record.originalRelative.string());
                if (record.symlink) line += " -> " + ut1::escapeTerminalText(record.linkTarget);
                appendColumns(line, record.size, record.date, width, record.symlink, record.linkOk);
            }
            else
            {
                const Node& node = *visible[index];
                directory = node.directory;
                size_t depth = node.displayDepth;
                line = "  " + std::string(depth * 2, ' ');
                if (node.directory)
                {
                    line += node.expanded ? "▾ " : "▸ ";
                }
                else
                {
                    line += "• ";
                }
                line += ut1::escapeTerminalText(node.name);
                if (node.symlink) line += " -> " + ut1::escapeTerminalText(node.linkTarget);
                appendColumns(line, node.size, node.date, width, node.symlink, node.linkOk);
            }
            line = ut1::tui::fitTerminalLine(line, width);
            const char* color = active ? ut1::tui::ansiWhiteOnBlue
                : (directory ? ut1::tui::ansiBrightCyan : ut1::tui::ansiGreen);
            std::cout << color << line << ut1::tui::ansiReset << "\n";
        }
        std::cout << ut1::tui::ansiGray
                  << ut1::tui::fitTerminalLine(" " + ut1::escapeTerminalText(status), width)
                  << ut1::tui::ansiReset << "\n";
        std::cout << ut1::tui::ansiBlackOnCyan
                  << ut1::tui::fitTerminalLine(showDetails
                      ? " Up/Down/PgUp/PgDn: scroll  Home/End: first/last  i/Esc: close  q: quit"
                      : showHelp
                      ? " Up/Down/PgUp/PgDn: scroll help  Home/End: first/last  h/?: close  q: quit"
                      : " Up/Down: select  Space: expand  Return/*: recursive  s: sort  o: open  i: info  d: delete  u: undo  H: hidden  h/?: help  q: quit", width)
                  << ut1::tui::ansiReset << std::flush;
    }

    static void appendColumns(std::string& line, uint64_t size, fs::file_time_type date, size_t width, bool symlink = false, bool linkOk = false)
    {
        std::string suffix = "  " + (symlink ? std::string(linkOk ? "symlink (OK)" : "symlink (broken)") : ut1::getApproxSizeStr(size, 2, true, false)) + "  " + formatDate(date);
        if (width > suffix.size())
        {
            line = ut1::tui::fitTerminalLine(line, width - suffix.size());
            line += std::string(width - ut1::tui::terminalTextWidth(line) - suffix.size(), ' ');
        }
        line += suffix;
    }

    void moveSelection(int delta)
    {
        size_t count = itemCount();
        if (count == 0) return;
        int64_t next = static_cast<int64_t>(selected) + delta;
        next = std::max<int64_t>(0, std::min<int64_t>(next, static_cast<int64_t>(count - 1)));
        selected = static_cast<size_t>(next);
    }

    Node* selectedNode()
    {
        return mode == ViewMode::Undo || visible.empty() ? nullptr : visible[selected];
    }

    void toggleSelected()
    {
        if (Node* node = selectedNode(); node && node->directory)
        {
            node->expanded = !node->expanded;
        }
    }

    void expandSelected()
    {
        if (Node* node = selectedNode(); node && node->directory) node->expanded = true;
    }

    void collapseOrParent()
    {
        Node* node = selectedNode();
        if (!node) return;
        if (node->directory && node->expanded)
        {
            node->expanded = false;
            return;
        }
        size_t depth = node->displayDepth;
        for (size_t i = selected; i-- > 0;)
        {
            if (visible[i]->displayDepth < depth)
            {
                selected = i;
                break;
            }
        }
    }

    static void setRecursive(Node& node, bool value)
    {
        if (node.directory) node.expanded = value;
        for (auto& child : node.children) setRecursive(child, value);
    }

    void toggleRecursive()
    {
        if (Node* node = selectedNode(); node && node->directory)
        {
            setRecursive(*node, !node->expanded);
        }
    }

    void setAllExpanded(bool value)
    {
        auto& nodes = mode == ViewMode::Trash ? trashNodes : treeNodes;
        for (auto& node : nodes) setRecursive(node, value);
    }

    std::string newId()
    {
        auto ticks = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return std::to_string(ticks) + "-" + std::to_string(getpid()) + "-" + std::to_string(idCounter++);
    }

    static void invalidateDirDb(const fs::path& directory)
    {
        if (ut1::isNetworkFilesystem(directory)) return;
        std::error_code ec;
        fs::path dbPath = directory / ".dirdb";
        if (ut1::fsExists(dbPath)) fs::remove(dbPath, ec);
    }

    void removeSelected()
    {
        if (mode != ViewMode::Tree)
        {
            status = "Switch to tree view (1) to remove items";
            return;
        }
        Node* node = selectedNode();
        if (!node || node->root)
        {
            status = "Command-line roots cannot be removed";
            return;
        }
        fs::path entryDir = entriesDir(node->rootIndex) / newId();
        std::error_code ec;
        if (!trashLayoutSafe(node->rootIndex, true, ec))
        {
            status = "Unsafe or unusable trash directory: " + ec.message();
            return;
        }
        fs::create_directories(entryDir, ec);
        if (ec)
        {
            status = "Cannot create trash: " + ec.message();
            return;
        }
        try
        {
            ut1::writeFile((entryDir / "original").string(), node->relativePath.string());
        }
        catch (const std::exception& e)
        {
            fs::remove_all(entryDir, ec);
            status = e.what();
            return;
        }
        // A live edit must not leave an aggregate snapshot claiming old content.
        fs::path snapshot = roots[node->rootIndex] / ".treedb";
        if (node->path != snapshot) fs::remove(snapshot, ec);
        if (ec)
        {
            fs::remove_all(entryDir);
            status = "Cannot invalidate .treedb before deletion: " + ec.message();
            return;
        }
        fs::rename(node->path, entryDir / "payload", ec);
        if (ec)
        {
            fs::remove_all(entryDir, ec);
            status = "Move to trash failed: " + ec.message();
            return;
        }
        if (!node->directory) invalidateDirDb(node->path.parent_path());
        status = "Trashed " + safe(node->relativePath);
        refresh();
    }

    std::optional<size_t> selectedRecordIndex() const
    {
        if (records.empty()) return std::nullopt;
        if (mode == ViewMode::Undo && selected < visibleUndo.size()) return visibleUndo[selected];
        if (mode == ViewMode::Trash && selected < visible.size())
        {
            const Node* node = visible[selected];
            for (size_t i = 0; i < trashNodes.size(); i++)
            {
                const Node* current = node;
                if (current == &trashNodes[i] || (current->rootIndex == trashNodes[i].rootIndex
                    && pathWithin(trashNodes[i].path, current->path))) return i;
            }
            return std::nullopt;
        }
        return mode == ViewMode::Tree ? std::optional<size_t>(0) : std::nullopt;
    }

    void undoSelected()
    {
        auto index = selectedRecordIndex();
        if (!index || *index >= records.size())
        {
            status = "Undo stack is empty";
            return;
        }
        TrashRecord record = records[*index];
        fs::path destination = (roots[record.rootIndex] / record.originalRelative).lexically_normal();
        std::error_code ec;
        fs::path canonicalParent = fs::weakly_canonical(destination.parent_path(), ec);
        if (ec || !pathWithin(roots[record.rootIndex], canonicalParent))
        {
            status = "Cannot restore outside the command-line root";
            return;
        }
        if (fs::exists(destination, ec))
        {
            status = "Cannot restore: destination already exists";
            return;
        }
        fs::create_directories(destination.parent_path(), ec);
        if (ec)
        {
            status = "Cannot create restore directory: " + ec.message();
            return;
        }
        fs::remove(roots[record.rootIndex] / ".treedb", ec);
        if (ec)
        {
            status = "Cannot invalidate .treedb before restore: " + ec.message();
            return;
        }
        fs::rename(record.payload, destination, ec);
        if (ec)
        {
            status = "Restore failed: " + ec.message();
            return;
        }
        if (!record.directory) invalidateDirDb(destination.parent_path());
        fs::remove_all(record.entryDir, ec);
        status = "Restored " + safe(record.originalRelative);
        refresh();
    }

    void emptyTrash()
    {
        if (!confirmEmpty)
        {
            confirmEmpty = true;
            status = "PERMANENTLY empty all trash? Press uppercase E again";
            return;
        }
        uint64_t removed = trashBytes;
        std::error_code ec;
        for (size_t i = 0; i < roots.size(); i++)
        {
            if (!trashLayoutSafe(i, false, ec))
            {
                status = "Unsafe or unusable trash directory: " + ec.message();
                confirmEmpty = false;
                return;
            }
            fs::remove_all(entriesDir(i), ec);
            if (ec)
            {
                status = "Failed to empty trash: " + ec.message();
                confirmEmpty = false;
                return;
            }
        }
        confirmEmpty = false;
        refresh();
        status = "Permanently removed " + ut1::getApproxSizeStr(removed, 2, true, false);
    }

    void setMode(ViewMode value)
    {
        mode = value;
        selected = scroll = 0;
        status = modeName();
    }

    void cycleMode()
    {
        if (mode == ViewMode::Tree) setMode(ViewMode::Undo);
        else if (mode == ViewMode::Undo) setMode(ViewMode::Trash);
        else setMode(ViewMode::Tree);
    }

    void cycleSort()
    {
        if (sortMode == SortMode::Name) sortMode = SortMode::Size;
        else if (sortMode == SortMode::Size) sortMode = SortMode::Date;
        else sortMode = SortMode::Name;
        for (auto& node : treeNodes) sortChildren(node);
        for (auto& node : trashNodes) sortChildren(node);
        status = "Sorted by " + sortName();
    }

    void readSearch()
    {
        std::string input;
        status = "Filter (Enter accepts, Esc clears): ";
        for (;;)
        {
            rebuildVisible();
            render();
            int key = ut1::tui::readKey();
            if (key == 3 || key < 0) { quitRequested = true; return; }
            if (key == '\r' || key == '\n') break;
            if (key == 27)
            {
                input.clear();
                break;
            }
            if ((key == 127 || key == 8) && !input.empty()) input.pop_back();
            else if (key >= 32 && key < 127) input += static_cast<char>(key);
            search = input;
            status = "Filter: " + ut1::escapeTerminalText(input);
        }
        search = input;
        selected = scroll = 0;
        status = search.empty() ? "Filter cleared" : "Filter: " + ut1::escapeTerminalText(search);
    }
};

}

void runRemoveInteractive(const std::vector<fs::path>& roots)
{
    Explorer explorer(roots);
    explorer.run();
}

}
