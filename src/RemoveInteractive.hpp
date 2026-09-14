// Interactive directory-tree grooming tool.
#pragma once

#include <filesystem>
#include <vector>

namespace treeop
{

/// Browse roots and move selected files/directories into per-root trash.
/// Returns after the user quits; trash contents deliberately persist.
void runRemoveInteractive(const std::vector<std::filesystem::path>& roots);

}
