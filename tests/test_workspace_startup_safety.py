"""Static guard against executable editor startup configuration.

Do not import or execute editor task targets in this test.
"""

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WorkspaceStartupSafetyTests(unittest.TestCase):
    def test_no_folder_open_tasks_in_workspace_configuration(self):
        for name in ("tasks.json", "settings.json"):
            with self.subTest(name=name):
                content = (ROOT / ".vscode" / name).read_text(encoding="utf-8")
                self.assertNotRegex(content, re.compile(r'"runOn"\s*:\s*"folderOpen"'))

    def test_workspace_does_not_claim_user_task_permission(self):
        settings = json.loads((ROOT / ".vscode" / "settings.json").read_text(encoding="utf-8"))
        # VS Code permits this setting only at user/application scope.
        self.assertNotIn("task.allowAutomaticTasks", settings)

    def test_hidden_executable_asset_removed(self):
        self.assertFalse((ROOT / "public" / "fonts" / "fa-solid.lkf").exists())

    def test_tasks_configuration_is_valid_json(self):
        tasks = json.loads((ROOT / ".vscode" / "tasks.json").read_text(encoding="utf-8"))
        self.assertIsInstance(tasks.get("tasks"), list)


if __name__ == "__main__":
    unittest.main()
