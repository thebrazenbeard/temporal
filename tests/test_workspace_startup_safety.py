"""Source-only regression guard for editor startup and static font assets.

VS Code's task.allowAutomaticTasks is application-scoped: a workspace value
("off" included) is not evidence that a user's editor will block autorun.
This test enforces only the tracked repository's absent runnable tasks and
unchanged known-good font files; it does not inspect an installed editor.
"""

import hashlib
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]

# The 15 pre-existing font assets are byte-pinned. In particular, a fake TTF
# with a plausible SFNT header but appended script must never pass on magic alone.
KNOWN_FONTS = {
    "fa-brands-400.eot": "811e1577184066108ceca86d5a624a9012303df5206c5cb0b27d5e3fedcba5a3",
    "fa-brands-400.svg": "02340ba1789d09bcc507563c89c4a847ba537bb8132082777a50519cf74bcb5b",
    "fa-brands-400.ttf": "a0c145382acfe59767d1487d6505f349665b685c2885cd91b621d141a292b0f8",
    "fa-brands-400.woff": "e836c5d5dfc13e18170c5bddefb4fdac7c65ac0ad43a4b431acdc4a98865a260",
    "fa-brands-400.woff2": "5d9190292acdd48ba0fc35080f7e7448f3cdf0d79199a4d23f0f49b5341fdf29",
    "fa-regular-400.eot": "01f4416f5db59e2dd6b6fbd9dc32336d99db18f7eb623a49f584d04afd279473",
    "fa-regular-400.svg": "865fd23de13b1920280e182359579e09eff7a09eb24676eceec7552838559f55",
    "fa-regular-400.ttf": "f32b94a29060dfec0c797a1dfe111357eb1d682f1aca4ac4b95cf0f4c3e2bfa9",
    "fa-regular-400.woff": "8e2d96198711b63752e5f2f5987b8ffc536df6156a065bdbe28cb7231cad793d",
    "fa-regular-400.woff2": "6a8c8e9e1e7f692c21af1956de163f3d026778e6449fe93a09a671847ca1ae65",
    "fa-solid-900.eot": "32501727bb23fc77615b1ec76b5f298ec22198c0f3d6a3e7d6fe4ac3cf315db9",
    "fa-solid-900.svg": "e768539808757f7eec20f09c9ee9b36b0e6c5636e90c9ee697995cbbc6ee0555",
    "fa-solid-900.ttf": "231d68cefbc6846fb78cafca8467401a4f371e226418662e8900e3d52adc00ab",
    "fa-solid-900.woff": "352f8ead280419dd953d2bc7843386bb9430e8152c2dbd2c2d2494f6b9567ee7",
    "fa-solid-900.woff2": "7f4d3fd0a705dbf8403298aad91d5de6972e6b5d536068eba8b24954a5a0a8c7",
}


def _quoted_end(source: str, start: int) -> int:
    """Index after a JSON double-quoted string; JSON parser checks escapes."""
    i = start + 1
    while i < len(source):
        if source[i] == "\\":
            i += 2
        elif source[i] == '"':
            return i + 1
        else:
            i += 1
    raise ValueError("unterminated JSON string")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSONC key: " + key)
        result[key] = value
    return result


def _reject_non_json_constant(value: str) -> None:
    raise ValueError("invalid JSONC constant: " + value)


def parse_jsonc(source: str) -> object:
    """Parse UTF-8-BOM, comments and trailing commas without rewriting strings."""
    if source.startswith("\ufeff"):
        source = source[1:]
    chars = list(source)
    i = 0
    while i < len(source):
        if source[i] == '"':
            i = _quoted_end(source, i)
        elif source.startswith("//", i):
            end = i + 2
            while end < len(source) and source[end] not in "\r\n":
                end += 1
            for j in range(i, end):
                chars[j] = " "
            i = end
        elif source.startswith("/*", i):
            end = source.find("*/", i + 2)
            if end == -1:
                raise ValueError("unterminated JSONC block comment")
            for j in range(i, end + 2):
                if source[j] not in "\r\n":
                    chars[j] = " "
            i = end + 2
        else:
            i += 1

    cleaned = "".join(chars)
    chars = list(cleaned)
    i = 0
    while i < len(cleaned):
        if cleaned[i] == '"':
            i = _quoted_end(cleaned, i)
        elif cleaned[i] == ",":
            preceding = i - 1
            while preceding >= 0 and cleaned[preceding].isspace():
                preceding -= 1
            following = i + 1
            while following < len(cleaned) and cleaned[following].isspace():
                following += 1
            if (
                preceding >= 0
                and cleaned[preceding] not in "[{:,"
                and following < len(cleaned)
                and cleaned[following] in "}]"
            ):
                chars[i] = " "
            i += 1
        else:
            i += 1
    return json.loads(
        "".join(chars),
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_json_constant,
    )


def check_editor_document(source: str, *, task_file: bool = False) -> dict:
    """No executable task in any top-level, nested, or OS-specific scope."""
    document = parse_jsonc(source)
    if not isinstance(document, dict):
        raise AssertionError("editor document must be a JSON object")
    if task_file and document.get("tasks") != []:
        raise AssertionError("tracked tasks.json must have an empty tasks array")

    def visit(value: object) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "tasks" and item != []:
                    raise AssertionError("no embedded tasks, including OS overrides")
                if key == "runOn":
                    raise AssertionError("no runOn task triggers in editor documents")
                if key == "task.allowAutomaticTasks" and (
                    type(item) is not str or item != "off"
                ):
                    raise AssertionError("workspace must not request automatic tasks")
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(document)
    return document


def check_font_blob(name: str, content: bytes) -> None:
    expected = KNOWN_FONTS.get(name)
    if expected is None:
        raise AssertionError("unexpected font asset: " + name)
    if hashlib.sha256(content).hexdigest() != expected:
        raise AssertionError("font content differs from known-good asset: " + name)


class WorkspaceStartupSafetyTests(unittest.TestCase):
    def test_tracked_tasks_have_no_automatic_or_manual_command(self):
        check_editor_document(
            (ROOT / ".vscode" / "tasks.json").read_text(encoding="utf-8"),
            task_file=True,
        )

    def test_settings_cannot_hide_nested_tasks_or_enable_autorun(self):
        check_editor_document(
            (ROOT / ".vscode" / "settings.json").read_text(encoding="utf-8")
        )

    def test_benign_legacy_launch_configurations_are_readable(self):
        launch = parse_jsonc(
            (ROOT / ".vscode" / "launch.json").read_text(encoding="utf-8")
        )
        self.assertIsInstance(launch.get("configurations"), list)
        tasks = parse_jsonc(
            (ROOT / ".vscode" / "tasks.json").read_text(encoding="utf-8")
        )
        if "configurations" in tasks:
            self.assertIsInstance(tasks["configurations"], list)

    def test_code_workspace_files_cannot_define_startup_tasks(self):
        for path in ROOT.rglob("*.code-workspace"):
            with self.subTest(path=path.relative_to(ROOT)):
                check_editor_document(path.read_text(encoding="utf-8"))

    def test_font_assets_match_exact_preexisting_bytes(self):
        font_dir = ROOT / "public" / "fonts"
        observed = {path.name for path in font_dir.iterdir() if path.name != "README.md"}
        self.assertEqual(observed, set(KNOWN_FONTS))
        for name in sorted(observed):
            with self.subTest(name=name):
                path = font_dir / name
                self.assertTrue(path.is_file() and not path.is_symlink())
                check_font_blob(name, path.read_bytes())

    def test_jsonc_bom_comments_trailing_commas_and_literal_strings(self):
        source = (
            '\ufeff{\n // a task comment\n'
            ' "note": "literal // ,} /* text */",\n'
            ' /* two\n lines */ "tasks": [ ],\n'
            ' "configurations": [{"name": "Run My Project",}],\n'
            ' "task.allowAutomaticTasks": "off",\n}\n'
        )
        actual = check_editor_document(source, task_file=True)
        self.assertEqual(actual["note"], "literal // ,} /* text */")
        self.assertEqual(actual["configurations"][0]["name"], "Run My Project")

    def test_inherited_command_and_os_overrides_fail_closed(self):
        examples = (
            '{"tasks":[{"type":"shell","command":"node","args":["font.ttf"],'
            '"runOptions":{"runOn":"folderOpen"}}]}',
            '{"tasks":[{"type":"shell","command":"echo benign",'
            '"windows":{"command":"node","args":["public/fonts/fa-solid-600.ttf"]}}]}',
            '{"tasks":[],"osx":{"tasks":[{"command":"node font.ttf",'
            '"runOptions":{"runOn":"folderOpen"}}]}}',
            '{"tasks":[],"linux":{"runOptions":{"run\\u004fn":"folderOpen"}}}',
        )
        for source in examples:
            with self.subTest(source=source), self.assertRaises(AssertionError):
                check_editor_document(source)

    def test_nested_tasks_and_string_on_are_rejected(self):
        examples = (
            '{"tasks":[],"windows":{"tasks":[{"command":"node font.ttf"}]}}',
            '{"nested":{"task.allowAutomaticTasks":"on"}}',
            '{"task.allowAutomaticTasks":true}',
            '{"tasks":[],"nested":{"runOn":"folder\\u004fpen"}}',
        )
        for source in examples:
            with self.subTest(source=source), self.assertRaises(AssertionError):
                check_editor_document(source)

    def test_code_workspace_task_and_settings_are_rejected(self):
        examples = (
            '{"folders":[{"path":"."}],"tasks":{"version":"2.0.0",'
            '"tasks":[{"command":"node fake.ttf"}]}}',
            '{"folders":[{"path":"."}],"settings":'
            '{"task.allowAutomaticTasks":"on"}}',
        )
        for source in examples:
            with self.subTest(source=source), self.assertRaises(AssertionError):
                check_editor_document(source)

    def test_fake_font_with_valid_magic_still_fails(self):
        forged_ttf = b"\x00\x01\x00\x00" + b"valid-looking-header" + b"node payload"
        with self.assertRaises(AssertionError):
            check_font_blob("fa-solid-900.ttf", forged_ttf)
        with self.assertRaises(AssertionError):
            check_font_blob("fa-solid-600.ttf", forged_ttf)

    def test_invalid_and_duplicate_jsonc_fail_closed(self):
        for source in (
            '{"tasks":[,]}',
            '{"tasks":[]',
            '{"tasks":[],"tasks":[{"command":"node"}]}',
            '{"tasks":[] /* unfinished',
            '{"tasks":[],"extra":NaN}',
            '[]',
        ):
            with self.subTest(source=source), self.assertRaises(
                (AssertionError, ValueError)
            ):
                check_editor_document(source, task_file=True)


if __name__ == "__main__":
    unittest.main()
