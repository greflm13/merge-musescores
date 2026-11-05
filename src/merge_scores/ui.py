#!/usr/bin/env python

import sys
import platform
import os
import json
from pathlib import Path

from PySide6.QtCore import QFile, QIODevice, QLibraryInfo, QLocale, QTranslator
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

import merge_scores.ui_resources.resources_rc  # noqa: F401
from merge_scores.main import get_version
from merge_scores.modules.gui import APP_ICON, MergeWindow
from typing import Any


def get_musescore_config_file() -> Path:
    if platform.system() == "Windows":
        base = Path(os.getenv("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))

    return base / "MuseScore" / "MuseScore4.ini"


CONFIG_FILE = get_musescore_config_file()


def parse_theme_json(raw: str) -> dict[str, Any]:
    """Decode the JSON theme object stored in MuseScore4.ini."""
    if not raw:
        return {}

    candidates = [raw.strip()]

    try:
        decoded = json.loads(raw)
        if isinstance(decoded, str):
            candidates.append(decoded)
        elif isinstance(decoded, list):
            return decoded[0] if decoded and isinstance(decoded[0], dict) else {}
        elif isinstance(decoded, dict):
            return decoded
    except (json.JSONDecodeError, TypeError):
        pass

    for candidate in candidates:
        try:
            decoded = json.loads(candidate)
            if isinstance(decoded, list):
                return decoded[0] if decoded and isinstance(decoded[0], dict) else {}
            if isinstance(decoded, dict):
                return decoded
        except (json.JSONDecodeError, TypeError):
            continue

    return {}


def load_musescore_config() -> dict[str, Any]:
    """Read MuseScore's INI file and return the relevant UI settings.

    ConfigParser is intentionally not used here: MuseScore's INI keys contain
    backslashes and its theme value is JSON embedded inside an INI value.
    A small parser avoids interpolation and escaping surprises.
    """
    result: dict[str, Any] = {"theme_code": "dark", "theme": {}}

    if not CONFIG_FILE.is_file():
        return result

    try:
        import configparser

        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str
        parser.read(CONFIG_FILE, encoding="utf-8")

        if not parser.has_section("ui"):
            return result

        theme_code = parser.get("ui", r"application\currentThemeCode", fallback="dark").strip().strip('"')
        result["theme_code"] = theme_code or "dark"

        raw_themes = parser.get("ui", r"application\themes", fallback="")
        result["theme"] = parse_theme_json(raw_themes)

    except (OSError, UnicodeError, ValueError) as exc:
        print(f"Could not read MuseScore preferences ({CONFIG_FILE}): {exc}")

    return result


def generate_accent_colors(accent: str) -> dict[str, str]:
    color = QColor(accent)

    if not color.isValid():
        color = QColor("#397caf")

    return {
        "ACCENT": color.name(),
        "ACCENT_HOVER": color.lighter(115).name(),
        "ACCENT_PRESSED": color.darker(115).name(),
        "ACCENT_BORDER": color.darker(125).name(),
        "ACCENT_DARK": color.darker(125).name(),
    }


def apply_color_to_stylesheet(stylesheet: str, color: str) -> str:
    colors = generate_accent_colors(color)

    for name, value in colors.items():
        stylesheet = stylesheet.replace("{{" + name + "}}", value)

    return stylesheet


def setup_theme(app: QApplication, theme_code: str, theme: dict) -> None:
    accent = theme.get("accentColor", "#397caf")
    resource_path = f":/style/{theme_code}.qss"
    resource_file = QFile(resource_path)

    if resource_file.open(QIODevice.OpenModeFlag.ReadOnly | QIODevice.OpenModeFlag.Text):
        stylesheet = apply_color_to_stylesheet(resource_file.readAll().toStdString(), accent)
        resource_file.close()

        app.setStyleSheet(stylesheet)
        return

    project_root = Path(__file__).resolve().parent.parent.parent
    stylesheet_path = project_root / "style" / f"{theme_code}.qss"

    if stylesheet_path.is_file():
        app.setStyleSheet(apply_color_to_stylesheet(stylesheet_path.read_text(encoding="utf-8"), accent))
        return

    print(f"Theme stylesheet not found: {stylesheet_path}")


def setup_translations(app: QApplication) -> list[QTranslator]:

    translators = []

    translations_path = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)

    qt_translator = QTranslator(app)

    if qt_translator.load(QLocale.system(), "qtbase", "_", translations_path):
        app.installTranslator(qt_translator)
        translators.append(qt_translator)

    app_translator = QTranslator(app)

    if app_translator.load(QLocale.system(), "i18n", "_", ":/translations"):
        app.installTranslator(app_translator)
        translators.append(app_translator)

    return translators


def main() -> int:

    app = QApplication(sys.argv)

    app.setApplicationVersion(get_version())
    app.setWindowIcon(APP_ICON)

    app.setStyle("Fusion")

    settings = load_musescore_config()

    theme_code = str(settings.get("theme_code") or "dark")
    theme = settings.get("theme", {})
    setup_theme(app, theme_code, theme)
    translators = setup_translations(app)

    window = MergeWindow()
    window.show()

    try:
        return app.exec()
    finally:
        translators.clear()


if __name__ == "__main__":
    sys.exit(main())
