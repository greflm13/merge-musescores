#!/usr/bin/env python

import json
import os
import platform
import sys
from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined
from PySide6.QtCore import QFile, QIODevice, QLibraryInfo, QLocale, QTranslator
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication

import merge_scores.ui_resources.resources_rc  # noqa: F401
from merge_scores.main import get_version
from merge_scores.modules.gui import APP_ICON, MergeWindow


def get_musescore_config_file() -> Path:
    if platform.system() == "Windows":
        base = Path(os.getenv("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))

    return base / "MuseScore" / "MuseScore4.ini"


CONFIG_FILE = get_musescore_config_file()


def hover_filter(color: str) -> str:
    return QColor(color).lighter(115).name()


def pressed_filter(color: str) -> str:
    return QColor(color).darker(115).name()


def border_filter(color: str) -> str:
    return QColor(color).darker(125).name()


def disabled_filter(color: str) -> str:
    c = QColor(color)

    disabled = QColor()
    disabled.setHsl(
        c.hslHue(),
        int(c.hslSaturation() * 0.2),
        c.lightness(),
        c.alpha(),
    )

    return disabled.name()


ENV = Environment(
    undefined=StrictUndefined,
    autoescape=False,
)

ENV.filters["hover"] = hover_filter
ENV.filters["pressed"] = pressed_filter
ENV.filters["border"] = border_filter
ENV.filters["disabled"] = disabled_filter


def parse_theme_json(raw: str) -> list[dict[str, Any]]:
    """Decode the JSON theme object stored in MuseScore4.ini."""
    if not raw:
        return {}

    candidates = [raw.strip()]

    try:
        decoded = json.loads(raw)
        if isinstance(decoded, str):
            candidates.append(decoded)
        elif isinstance(decoded, list):
            return decoded
        elif isinstance(decoded, dict):
            return [decoded]
    except (json.JSONDecodeError, TypeError):
        pass

    for candidate in candidates:
        try:
            decoded = json.loads(candidate)
            if isinstance(decoded, list):
                return decoded
            elif isinstance(decoded, dict):
                return [decoded]
        except (json.JSONDecodeError, TypeError):
            continue

    return {}


def load_musescore_config() -> tuple[list[dict[str, Any]], str]:
    """Read MuseScore's INI file and return the relevant UI settings.

    ConfigParser is intentionally not used here: MuseScore's INI keys contain
    backslashes and its theme value is JSON embedded inside an INI value.
    A small parser avoids interpolation and escaping surprises.
    """
    result: list[dict[str, Any]] = []
    theme_code = "dakr"

    if not CONFIG_FILE.is_file():
        return result, theme_code

    try:
        import configparser

        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str
        parser.read(CONFIG_FILE, encoding="utf-8")

        if not parser.has_section("ui"):
            return result, theme_code

        raw_themes = parser.get("ui", r"application\themes", fallback="")
        theme_code = parser.get("ui", r"application\currentThemeCode", fallback="dark")
        result = parse_theme_json(raw_themes)

    except (OSError, UnicodeError, ValueError) as exc:
        print(f"Could not read MuseScore preferences ({CONFIG_FILE}): {exc}")

    return result, theme_code


def setup_theme(app: QApplication, theme: dict[str, str]) -> None:
    file = QFile(":/style/style.qss.j2")

    if file.open(QIODevice.OpenModeFlag.ReadOnly | QIODevice.OpenModeFlag.Text):
        template_text = file.readAll().toStdString()
        file.close()

        stylesheet = ENV.from_string(template_text).render(**{k: v for k, v in theme.items()})

        app.setStyleSheet(stylesheet)
        return


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

    themes, theme_code = load_musescore_config()
    theme = {}
    for the in themes:
        if the["codeKey"] == theme_code:
            theme = the

    setup_theme(app, theme)
    translators = setup_translations(app)

    window = MergeWindow(theme["fontPrimaryColor"])
    window.show()

    try:
        return app.exec()
    finally:
        translators.clear()


if __name__ == "__main__":
    sys.exit(main())
