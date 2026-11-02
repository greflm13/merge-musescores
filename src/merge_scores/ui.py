#!/usr/bin/env python

import sys
from pathlib import Path

from PySide6.QtCore import QLibraryInfo, QLocale, QTranslator
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication

import merge_scores.ui_resources.resources_rc  # noqa: F401
from merge_scores.main import get_version
from merge_scores.modules.gui import APP_ICON, MergeWindow


def setup_palette(app: QApplication) -> None:
    """Apply MuseScore 4-inspired base colors."""

    palette = QPalette()

    palette.setColor(QPalette.ColorRole.Window, QColor("#27272B"))
    palette.setColor(QPalette.ColorRole.WindowText, QColor("#E6E6E8"))

    palette.setColor(QPalette.ColorRole.Base, QColor("#202024"))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor("#303035"))

    palette.setColor(QPalette.ColorRole.ToolTipBase, QColor("#39393C"))
    palette.setColor(QPalette.ColorRole.ToolTipText, QColor("#FFFFFF"))

    palette.setColor(QPalette.ColorRole.Text, QColor("#E6E6E8"))

    palette.setColor(QPalette.ColorRole.Button, QColor("#39393C"))
    palette.setColor(QPalette.ColorRole.ButtonText, QColor("#E6E6E8"))

    palette.setColor(QPalette.ColorRole.Highlight, QColor("#3977B8"))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#FFFFFF"))

    palette.setColor(QPalette.ColorRole.PlaceholderText, QColor("#929298"))

    app.setPalette(palette)


def setup_theme(app: QApplication) -> None:

    project_root = Path(__file__).resolve().parent.parent.parent

    stylesheet = project_root / "style" / "musescore_dark.qss"

    if stylesheet.is_file():
        app.setStyleSheet(stylesheet.read_text(encoding="utf-8"))
    else:
        print(f"Theme stylesheet not found: {stylesheet}")


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

    setup_palette(app)
    setup_theme(app)

    translators = setup_translations(app)

    window = MergeWindow()
    window.show()

    try:
        return app.exec()
    finally:
        translators.clear()


if __name__ == "__main__":
    sys.exit(main())
