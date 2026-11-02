#!/usr/bin/env python
import sys

from PySide6.QtCore import QLibraryInfo, QLocale, QTranslator
from PySide6.QtWidgets import QApplication

import merge_scores.ui.resources_rc  # noqa: F401
from merge_scores.main import get_version
from merge_scores.modules.gui import APP_ICON, MergeWindow

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationVersion(get_version())
    app.setWindowIcon(APP_ICON)
    path = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)
    translator = QTranslator(app)
    if translator.load(QLocale.system(), "qtbase", "_", path):
        app.installTranslator(translator)
    translator = QTranslator(app)
    path = ":/translations"
    if translator.load(QLocale.system(), "i18n", "_", path):
        app.installTranslator(translator)
    window = MergeWindow()
    window.show()

    sys.exit(app.exec())
