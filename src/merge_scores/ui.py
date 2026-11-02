#!/usr/bin/env python
import sys

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QTranslator, QLibraryInfo, QLocale

# import ui.resources_rc  # noqa: F401
from merge_scores.modules.gui import MergeWindow

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # app.setApplicationVersion(get_version())
    # app.setWindowIcon(APP_ICON)
    # path = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)
    # translator = QTranslator(app)
    # if translator.load(QLocale.system(), "qtbase", "_", path):
    #     app.installTranslator(translator)
    # translator = QTranslator(app)
    # path = ":/translations"
    # if translator.load(QLocale.system(), "i18n", "_", path):
    #     app.installTranslator(translator)
    window = MergeWindow()
    window.show()

    sys.exit(app.exec())
