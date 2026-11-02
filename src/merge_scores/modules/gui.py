from PySide6.QtCore import (
    Qt,
    QUrl,
    QObject,
    QByteArray,
    QEventLoop,
    QRegularExpression,
    QAbstractTableModel,
)
from PySide6.QtWidgets import (
    QComboBox,
    QCompleter,
    QDialog,
    QFileDialog,
    QFormLayout,
    QLineEdit,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
)
from PySide6.QtGui import QIcon, QDesktopServices, QRegularExpressionValidator

from merge_scores.ui.main import Ui_MergeWindow
from merge_scores.main import merge


class MergeWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.ui = Ui_MergeWindow()
        self.ui.setupUi(self)

        self.ui.btnAddFiles.clicked.connect(self.add_files)
        self.ui.btnMerge.clicked.connect(self.run_merge)

    def add_files(self): ...

    def run_merge(self): ...
