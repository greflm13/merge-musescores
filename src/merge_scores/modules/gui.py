import configparser
import logging
import os
import platform
from pathlib import Path

from PySide6.QtCore import (
    QObject,
    QStandardPaths,
    QThread,
    QUrl,
    Signal,
)
from PySide6.QtGui import QDesktopServices, QIcon
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QMainWindow,
    QMessageBox,
)

from merge_scores.main import get_version, merge
from merge_scores.ui_resources.main_window import Ui_MainWindow

APP_ICON = QIcon(":/assets/icon.svg")

if platform.system() != "Windows":
    base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    DATA_DIR = base / "mergemusescores"
else:
    DATA_DIR = Path(os.getenv("LOCALAPPDATA", Path.home())) / "mergemusescores" / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)

DATA_FILE = DATA_DIR / "paths.ini"

config = configparser.ConfigParser()

if DATA_FILE.exists():
    config.read(DATA_FILE, encoding="utf-8")


def get_last_location(key: str, fallback: Path) -> str:
    """Return the last selected location or the default directory."""
    saved_path = config.get("Locations", key, fallback=str(fallback))

    if Path(saved_path).is_dir():
        return saved_path

    return str(fallback)


def save_last_location(key: str, path: Path) -> None:
    """Save the last selected location."""
    if not config.has_section("Locations"):
        config.add_section("Locations")

    config.set("Locations", key, str(path))

    with DATA_FILE.open("w", encoding="utf-8") as file:
        config.write(file)


class MergeWorker(QObject):
    finished = Signal()

    def __init__(
        self,
        files,
        out_name,
        out_dir,
        new_page,
        toc,
        toc_name,
        title_page,
        style,
    ):
        super().__init__()
        self.files = files
        self.out_name = out_name
        self.out_dir = out_dir
        self.new_page = new_page
        self.toc = toc
        self.toc_name = toc_name
        self.title_page = title_page
        self.style = style

    def run(self):
        try:
            merge(
                self.files,
                self.out_name,
                self.out_dir,
                self.new_page,
                self.toc,
                self.toc_name,
                self.title_page,
                self.style,
            )
        except Exception as e:
            logging.error(e)
        finally:
            self.finished.emit()


class LogEmitter(QObject):
    log_signal = Signal(str)


class QtLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.emitter = LogEmitter()

    def emit(self, record):
        self.emitter.log_signal.emit(self.format(record))


class MergeWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))
        scores = docs / "MuseScore4" / "Scores"

        default_dir = scores if scores.exists() else docs
        start_dir = get_last_location("output", default_dir)

        self.ui.fileList.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.ui.fileList.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.ui.fileList.setDragEnabled(True)
        self.ui.fileList.setAcceptDrops(True)
        self.ui.fileList.setDropIndicatorShown(True)
        self.ui.btnAddFiles.clicked.connect(self.add_files)
        self.ui.btnClearFiles.clicked.connect(self.clear_files)
        self.ui.btnRemoveFiles.clicked.connect(self.remove_files)
        self.ui.btnUp.clicked.connect(self.up_files)
        self.ui.btnDown.clicked.connect(self.down_files)
        self.ui.btnBrowseStyle.clicked.connect(self.browse_style)
        self.ui.btnBrowseTitleSvg.clicked.connect(self.browse_title)
        self.ui.btnBrowseOutputDir.clicked.connect(self.browse_output_dir)
        self.ui.btnMerge.clicked.connect(self.run_merge)
        self.ui.txtOutputDir.setText(start_dir)
        self.ui.actionInfo.triggered.connect(self._show_info_dialog)

        handler = QtLogHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        handler.emitter.log_signal.connect(self.ui.txtLog.appendPlainText)

        logging.getLogger("defaultlogger").addHandler(handler)
        logging.getLogger("defaultlogger").setLevel(logging.INFO)

    def _show_info_dialog(self) -> None:
        QMessageBox.about(
            self,
            self.tr("About MergeMuseScores"),
            self.tr(f"""
            <b>MergeMuseScores</b><br>
            Version: {get_version()}<br><br>
            Merge multiple Scores into one file.
            """),
        )

    def add_files(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))
        scores = docs / "MuseScore4" / "Scores"

        default_dir = scores if scores.exists() else docs
        start_dir = get_last_location("files", default_dir)
        files, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("Select MuseScore Files"),
            start_dir,  # start directory
            self.tr("MuseScore Files (*.mscz);;All Files (*)"),
        )

        if not files:
            return

        for file in files:
            self.ui.fileList.addItem(file)
        save_last_location("files", Path(files[0]).parent)

    def clear_files(self):
        self.ui.fileList.clear()

    def remove_files(self):
        for item in self.ui.fileList.selectedItems():
            row = self.ui.fileList.row(item)
            self.ui.fileList.takeItem(row)

    def up_files(self):
        rows = sorted({self.ui.fileList.row(item) for item in self.ui.fileList.selectedItems()})

        for row in rows:
            if row == 0:
                continue

            item = self.ui.fileList.takeItem(row)
            self.ui.fileList.insertItem(row - 1, item)
            item.setSelected(True)

    def down_files(self):
        rows = sorted(
            {self.ui.fileList.row(item) for item in self.ui.fileList.selectedItems()},
            reverse=True,
        )

        for row in rows:
            if row >= self.ui.fileList.count() - 1:
                continue

            item = self.ui.fileList.takeItem(row)
            self.ui.fileList.insertItem(row + 1, item)
            item.setSelected(True)

    def browse_style(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))
        styles = docs / "MuseScore4" / "Styles"

        default_dir = styles if styles.exists() else docs
        start_dir = get_last_location("style", default_dir)

        style, _ = QFileDialog.getOpenFileName(
            self,
            self.tr("Select Style File"),
            start_dir,
            self.tr("MuseScore Style Files (*.mss);;All Files (*)"),
        )

        if not style:
            return

        self.ui.txtStyle.setText(style)
        save_last_location("style", Path(style).parent)

    def browse_title(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))

        start_dir = get_last_location("title", docs)
        title_svg, _ = QFileDialog.getOpenFileName(
            self,
            self.tr("Select Title Page"),
            start_dir,
            self.tr("Scalable Vector Graphics (*.svg);;All Files (*)"),
        )

        if not title_svg:
            return

        self.ui.txtTitleSvg.setText(title_svg)
        save_last_location("title", Path(title_svg).parent)

    def browse_output_dir(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))
        scores = docs / "MuseScore4" / "Scores"

        default_dir = scores if scores.exists() else docs
        start_dir = get_last_location("output", default_dir)

        directory = QFileDialog.getExistingDirectory(
            self,
            self.tr("Select Output Directory"),
            start_dir,
            QFileDialog.Option.ShowDirsOnly,
        )

        if not directory:
            return

        self.ui.txtOutputDir.setText(directory)
        save_last_location("output", Path(directory))

    def validate(self):
        invalid = 0
        if not self.ui.txtOutputName.text().strip():
            self.ui.txtOutputName.setStyleSheet("border: 1px solid red;")
            invalid += 1
        else:
            self.ui.txtOutputName.setStyleSheet("")

        if not self.ui.txtOutputDir.text().strip():
            self.ui.txtOutputDir.setStyleSheet("border: 1px solid red;")
            invalid += 1
        else:
            self.ui.txtOutputDir.setStyleSheet("")

        if not self.ui.fileList.count() > 0:
            self.ui.fileList.setStyleSheet("border: 1px solid red;")
            invalid += 1
        else:
            self.ui.fileList.setStyleSheet("")

        if invalid > 0:
            return False
        else:
            return True

    def enable_merge(self):
        self.ui.btnMerge.setEnabled(True)

    def run_merge(self):
        if not self.validate():
            QMessageBox.warning(self, self.tr("Missing Information"), self.tr("Please fill in all required fields."))
            return
        files = [self.ui.fileList.item(i).text() for i in range(self.ui.fileList.count())]

        self.ui.btnMerge.setEnabled(False)

        self.merge_thread = QThread()

        self.merge_worker = MergeWorker(
            files,
            self.ui.txtOutputName.text(),
            self.ui.txtOutputDir.text(),
            self.ui.chkNewPage.isChecked(),
            self.ui.chkToc.isChecked(),
            self.ui.txtTocName.text(),
            self.ui.txtTitleSvg.text(),
            self.ui.txtStyle.text(),
        )

        self.merge_worker.moveToThread(self.merge_thread)

        self.merge_thread.started.connect(self.merge_worker.run)
        self.merge_worker.finished.connect(self.enable_merge)
        self.merge_worker.finished.connect(self.merge_thread.quit)
        self.merge_worker.finished.connect(self.show_done_dialog)
        self.merge_worker.finished.connect(self.merge_worker.deleteLater)
        self.merge_thread.finished.connect(self.merge_thread.deleteLater)

        self.merge_thread.start()

    def show_done_dialog(self):
        mscz_path = os.path.join(self.ui.txtOutputDir.text(), self.ui.txtOutputName.text() + ".mscz")
        msg = QMessageBox(self)
        msg.setWindowTitle(self.tr("Done"))
        msg.setText(self.tr("Scores merged successfully."))
        msg.setIcon(QMessageBox.Icon.Information)

        open_button = msg.addButton(self.tr("Open Score"), QMessageBox.ButtonRole.AcceptRole)
        msg.addButton(self.tr("Close"), QMessageBox.ButtonRole.RejectRole)

        msg.exec()

        if msg.clickedButton() == open_button:
            QDesktopServices.openUrl(QUrl.fromLocalFile(mscz_path))
