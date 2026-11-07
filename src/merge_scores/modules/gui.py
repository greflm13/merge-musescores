import configparser
import logging
import os
import platform
import json
from pathlib import Path

from PySide6.QtCore import QObject, QStandardPaths, Qt, QThread, QUrl, Signal, QSortFilterProxyModel, QStringListModel, QSize
from PySide6.QtGui import QColor, QDesktopServices, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import (
    QAbstractButton,
    QAbstractItemView,
    QDialogButtonBox,
    QFileDialog,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QWidget,
    QCompleter,
    QListView,
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

DATA_FILE = DATA_DIR / "MergeMuseScores.ini"

config = configparser.ConfigParser()

if DATA_FILE.exists():
    config.read(DATA_FILE, encoding="utf-8")


def get_last_location(key: str, fallback: Path) -> str:
    """Return the last selected location or the default directory."""
    saved_path = config.get("Locations", key, fallback=str(fallback))

    if Path(saved_path).is_dir():
        return saved_path

    return str(fallback)


def get_last_name(key: str, fallback: str) -> str:
    """Return the last selected name or the default name."""
    name = config.get("Names", key, fallback=str(fallback))
    return name


def get_name_history(key: str, limit: int = 20) -> list[str]:
    """Return previously used names, newest first."""
    raw = config.get("History", key, fallback="[]")

    try:
        history = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []

    if not isinstance(history, list):
        return []

    return [name for name in history if isinstance(name, str) and name.strip()][:limit]


def save_last_location(key: str, path: Path) -> None:
    """Save the last selected location."""
    if not config.has_section("Locations"):
        config.add_section("Locations")

    config.set("Locations", key, str(path))

    with DATA_FILE.open("w", encoding="utf-8") as file:
        config.write(file)


def save_last_name(key: str, name: str) -> None:
    """Save the last name."""
    if not config.has_section("Names"):
        config.add_section("Names")

    config.set("Names", key, name)

    with DATA_FILE.open("w", encoding="utf-8") as file:
        config.write(file)


def save_name_history(key: str, name: str, limit: int = 20) -> None:
    """Save a name to history, moving it to the front."""
    name = name.strip()

    if not name:
        return

    history = get_name_history(key, limit)

    history = [item for item in history if item.casefold() != name.casefold()]

    history.insert(0, name)

    history = history[:limit]

    if not config.has_section("History"):
        config.add_section("History")

    config.set("History", key, json.dumps(history, ensure_ascii=False))

    with DATA_FILE.open("w", encoding="utf-8") as file:
        config.write(file)


def recolor_icon(icon: QIcon, color: str, size: int = 16) -> QIcon:
    pixmap = icon.pixmap(QSize(size, size))

    if pixmap.isNull():
        return icon

    dpr = pixmap.devicePixelRatioF()

    result = QPixmap(pixmap.size())
    result.setDevicePixelRatio(dpr)
    result.fill(Qt.GlobalColor.transparent)

    painter = QPainter(result)
    painter.drawPixmap(0, 0, pixmap)
    painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_SourceIn)
    painter.fillRect(result.rect(), QColor(color))
    painter.end()

    return QIcon(result)


def prepare_file_dialog(dialog: QFileDialog, icon_color: str) -> None:
    dialog.setOption(QFileDialog.Option.DontUseNativeDialog, True)

    button_box = dialog.findChild(QDialogButtonBox)
    if not button_box:
        return

    for button in dialog.findChildren(QPushButton):
        role = button_box.buttonRole(button)

        if role == QDialogButtonBox.ButtonRole.AcceptRole:
            button.setObjectName("acceptButton")
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

        elif role == QDialogButtonBox.ButtonRole.RejectRole:
            button.setObjectName("rejectButton")
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

    for widget in dialog.findChildren(QWidget):
        if isinstance(widget, QAbstractButton):
            widget.setIcon(recolor_icon(widget.icon(), icon_color))


class FuzzyFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.query = ""

        self.setDynamicSortFilter(True)

    def set_query(self, query: str) -> None:
        self.query = query.casefold().strip()
        self.invalidateFilter()
        self.sort(0, Qt.SortOrder.AscendingOrder)

    def score(self, text: str) -> tuple[int, int, int] | None:
        text = text.casefold()
        query = self.query

        if not query:
            return (0, 0, 0)

        if query in text:
            return (0, text.index(query), len(text) - len(query))

        positions: list[int] = []
        position = 0

        for char in query:
            position = text.find(char, position)

            if position == -1:
                return None

            positions.append(position)
            position += 1

        first = positions[0]
        span = positions[-1] - first + 1
        gaps = span - len(query)

        return (1, first, gaps)

    def filterAcceptsRow(self, source_row, source_parent) -> bool:
        index = self.sourceModel().index(source_row, 0, source_parent)
        text = self.sourceModel().data(index)

        return self.score(str(text)) is not None

    def lessThan(self, left, right):
        left_text = str(self.sourceModel().data(left))
        right_text = str(self.sourceModel().data(right))

        left_score = self.score(left_text)
        right_score = self.score(right_text)

        if left_score is None:
            return False

        if right_score is None:
            return True

        return left_score < right_score


class FuzzyCompleter(QCompleter):
    def __init__(self, line_edit, suggestions=None, parent=None):
        super().__init__(parent or line_edit)

        self.line_edit = line_edit

        self.source_model = QStringListModel(
            suggestions or [],
            self,
        )

        self.proxy_model = FuzzyFilterProxyModel(self)
        self.proxy_model.setSourceModel(self.source_model)

        self.setModel(self.proxy_model)
        self.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.setFilterMode(Qt.MatchFlag.MatchContains)

        line_edit.setCompleter(self)

        line_edit.textEdited.connect(self.update_suggestions)

        self.activated.connect(self.apply_suggestion)

        popup = self.popup()

        if isinstance(popup, QListView):
            popup.setObjectName("nameCompleterPopup")
            popup.setSpacing(0)
            popup.setUniformItemSizes(True)
            popup.setMaximumHeight(250)
            popup.setMinimumHeight(0)

    def update_suggestions(self, text: str):
        self.proxy_model.set_query(text)

        if not text.strip():
            popup = self.popup()
            if popup is not None:
                popup.hide()
            return

        self.setCompletionPrefix("")

        if self.proxy_model.rowCount() > 0:
            self.complete()
        else:
            popup = self.popup()
            if popup is not None:
                popup.hide()

    def apply_suggestion(self, text: str):
        self.line_edit.setText(text)

    def update_history(self, suggestions: list[str]):
        self.source_model.setStringList(suggestions)

    def splitPath(self, path: str) -> list[str]:
        return [""]


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
    def __init__(self, icon_color):
        super().__init__()
        self.icon_color = icon_color

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
        self.output_name_completer = FuzzyCompleter(self.ui.txtOutputName, get_name_history("out_file"), self)
        self.ui.txtTocName.setText(get_last_name("toc", self.tr("Table of Contents")))
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

        dialog = QFileDialog(self)
        dialog.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        dialog.setWindowTitle(self.tr("Select MuseScore Files"))
        dialog.setDirectory(str(start_dir))
        dialog.setNameFilter(self.tr("MuseScore Files (*.mscz);;All Files (*)"))
        dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)

        prepare_file_dialog(dialog, self.icon_color)

        if not dialog.exec():
            return

        files = dialog.selectedFiles()

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

        dialog = QFileDialog(self)
        dialog.setWindowTitle(self.tr("Select Style File"))
        dialog.setDirectory(str(start_dir))
        dialog.setNameFilter(self.tr("MuseScore Style Files (*.mss);;All Files (*)"))
        dialog.setFileMode(QFileDialog.FileMode.ExistingFile)

        prepare_file_dialog(dialog, self.icon_color)

        if not dialog.exec():
            return

        style = dialog.selectedFiles()[0]

        self.ui.txtStyle.setText(style)
        save_last_location("style", Path(style).parent)

    def browse_title(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))

        start_dir = get_last_location("title", docs)

        dialog = QFileDialog(self)
        dialog.setWindowTitle(self.tr("Select Title Page"))
        dialog.setDirectory(str(start_dir))
        dialog.setNameFilter(self.tr("Scalable Vector Graphics (*.svg);;All Files (*)"))
        dialog.setFileMode(QFileDialog.FileMode.ExistingFile)

        prepare_file_dialog(dialog, self.icon_color)

        if not dialog.exec():
            return

        title_svg = dialog.selectedFiles()[0]

        self.ui.txtTitleSvg.setText(title_svg)
        save_last_location("title", Path(title_svg).parent)

    def browse_output_dir(self):
        docs = Path(QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation))
        scores = docs / "MuseScore4" / "Scores"

        default_dir = scores if scores.exists() else docs
        start_dir = get_last_location("output", default_dir)

        dialog = QFileDialog(self)
        dialog.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        dialog.setFileMode(QFileDialog.FileMode.Directory)
        dialog.setOption(QFileDialog.Option.ShowDirsOnly, True)
        dialog.setDirectory(str(start_dir))
        dialog.setWindowTitle(self.tr("Select Output Directory"))

        prepare_file_dialog(dialog, self.icon_color)

        if not dialog.exec():
            return

        directory = dialog.selectedFiles()[0]

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

        save_last_name("toc", self.ui.txtTocName.text())
        save_name_history("out_file", self.ui.txtOutputName.text().strip())
        self.output_name_completer.update_history(get_name_history("out_file"))

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
        open_button.setObjectName("openScoreButton")
        open_button.style().unpolish(open_button)
        open_button.style().polish(open_button)
        open_button.update()
        msg.addButton(self.tr("Close"), QMessageBox.ButtonRole.RejectRole)

        msg.exec()

        if msg.clickedButton() == open_button:
            QDesktopServices.openUrl(QUrl.fromLocalFile(mscz_path))
