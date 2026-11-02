################################################################################
## Form generated from reading UI file 'main_window.ui'
##
## Created by: Qt User Interface Compiler version 6.11.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import QCoreApplication, QMetaObject, QSize, Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class Ui_MainWindow:
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1224, 906)
        MainWindow.setMinimumSize(QSize(384, 832))
        self.actionInfo = QAction(MainWindow)
        self.actionInfo.setObjectName("actionInfo")
        self.actionInfo.setMenuRole(QAction.MenuRole.AboutRole)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.fileGroupBox = QGroupBox(self.centralwidget)
        self.fileGroupBox.setObjectName("fileGroupBox")
        self.gridLayout = QGridLayout(self.fileGroupBox)
        self.gridLayout.setObjectName("gridLayout")
        self.gridLayout.setVerticalSpacing(6)
        self.fileList = QListWidget(self.fileGroupBox)
        self.fileList.setObjectName("fileList")
        self.fileList.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.fileList.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        self.gridLayout.addWidget(self.fileList, 0, 0, 1, 1)

        self.verticalFrame = QFrame(self.fileGroupBox)
        self.verticalFrame.setObjectName("verticalFrame")
        self.verticalLayout_2 = QVBoxLayout(self.verticalFrame)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.btnUp = QPushButton(self.verticalFrame)
        self.btnUp.setObjectName("btnUp")

        self.verticalLayout_2.addWidget(self.btnUp)

        self.btnRemoveFiles = QPushButton(self.verticalFrame)
        self.btnRemoveFiles.setObjectName("btnRemoveFiles")

        self.verticalLayout_2.addWidget(self.btnRemoveFiles)

        self.btnDown = QPushButton(self.verticalFrame)
        self.btnDown.setObjectName("btnDown")

        self.verticalLayout_2.addWidget(self.btnDown)

        self.verticalSpacer = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_2.addItem(self.verticalSpacer)

        self.btnClearFiles = QPushButton(self.verticalFrame)
        self.btnClearFiles.setObjectName("btnClearFiles")

        self.verticalLayout_2.addWidget(self.btnClearFiles)

        self.btnAddFiles = QPushButton(self.verticalFrame)
        self.btnAddFiles.setObjectName("btnAddFiles")

        self.verticalLayout_2.addWidget(self.btnAddFiles)

        self.gridLayout.addWidget(self.verticalFrame, 0, 4, 1, 1)

        self.verticalLayout.addWidget(self.fileGroupBox)

        self.groupOptions = QGroupBox(self.centralwidget)
        self.groupOptions.setObjectName("groupOptions")
        self.formLayout = QFormLayout(self.groupOptions)
        self.formLayout.setObjectName("formLayout")
        self.labelOutputName = QLabel(self.groupOptions)
        self.labelOutputName.setObjectName("labelOutputName")

        self.formLayout.setWidget(0, QFormLayout.ItemRole.LabelRole, self.labelOutputName)

        self.txtOutputName = QLineEdit(self.groupOptions)
        self.txtOutputName.setObjectName("txtOutputName")

        self.formLayout.setWidget(0, QFormLayout.ItemRole.FieldRole, self.txtOutputName)

        self.labelOutputDir = QLabel(self.groupOptions)
        self.labelOutputDir.setObjectName("labelOutputDir")

        self.formLayout.setWidget(1, QFormLayout.ItemRole.LabelRole, self.labelOutputDir)

        self.layoutOutputDir = QHBoxLayout()
        self.layoutOutputDir.setObjectName("layoutOutputDir")
        self.txtOutputDir = QLineEdit(self.groupOptions)
        self.txtOutputDir.setObjectName("txtOutputDir")

        self.layoutOutputDir.addWidget(self.txtOutputDir)

        self.btnBrowseOutputDir = QPushButton(self.groupOptions)
        self.btnBrowseOutputDir.setObjectName("btnBrowseOutputDir")

        self.layoutOutputDir.addWidget(self.btnBrowseOutputDir)

        self.formLayout.setLayout(1, QFormLayout.ItemRole.FieldRole, self.layoutOutputDir)

        self.labelStyle = QLabel(self.groupOptions)
        self.labelStyle.setObjectName("labelStyle")

        self.formLayout.setWidget(2, QFormLayout.ItemRole.LabelRole, self.labelStyle)

        self.layoutStyle = QHBoxLayout()
        self.layoutStyle.setObjectName("layoutStyle")
        self.txtStyle = QLineEdit(self.groupOptions)
        self.txtStyle.setObjectName("txtStyle")

        self.layoutStyle.addWidget(self.txtStyle)

        self.btnBrowseStyle = QPushButton(self.groupOptions)
        self.btnBrowseStyle.setObjectName("btnBrowseStyle")

        self.layoutStyle.addWidget(self.btnBrowseStyle)

        self.formLayout.setLayout(2, QFormLayout.ItemRole.FieldRole, self.layoutStyle)

        self.chkToc = QCheckBox(self.groupOptions)
        self.chkToc.setObjectName("chkToc")

        self.formLayout.setWidget(4, QFormLayout.ItemRole.FieldRole, self.chkToc)

        self.labelTocName = QLabel(self.groupOptions)
        self.labelTocName.setObjectName("labelTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.LabelRole, self.labelTocName)

        self.txtTocName = QLineEdit(self.groupOptions)
        self.txtTocName.setObjectName("txtTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.FieldRole, self.txtTocName)

        self.labelTitleSvg = QLabel(self.groupOptions)
        self.labelTitleSvg.setObjectName("labelTitleSvg")

        self.formLayout.setWidget(6, QFormLayout.ItemRole.LabelRole, self.labelTitleSvg)

        self.layoutTitleSvg = QHBoxLayout()
        self.layoutTitleSvg.setObjectName("layoutTitleSvg")
        self.txtTitleSvg = QLineEdit(self.groupOptions)
        self.txtTitleSvg.setObjectName("txtTitleSvg")

        self.layoutTitleSvg.addWidget(self.txtTitleSvg)

        self.btnBrowseTitleSvg = QPushButton(self.groupOptions)
        self.btnBrowseTitleSvg.setObjectName("btnBrowseTitleSvg")

        self.layoutTitleSvg.addWidget(self.btnBrowseTitleSvg)

        self.formLayout.setLayout(6, QFormLayout.ItemRole.FieldRole, self.layoutTitleSvg)

        self.chkNewPage = QCheckBox(self.groupOptions)
        self.chkNewPage.setObjectName("chkNewPage")

        self.formLayout.setWidget(3, QFormLayout.ItemRole.FieldRole, self.chkNewPage)

        self.verticalLayout.addWidget(self.groupOptions)

        self.btnMerge = QPushButton(self.centralwidget)
        self.btnMerge.setObjectName("btnMerge")

        self.verticalLayout.addWidget(self.btnMerge)

        self.logGroupBox = QGroupBox(self.centralwidget)
        self.logGroupBox.setObjectName("logGroupBox")
        self.horizontalLayout = QHBoxLayout(self.logGroupBox)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.txtLog = QPlainTextEdit(self.logGroupBox)
        self.txtLog.setObjectName("txtLog")
        self.txtLog.setReadOnly(True)

        self.horizontalLayout.addWidget(self.txtLog)

        self.verticalLayout.addWidget(self.logGroupBox)

        MainWindow.setCentralWidget(self.centralwidget)
        self.toolBar = QToolBar(MainWindow)
        self.toolBar.setObjectName("toolBar")
        MainWindow.addToolBar(Qt.ToolBarArea.TopToolBarArea, self.toolBar)

        self.toolBar.addAction(self.actionInfo)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)

    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", "MergeMuseScores", None))
        self.actionInfo.setText(QCoreApplication.translate("MainWindow", "Info", None))
        self.fileGroupBox.setTitle(QCoreApplication.translate("MainWindow", "MuseScore Files", None))
        self.btnUp.setText(QCoreApplication.translate("MainWindow", "Move Up", None))
        self.btnRemoveFiles.setText(QCoreApplication.translate("MainWindow", "Remove Selected", None))
        self.btnDown.setText(QCoreApplication.translate("MainWindow", "Move Down", None))
        self.btnClearFiles.setText(QCoreApplication.translate("MainWindow", "Clear", None))
        self.btnAddFiles.setText(QCoreApplication.translate("MainWindow", "Add Files", None))
        self.groupOptions.setTitle(QCoreApplication.translate("MainWindow", "Merge Options", None))
        self.labelOutputName.setText(QCoreApplication.translate("MainWindow", "Output Name", None))
        self.labelOutputDir.setText(QCoreApplication.translate("MainWindow", "Output Directory", None))
        self.btnBrowseOutputDir.setText(QCoreApplication.translate("MainWindow", "Browse...", None))
        self.labelStyle.setText(QCoreApplication.translate("MainWindow", "Style File", None))
        self.btnBrowseStyle.setText(QCoreApplication.translate("MainWindow", "Browse...", None))
        self.chkToc.setText(QCoreApplication.translate("MainWindow", "Generate Table of Contents", None))
        self.labelTocName.setText(QCoreApplication.translate("MainWindow", "TOC Title", None))
        self.txtTocName.setText(QCoreApplication.translate("MainWindow", "Table of Contents", None))
        self.labelTitleSvg.setText(QCoreApplication.translate("MainWindow", "Title Page SVG", None))
        self.btnBrowseTitleSvg.setText(QCoreApplication.translate("MainWindow", "Browse...", None))
        self.chkNewPage.setText(QCoreApplication.translate("MainWindow", "Insert Page Break Between Scores", None))
        self.btnMerge.setText(QCoreApplication.translate("MainWindow", "Merge Scores", None))
        self.logGroupBox.setTitle(QCoreApplication.translate("MainWindow", "Log Output", None))
        self.toolBar.setWindowTitle(QCoreApplication.translate("MainWindow", "toolBar", None))

    # retranslateUi
