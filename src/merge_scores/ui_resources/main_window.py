################################################################################
## Form generated from reading UI file 'main_window.ui'
##
## Created by: Qt User Interface Compiler version 6.11.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import QCoreApplication, QMetaObject, Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFormLayout,
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


class Ui_MergeWindow:
    def setupUi(self, MergeWindow):
        if not MergeWindow.objectName():
            MergeWindow.setObjectName("MergeWindow")
        MergeWindow.resize(1021, 709)
        self.actionInfo = QAction(MergeWindow)
        self.actionInfo.setObjectName("actionInfo")
        self.actionInfo.setMenuRole(QAction.MenuRole.AboutRole)
        self.centralwidget = QWidget(MergeWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.labelFiles = QLabel(self.centralwidget)
        self.labelFiles.setObjectName("labelFiles")

        self.verticalLayout.addWidget(self.labelFiles)

        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.gridLayout.setVerticalSpacing(6)
        self.fileList = QListWidget(self.centralwidget)
        self.fileList.setObjectName("fileList")
        self.fileList.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.fileList.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        self.gridLayout.addWidget(self.fileList, 0, 0, 1, 1)

        self.verticalGroupBox = QGroupBox(self.centralwidget)
        self.verticalGroupBox.setObjectName("verticalGroupBox")
        self.verticalLayout_2 = QVBoxLayout(self.verticalGroupBox)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.verticalSpacer = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_2.addItem(self.verticalSpacer)

        self.btnUp = QPushButton(self.verticalGroupBox)
        self.btnUp.setObjectName("btnUp")

        self.verticalLayout_2.addWidget(self.btnUp)

        self.btnRemoveFiles = QPushButton(self.verticalGroupBox)
        self.btnRemoveFiles.setObjectName("btnRemoveFiles")

        self.verticalLayout_2.addWidget(self.btnRemoveFiles)

        self.btnDown = QPushButton(self.verticalGroupBox)
        self.btnDown.setObjectName("btnDown")

        self.verticalLayout_2.addWidget(self.btnDown)

        self.btnClearFiles = QPushButton(self.verticalGroupBox)
        self.btnClearFiles.setObjectName("btnClearFiles")

        self.verticalLayout_2.addWidget(self.btnClearFiles)

        self.btnAddFiles = QPushButton(self.verticalGroupBox)
        self.btnAddFiles.setObjectName("btnAddFiles")

        self.verticalLayout_2.addWidget(self.btnAddFiles)

        self.gridLayout.addWidget(self.verticalGroupBox, 0, 4, 1, 1)

        self.verticalLayout.addLayout(self.gridLayout)

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

        self.chkNewPage = QCheckBox(self.groupOptions)
        self.chkNewPage.setObjectName("chkNewPage")

        self.formLayout.setWidget(3, QFormLayout.ItemRole.FieldRole, self.chkNewPage)

        self.chkToc = QCheckBox(self.groupOptions)
        self.chkToc.setObjectName("chkToc")

        self.formLayout.setWidget(4, QFormLayout.ItemRole.FieldRole, self.chkToc)

        self.labelTocName = QLabel(self.groupOptions)
        self.labelTocName.setObjectName("labelTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.LabelRole, self.labelTocName)

        self.txtTocName = QLineEdit(self.groupOptions)
        self.txtTocName.setObjectName("txtTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.FieldRole, self.txtTocName)

        self.chkTitlePage = QCheckBox(self.groupOptions)
        self.chkTitlePage.setObjectName("chkTitlePage")

        self.formLayout.setWidget(6, QFormLayout.ItemRole.FieldRole, self.chkTitlePage)

        self.labelTitleSvg = QLabel(self.groupOptions)
        self.labelTitleSvg.setObjectName("labelTitleSvg")

        self.formLayout.setWidget(7, QFormLayout.ItemRole.LabelRole, self.labelTitleSvg)

        self.layoutTitleSvg = QHBoxLayout()
        self.layoutTitleSvg.setObjectName("layoutTitleSvg")
        self.txtTitleSvg = QLineEdit(self.groupOptions)
        self.txtTitleSvg.setObjectName("txtTitleSvg")

        self.layoutTitleSvg.addWidget(self.txtTitleSvg)

        self.btnBrowseTitleSvg = QPushButton(self.groupOptions)
        self.btnBrowseTitleSvg.setObjectName("btnBrowseTitleSvg")

        self.layoutTitleSvg.addWidget(self.btnBrowseTitleSvg)

        self.formLayout.setLayout(7, QFormLayout.ItemRole.FieldRole, self.layoutTitleSvg)

        self.verticalLayout.addWidget(self.groupOptions)

        self.btnMerge = QPushButton(self.centralwidget)
        self.btnMerge.setObjectName("btnMerge")

        self.verticalLayout.addWidget(self.btnMerge)

        self.labelLog = QLabel(self.centralwidget)
        self.labelLog.setObjectName("labelLog")

        self.verticalLayout.addWidget(self.labelLog)

        self.txtLog = QPlainTextEdit(self.centralwidget)
        self.txtLog.setObjectName("txtLog")
        self.txtLog.setReadOnly(True)

        self.verticalLayout.addWidget(self.txtLog)

        MergeWindow.setCentralWidget(self.centralwidget)
        self.toolBar = QToolBar(MergeWindow)
        self.toolBar.setObjectName("toolBar")
        MergeWindow.addToolBar(Qt.ToolBarArea.TopToolBarArea, self.toolBar)

        self.toolBar.addAction(self.actionInfo)

        self.retranslateUi(MergeWindow)

        QMetaObject.connectSlotsByName(MergeWindow)

    # setupUi

    def retranslateUi(self, MergeWindow):
        MergeWindow.setWindowTitle(QCoreApplication.translate("MergeWindow", "MergeMuseScores", None))
        self.actionInfo.setText(QCoreApplication.translate("MergeWindow", "Info", None))
        self.labelFiles.setText(QCoreApplication.translate("MergeWindow", "MuseScore Files", None))
        self.btnUp.setText(QCoreApplication.translate("MergeWindow", "Move Up", None))
        self.btnRemoveFiles.setText(QCoreApplication.translate("MergeWindow", "Remove Selected", None))
        self.btnDown.setText(QCoreApplication.translate("MergeWindow", "Move Down", None))
        self.btnClearFiles.setText(QCoreApplication.translate("MergeWindow", "Clear", None))
        self.btnAddFiles.setText(QCoreApplication.translate("MergeWindow", "Add Files", None))
        self.groupOptions.setTitle(QCoreApplication.translate("MergeWindow", "Merge Options", None))
        self.labelOutputName.setText(QCoreApplication.translate("MergeWindow", "Output Name", None))
        self.labelOutputDir.setText(QCoreApplication.translate("MergeWindow", "Output Directory", None))
        self.btnBrowseOutputDir.setText(QCoreApplication.translate("MergeWindow", "Browse...", None))
        self.labelStyle.setText(QCoreApplication.translate("MergeWindow", "Style File", None))
        self.btnBrowseStyle.setText(QCoreApplication.translate("MergeWindow", "Browse...", None))
        self.chkNewPage.setText(QCoreApplication.translate("MergeWindow", "Insert Page Break Between Scores", None))
        self.chkToc.setText(QCoreApplication.translate("MergeWindow", "Generate Table of Contents", None))
        self.labelTocName.setText(QCoreApplication.translate("MergeWindow", "TOC Title", None))
        self.txtTocName.setText(QCoreApplication.translate("MergeWindow", "Table of Contents", None))
        self.chkTitlePage.setText(QCoreApplication.translate("MergeWindow", "Generate Title Page", None))
        self.labelTitleSvg.setText(QCoreApplication.translate("MergeWindow", "Title Page SVG", None))
        self.btnBrowseTitleSvg.setText(QCoreApplication.translate("MergeWindow", "Browse...", None))
        self.btnMerge.setText(QCoreApplication.translate("MergeWindow", "Merge Scores", None))
        self.labelLog.setText(QCoreApplication.translate("MergeWindow", "Log Output", None))
        self.toolBar.setWindowTitle(QCoreApplication.translate("MergeWindow", "toolBar", None))

    # retranslateUi
