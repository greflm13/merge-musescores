# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'main.ui'
##
## Created by: Qt User Interface Compiler version 6.11.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QAbstractItemView, QApplication, QCheckBox, QFormLayout,
    QGroupBox, QHBoxLayout, QLabel, QLineEdit,
    QListWidget, QListWidgetItem, QMainWindow, QPlainTextEdit,
    QPushButton, QSizePolicy, QStatusBar, QVBoxLayout,
    QWidget)

class Ui_MergeWindow(object):
    def setupUi(self, MergeWindow):
        if not MergeWindow.objectName():
            MergeWindow.setObjectName(u"MergeWindow")
        MergeWindow.resize(900, 800)
        self.centralwidget = QWidget(MergeWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.labelFiles = QLabel(self.centralwidget)
        self.labelFiles.setObjectName(u"labelFiles")

        self.verticalLayout.addWidget(self.labelFiles)

        self.fileList = QListWidget(self.centralwidget)
        self.fileList.setObjectName(u"fileList")
        self.fileList.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.fileList.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        self.verticalLayout.addWidget(self.fileList)

        self.horizontalLayoutFiles = QHBoxLayout()
        self.horizontalLayoutFiles.setObjectName(u"horizontalLayoutFiles")
        self.btnAddFiles = QPushButton(self.centralwidget)
        self.btnAddFiles.setObjectName(u"btnAddFiles")

        self.horizontalLayoutFiles.addWidget(self.btnAddFiles)

        self.btnRemoveFiles = QPushButton(self.centralwidget)
        self.btnRemoveFiles.setObjectName(u"btnRemoveFiles")

        self.horizontalLayoutFiles.addWidget(self.btnRemoveFiles)

        self.btnClearFiles = QPushButton(self.centralwidget)
        self.btnClearFiles.setObjectName(u"btnClearFiles")

        self.horizontalLayoutFiles.addWidget(self.btnClearFiles)


        self.verticalLayout.addLayout(self.horizontalLayoutFiles)

        self.groupOptions = QGroupBox(self.centralwidget)
        self.groupOptions.setObjectName(u"groupOptions")
        self.formLayout = QFormLayout(self.groupOptions)
        self.formLayout.setObjectName(u"formLayout")
        self.labelOutputName = QLabel(self.groupOptions)
        self.labelOutputName.setObjectName(u"labelOutputName")

        self.formLayout.setWidget(0, QFormLayout.ItemRole.LabelRole, self.labelOutputName)

        self.txtOutputName = QLineEdit(self.groupOptions)
        self.txtOutputName.setObjectName(u"txtOutputName")

        self.formLayout.setWidget(0, QFormLayout.ItemRole.FieldRole, self.txtOutputName)

        self.labelOutputDir = QLabel(self.groupOptions)
        self.labelOutputDir.setObjectName(u"labelOutputDir")

        self.formLayout.setWidget(1, QFormLayout.ItemRole.LabelRole, self.labelOutputDir)

        self.layoutOutputDir = QHBoxLayout()
        self.layoutOutputDir.setObjectName(u"layoutOutputDir")
        self.txtOutputDir = QLineEdit(self.groupOptions)
        self.txtOutputDir.setObjectName(u"txtOutputDir")

        self.layoutOutputDir.addWidget(self.txtOutputDir)

        self.btnBrowseOutputDir = QPushButton(self.groupOptions)
        self.btnBrowseOutputDir.setObjectName(u"btnBrowseOutputDir")

        self.layoutOutputDir.addWidget(self.btnBrowseOutputDir)


        self.formLayout.setLayout(1, QFormLayout.ItemRole.FieldRole, self.layoutOutputDir)

        self.labelStyle = QLabel(self.groupOptions)
        self.labelStyle.setObjectName(u"labelStyle")

        self.formLayout.setWidget(2, QFormLayout.ItemRole.LabelRole, self.labelStyle)

        self.layoutStyle = QHBoxLayout()
        self.layoutStyle.setObjectName(u"layoutStyle")
        self.txtStyle = QLineEdit(self.groupOptions)
        self.txtStyle.setObjectName(u"txtStyle")

        self.layoutStyle.addWidget(self.txtStyle)

        self.btnBrowseStyle = QPushButton(self.groupOptions)
        self.btnBrowseStyle.setObjectName(u"btnBrowseStyle")

        self.layoutStyle.addWidget(self.btnBrowseStyle)


        self.formLayout.setLayout(2, QFormLayout.ItemRole.FieldRole, self.layoutStyle)

        self.chkNewPage = QCheckBox(self.groupOptions)
        self.chkNewPage.setObjectName(u"chkNewPage")

        self.formLayout.setWidget(3, QFormLayout.ItemRole.FieldRole, self.chkNewPage)

        self.chkToc = QCheckBox(self.groupOptions)
        self.chkToc.setObjectName(u"chkToc")

        self.formLayout.setWidget(4, QFormLayout.ItemRole.FieldRole, self.chkToc)

        self.labelTocName = QLabel(self.groupOptions)
        self.labelTocName.setObjectName(u"labelTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.LabelRole, self.labelTocName)

        self.txtTocName = QLineEdit(self.groupOptions)
        self.txtTocName.setObjectName(u"txtTocName")

        self.formLayout.setWidget(5, QFormLayout.ItemRole.FieldRole, self.txtTocName)

        self.chkTitlePage = QCheckBox(self.groupOptions)
        self.chkTitlePage.setObjectName(u"chkTitlePage")

        self.formLayout.setWidget(6, QFormLayout.ItemRole.FieldRole, self.chkTitlePage)

        self.labelTitleSvg = QLabel(self.groupOptions)
        self.labelTitleSvg.setObjectName(u"labelTitleSvg")

        self.formLayout.setWidget(7, QFormLayout.ItemRole.LabelRole, self.labelTitleSvg)

        self.layoutTitleSvg = QHBoxLayout()
        self.layoutTitleSvg.setObjectName(u"layoutTitleSvg")
        self.txtTitleSvg = QLineEdit(self.groupOptions)
        self.txtTitleSvg.setObjectName(u"txtTitleSvg")

        self.layoutTitleSvg.addWidget(self.txtTitleSvg)

        self.btnBrowseTitleSvg = QPushButton(self.groupOptions)
        self.btnBrowseTitleSvg.setObjectName(u"btnBrowseTitleSvg")

        self.layoutTitleSvg.addWidget(self.btnBrowseTitleSvg)


        self.formLayout.setLayout(7, QFormLayout.ItemRole.FieldRole, self.layoutTitleSvg)


        self.verticalLayout.addWidget(self.groupOptions)

        self.btnMerge = QPushButton(self.centralwidget)
        self.btnMerge.setObjectName(u"btnMerge")

        self.verticalLayout.addWidget(self.btnMerge)

        self.labelLog = QLabel(self.centralwidget)
        self.labelLog.setObjectName(u"labelLog")

        self.verticalLayout.addWidget(self.labelLog)

        self.txtLog = QPlainTextEdit(self.centralwidget)
        self.txtLog.setObjectName(u"txtLog")
        self.txtLog.setReadOnly(True)

        self.verticalLayout.addWidget(self.txtLog)

        MergeWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QStatusBar(MergeWindow)
        self.statusbar.setObjectName(u"statusbar")
        MergeWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MergeWindow)

        QMetaObject.connectSlotsByName(MergeWindow)
    # setupUi

    def retranslateUi(self, MergeWindow):
        MergeWindow.setWindowTitle(QCoreApplication.translate("MergeWindow", u"MergeMuseScores", None))
        self.labelFiles.setText(QCoreApplication.translate("MergeWindow", u"MuseScore Files", None))
        self.btnAddFiles.setText(QCoreApplication.translate("MergeWindow", u"Add Files", None))
        self.btnRemoveFiles.setText(QCoreApplication.translate("MergeWindow", u"Remove Selected", None))
        self.btnClearFiles.setText(QCoreApplication.translate("MergeWindow", u"Clear", None))
        self.groupOptions.setTitle(QCoreApplication.translate("MergeWindow", u"Merge Options", None))
        self.labelOutputName.setText(QCoreApplication.translate("MergeWindow", u"Output Name", None))
        self.labelOutputDir.setText(QCoreApplication.translate("MergeWindow", u"Output Directory", None))
        self.btnBrowseOutputDir.setText(QCoreApplication.translate("MergeWindow", u"Browse...", None))
        self.labelStyle.setText(QCoreApplication.translate("MergeWindow", u"Style File", None))
        self.btnBrowseStyle.setText(QCoreApplication.translate("MergeWindow", u"Browse...", None))
        self.chkNewPage.setText(QCoreApplication.translate("MergeWindow", u"Insert Page Break Between Scores", None))
        self.chkToc.setText(QCoreApplication.translate("MergeWindow", u"Generate Table of Contents", None))
        self.labelTocName.setText(QCoreApplication.translate("MergeWindow", u"TOC Title", None))
        self.txtTocName.setText(QCoreApplication.translate("MergeWindow", u"Table of Contents", None))
        self.chkTitlePage.setText(QCoreApplication.translate("MergeWindow", u"Generate Title Page", None))
        self.labelTitleSvg.setText(QCoreApplication.translate("MergeWindow", u"Title Page SVG", None))
        self.btnBrowseTitleSvg.setText(QCoreApplication.translate("MergeWindow", u"Browse...", None))
        self.btnMerge.setText(QCoreApplication.translate("MergeWindow", u"Merge Scores", None))
        self.labelLog.setText(QCoreApplication.translate("MergeWindow", u"Log Output", None))
    # retranslateUi

