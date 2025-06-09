# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'mainwindowmtLyWy.ui'
##
## Created by: Qt User Interface Compiler version 6.6.2
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QApplication, QFormLayout, QHBoxLayout, QHeaderView,
    QLabel, QLineEdit, QMainWindow, QMenu,
    QMenuBar, QPushButton, QSizePolicy, QStatusBar,
    QTreeWidget, QTreeWidgetItem, QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1183, 704)
        self.actionOpen = QAction(MainWindow)
        self.actionOpen.setObjectName(u"actionOpen")
        self.actionExit = QAction(MainWindow)
        self.actionExit.setObjectName(u"actionExit")
        self.actionRescan_all = QAction(MainWindow)
        self.actionRescan_all.setObjectName(u"actionRescan_all")
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.infowidget = QWidget(self.centralwidget)
        self.infowidget.setObjectName(u"infowidget")
        self.infowidget.setGeometry(QRect(930, 30, 191, 81))
        self.formLayoutWidget_3 = QWidget(self.infowidget)
        self.formLayoutWidget_3.setObjectName(u"formLayoutWidget_3")
        self.formLayoutWidget_3.setGeometry(QRect(10, 10, 151, 32))
        self.formLayout = QFormLayout(self.formLayoutWidget_3)
        self.formLayout.setObjectName(u"formLayout")
        self.formLayout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self.formLayout.setLabelAlignment(Qt.AlignCenter)
        self.formLayout.setFormAlignment(Qt.AlignJustify|Qt.AlignTop)  # type: ignore
        self.formLayout.setContentsMargins(0, 3, 3, 3)
        self.idLabel = QLabel(self.formLayoutWidget_3)
        self.idLabel.setObjectName(u"idLabel")

        self.formLayout.setWidget(0, QFormLayout.LabelRole, self.idLabel)

        self.idLineEdit = QLineEdit(self.formLayoutWidget_3)
        self.idLineEdit.setObjectName(u"idLineEdit")

        self.formLayout.setWidget(0, QFormLayout.FieldRole, self.idLineEdit)

        self.layoutWidget = QWidget(self.centralwidget)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.layoutWidget.setGeometry(QRect(10, 600, 491, 40))
        self.horizontalLayout = QHBoxLayout(self.layoutWidget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(7, 7, 7, 7)
        self.folderButton = QPushButton(self.layoutWidget)
        self.folderButton.setObjectName(u"folderButton")

        self.horizontalLayout.addWidget(self.folderButton)

        self.getdupes_button = QPushButton(self.layoutWidget)
        self.getdupes_button.setObjectName(u"getdupes_button")
        self.getdupes_button.setEnabled(True)

        self.horizontalLayout.addWidget(self.getdupes_button)

        self.searchpaths_button = QPushButton(self.layoutWidget)
        self.searchpaths_button.setObjectName(u"searchpaths_button")
        self.searchpaths_button.setEnabled(False)

        self.horizontalLayout.addWidget(self.searchpaths_button)

        self.runscan_button = QPushButton(self.layoutWidget)
        self.runscan_button.setObjectName(u"runscan_button")
        self.runscan_button.setEnabled(False)

        self.horizontalLayout.addWidget(self.runscan_button)

        self.pushButton_5 = QPushButton(self.layoutWidget)
        self.pushButton_5.setObjectName(u"pushButton_5")
        self.pushButton_5.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_5)

        self.dupe_paths_widget = QTreeWidget(self.centralwidget)
        __qtreewidgetitem = QTreeWidgetItem()
        __qtreewidgetitem.setText(0, u"1")
        self.dupe_paths_widget.setHeaderItem(__qtreewidgetitem)
        self.dupe_paths_widget.setObjectName(u"dupe_paths_widget")
        self.dupe_paths_widget.setGeometry(QRect(880, 120, 241, 121))
        self.repotree = QTreeWidget(self.centralwidget)
        __qtreewidgetitem1 = QTreeWidgetItem()
        __qtreewidgetitem1.setText(0, u"1")
        self.repotree.setHeaderItem(__qtreewidgetitem1)
        self.repotree.setObjectName(u"repotree")
        self.repotree.setGeometry(QRect(10, 10, 861, 581))
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setObjectName(u"menubar")
        self.menubar.setGeometry(QRect(0, 0, 1183, 23))
        self.menuFile = QMenu(self.menubar)
        self.menuFile.setObjectName(u"menuFile")
        self.menuScan = QMenu(self.menubar)
        self.menuScan.setObjectName(u"menuScan")
        self.menuDatabase = QMenu(self.menubar)
        self.menuDatabase.setObjectName(u"menuDatabase")
        self.menuTools = QMenu(self.menubar)
        self.menuTools.setObjectName(u"menuTools")
        self.menuAbout = QMenu(self.menubar)
        self.menuAbout.setObjectName(u"menuAbout")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.setObjectName(u"statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.menubar.addAction(self.menuFile.menuAction())
        self.menubar.addAction(self.menuScan.menuAction())
        self.menubar.addAction(self.menuDatabase.menuAction())
        self.menubar.addAction(self.menuTools.menuAction())
        self.menubar.addAction(self.menuAbout.menuAction())
        self.menuFile.addAction(self.actionOpen)
        self.menuFile.addSeparator()
        self.menuFile.addAction(self.actionExit)
        self.menuScan.addAction(self.actionRescan_all)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"MainWindow", None))
        self.actionOpen.setText(QCoreApplication.translate("MainWindow", u"Open", None))
        self.actionExit.setText(QCoreApplication.translate("MainWindow", u"Exit", None))
        self.actionRescan_all.setText(QCoreApplication.translate("MainWindow", u"Rescan all", None))
        self.idLabel.setText(QCoreApplication.translate("MainWindow", u"id", None))
        self.folderButton.setText(QCoreApplication.translate("MainWindow", u"Folders", None))
        self.getdupes_button.setText(QCoreApplication.translate("MainWindow", u"Dupes", None))
        self.searchpaths_button.setText(QCoreApplication.translate("MainWindow", u"Searchpaths", None))
        self.runscan_button.setText(QCoreApplication.translate("MainWindow", u"Run scan", None))
        self.pushButton_5.setText(QCoreApplication.translate("MainWindow", u"btn-four", None))
        self.menuFile.setTitle(QCoreApplication.translate("MainWindow", u"File", None))
        self.menuScan.setTitle(QCoreApplication.translate("MainWindow", u"Scan", None))
        self.menuDatabase.setTitle(QCoreApplication.translate("MainWindow", u"Database", None))
        self.menuTools.setTitle(QCoreApplication.translate("MainWindow", u"Tools", None))
        self.menuAbout.setTitle(QCoreApplication.translate("MainWindow", u"About", None))
    # retranslateUi

