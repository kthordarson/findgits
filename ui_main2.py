# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'mainMmTBIu.ui'
##
## Created by: Qt User Interface Compiler version 6.5.2
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
from PySide6.QtWidgets import (QAbstractScrollArea, QApplication, QHBoxLayout, QHeaderView,
    QPushButton, QSizePolicy, QTableWidget, QTableWidgetItem,
    QTreeWidget, QTreeWidgetItem, QWidget)

class Ui_FindGitsApp(object):
    def setupUi(self, FindGitsApp):
        if not FindGitsApp.objectName():
            FindGitsApp.setObjectName(u"FindGitsApp")
        FindGitsApp.resize(1066, 464)
        self.actionactionone = QAction(FindGitsApp)
        self.actionactionone.setObjectName(u"actionactionone")
        self.actionactionone.setCheckable(True)
        self.repotree = QTreeWidget(FindGitsApp)
        self.repotree.setObjectName(u"repotree")
        self.repotree.setGeometry(QRect(10, 10, 591, 400))
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.repotree.sizePolicy().hasHeightForWidth())
        self.repotree.setSizePolicy(sizePolicy)
        self.repotree.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.repotree.setSortingEnabled(True)
        self.layoutWidget = QWidget(FindGitsApp)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.layoutWidget.setGeometry(QRect(50, 420, 491, 29))
        self.horizontalLayout = QHBoxLayout(self.layoutWidget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
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

        self.gitshow_button = QPushButton(FindGitsApp)
        self.gitshow_button.setObjectName(u"gitshow_button")
        self.gitshow_button.setGeometry(QRect(760, 420, 94, 27))
        self.gitstatus_button = QPushButton(FindGitsApp)
        self.gitstatus_button.setObjectName(u"gitstatus_button")
        self.gitstatus_button.setGeometry(QRect(860, 420, 94, 27))
        self.gitlog_button = QPushButton(FindGitsApp)
        self.gitlog_button.setObjectName(u"gitlog_button")
        self.gitlog_button.setGeometry(QRect(960, 420, 94, 27))
        self.infowidget = QWidget(FindGitsApp)
        self.infowidget.setObjectName(u"infowidget")
        self.infowidget.setGeometry(QRect(629, 29, 401, 371))
        self.infotablewidget = QTableWidget(self.infowidget)
        self.infotablewidget.setObjectName(u"infotablewidget")
        self.infotablewidget.setGeometry(QRect(15, 11, 371, 351))

        self.retranslateUi(FindGitsApp)

        QMetaObject.connectSlotsByName(FindGitsApp)
    # setupUi

    def retranslateUi(self, FindGitsApp):
        FindGitsApp.setWindowTitle(QCoreApplication.translate("FindGitsApp", u"Form", None))
        self.actionactionone.setText(QCoreApplication.translate("FindGitsApp", u"actionone", None))
#if QT_CONFIG(tooltip)
        self.actionactionone.setToolTip(QCoreApplication.translate("FindGitsApp", u"actiononetooltip", None))
#endif // QT_CONFIG(tooltip)
        self.folderButton.setText(QCoreApplication.translate("FindGitsApp", u"Folders", None))
        self.getdupes_button.setText(QCoreApplication.translate("FindGitsApp", u"Dupes", None))
        self.searchpaths_button.setText(QCoreApplication.translate("FindGitsApp", u"Searchpaths", None))
        self.runscan_button.setText(QCoreApplication.translate("FindGitsApp", u"Run scan", None))
        self.pushButton_5.setText(QCoreApplication.translate("FindGitsApp", u"btn-four", None))
        self.gitshow_button.setText(QCoreApplication.translate("FindGitsApp", u"Git show", None))
        self.gitstatus_button.setText(QCoreApplication.translate("FindGitsApp", u"Git status", None))
        self.gitlog_button.setText(QCoreApplication.translate("FindGitsApp", u"Git log", None))
    # retranslateUi

