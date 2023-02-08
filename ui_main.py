# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'main.ui'
##
## Created by: Qt User Interface Compiler version 6.4.2
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
from PySide6.QtWidgets import (QAbstractScrollArea, QApplication, QFormLayout, QGroupBox,
    QHBoxLayout, QHeaderView, QListView, QPushButton,
    QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget)

class Ui_FindGitsApp(object):
    def setupUi(self, FindGitsApp):
        if not FindGitsApp.objectName():
            FindGitsApp.setObjectName(u"FindGitsApp")
        FindGitsApp.resize(1036, 464)
        self.treeWidget = QTreeWidget(FindGitsApp)
        __qtreewidgetitem = QTreeWidgetItem()
        __qtreewidgetitem.setText(0, u"id");
        self.treeWidget.setHeaderItem(__qtreewidgetitem)
        self.treeWidget.setObjectName(u"treeWidget")
        self.treeWidget.setGeometry(QRect(10, 10, 491, 391))
        self.treeWidget.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.groupBox = QGroupBox(FindGitsApp)
        self.groupBox.setObjectName(u"groupBox")
        self.groupBox.setGeometry(QRect(520, 10, 501, 391))
        self.formLayoutWidget = QWidget(self.groupBox)
        self.formLayoutWidget.setObjectName(u"formLayoutWidget")
        self.formLayoutWidget.setGeometry(QRect(19, 39, 461, 331))
        self.formLayout = QFormLayout(self.formLayoutWidget)
        self.formLayout.setObjectName(u"formLayout")
        self.formLayout.setContentsMargins(0, 0, 0, 0)
        self.listView = QListView(self.formLayoutWidget)
        self.listView.setObjectName(u"listView")

        self.formLayout.setWidget(0, QFormLayout.LabelRole, self.listView)

        self.widget = QWidget(FindGitsApp)
        self.widget.setObjectName(u"widget")
        self.widget.setGeometry(QRect(10, 410, 426, 29))
        self.horizontalLayout = QHBoxLayout(self.widget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.pushButton = QPushButton(self.widget)
        self.pushButton.setObjectName(u"pushButton")

        self.horizontalLayout.addWidget(self.pushButton)

        self.pushButton_2 = QPushButton(self.widget)
        self.pushButton_2.setObjectName(u"pushButton_2")

        self.horizontalLayout.addWidget(self.pushButton_2)

        self.pushButton_3 = QPushButton(self.widget)
        self.pushButton_3.setObjectName(u"pushButton_3")

        self.horizontalLayout.addWidget(self.pushButton_3)

        self.pushButton_4 = QPushButton(self.widget)
        self.pushButton_4.setObjectName(u"pushButton_4")

        self.horizontalLayout.addWidget(self.pushButton_4)

        self.pushButton_5 = QPushButton(self.widget)
        self.pushButton_5.setObjectName(u"pushButton_5")

        self.horizontalLayout.addWidget(self.pushButton_5)


        self.retranslateUi(FindGitsApp)

        QMetaObject.connectSlotsByName(FindGitsApp)
    # setupUi

    def retranslateUi(self, FindGitsApp):
        FindGitsApp.setWindowTitle(QCoreApplication.translate("FindGitsApp", u"Form", None))
        ___qtreewidgetitem = self.treeWidget.headerItem()
        ___qtreewidgetitem.setText(1, QCoreApplication.translate("FindGitsApp", u"url", None));
        self.groupBox.setTitle(QCoreApplication.translate("FindGitsApp", u"GroupBox", None))
        self.pushButton.setText(QCoreApplication.translate("FindGitsApp", u"populate", None))
        self.pushButton_2.setText(QCoreApplication.translate("FindGitsApp", u"populate", None))
        self.pushButton_3.setText(QCoreApplication.translate("FindGitsApp", u"populate", None))
        self.pushButton_4.setText(QCoreApplication.translate("FindGitsApp", u"populate", None))
        self.pushButton_5.setText(QCoreApplication.translate("FindGitsApp", u"populate", None))
    # retranslateUi

