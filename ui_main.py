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
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QAbstractScrollArea, QApplication, QFormLayout, QFrame,
    QHBoxLayout, QHeaderView, QLabel, QPushButton,
    QSizePolicy, QTableWidget, QTableWidgetItem, QTreeWidget,
    QTreeWidgetItem, QWidget)

class Ui_FindGitsApp(object):
    def setupUi(self, FindGitsApp):
        if not FindGitsApp.objectName():
            FindGitsApp.setObjectName(u"FindGitsApp")
        FindGitsApp.resize(1036, 464)
        self.actionactionone = QAction(FindGitsApp)
        self.actionactionone.setObjectName(u"actionactionone")
        self.actionactionone.setCheckable(True)
        self.treeWidget = QTreeWidget(FindGitsApp)
        __qtreewidgetitem = QTreeWidgetItem()
        __qtreewidgetitem.setText(0, u"id");
        self.treeWidget.setHeaderItem(__qtreewidgetitem)
        self.treeWidget.setObjectName(u"treeWidget")
        self.treeWidget.setGeometry(QRect(10, 10, 500, 400))
        self.treeWidget.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.treeWidget.setSortingEnabled(True)
        self.tableWidget = QTableWidget(FindGitsApp)
        self.tableWidget.setObjectName(u"tableWidget")
        self.tableWidget.setGeometry(QRect(520, 241, 491, 171))
        self.formLayoutWidget = QWidget(FindGitsApp)
        self.formLayoutWidget.setObjectName(u"formLayoutWidget")
        self.formLayoutWidget.setGeometry(QRect(510, 20, 511, 61))
        self.formLayout = QFormLayout(self.formLayoutWidget)
        self.formLayout.setObjectName(u"formLayout")
        self.formLayout.setContentsMargins(0, 0, 0, 0)
        self.label_3 = QLabel(self.formLayoutWidget)
        self.label_3.setObjectName(u"label_3")

        self.formLayout.setWidget(0, QFormLayout.LabelRole, self.label_3)

        self.label_repoid = QLabel(self.formLayoutWidget)
        self.label_repoid.setObjectName(u"label_repoid")
        self.label_repoid.setEnabled(True)
        self.label_repoid.setFrameShape(QFrame.NoFrame)

        self.formLayout.setWidget(0, QFormLayout.FieldRole, self.label_repoid)

        self.label_2 = QLabel(self.formLayoutWidget)
        self.label_2.setObjectName(u"label_2")

        self.formLayout.setWidget(1, QFormLayout.LabelRole, self.label_2)

        self.label_repourl = QLabel(self.formLayoutWidget)
        self.label_repourl.setObjectName(u"label_repourl")
        self.label_repourl.setFrameShape(QFrame.NoFrame)

        self.formLayout.setWidget(1, QFormLayout.FieldRole, self.label_repourl)

        self.widget = QWidget(FindGitsApp)
        self.widget.setObjectName(u"widget")
        self.widget.setGeometry(QRect(10, 420, 491, 29))
        self.horizontalLayout = QHBoxLayout(self.widget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.pushButton = QPushButton(self.widget)
        self.pushButton.setObjectName(u"pushButton")

        self.horizontalLayout.addWidget(self.pushButton)

        self.pushButton_2 = QPushButton(self.widget)
        self.pushButton_2.setObjectName(u"pushButton_2")
        self.pushButton_2.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_2)

        self.pushButton_3 = QPushButton(self.widget)
        self.pushButton_3.setObjectName(u"pushButton_3")
        self.pushButton_3.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_3)

        self.pushButton_4 = QPushButton(self.widget)
        self.pushButton_4.setObjectName(u"pushButton_4")
        self.pushButton_4.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_4)

        self.pushButton_5 = QPushButton(self.widget)
        self.pushButton_5.setObjectName(u"pushButton_5")
        self.pushButton_5.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_5)


        self.retranslateUi(FindGitsApp)

        QMetaObject.connectSlotsByName(FindGitsApp)
    # setupUi

    def retranslateUi(self, FindGitsApp):
        FindGitsApp.setWindowTitle(QCoreApplication.translate("FindGitsApp", u"Form", None))
        self.actionactionone.setText(QCoreApplication.translate("FindGitsApp", u"actionone", None))
#if QT_CONFIG(tooltip)
        self.actionactionone.setToolTip(QCoreApplication.translate("FindGitsApp", u"actiononetooltip", None))
#endif // QT_CONFIG(tooltip)
        ___qtreewidgetitem = self.treeWidget.headerItem()
        ___qtreewidgetitem.setText(1, QCoreApplication.translate("FindGitsApp", u"git_path", None));
        self.label_3.setText(QCoreApplication.translate("FindGitsApp", u"repoid", None))
        self.label_repoid.setText("")
        self.label_2.setText(QCoreApplication.translate("FindGitsApp", u"repourl", None))
        self.label_repourl.setText("")
        self.pushButton.setText(QCoreApplication.translate("FindGitsApp", u"Folders", None))
        self.pushButton_2.setText(QCoreApplication.translate("FindGitsApp", u"btn-one", None))
        self.pushButton_3.setText(QCoreApplication.translate("FindGitsApp", u"btn-two", None))
        self.pushButton_4.setText(QCoreApplication.translate("FindGitsApp", u"btn-three", None))
        self.pushButton_5.setText(QCoreApplication.translate("FindGitsApp", u"btn-four", None))
    # retranslateUi

