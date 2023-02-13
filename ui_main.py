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
from PySide6.QtWidgets import (QAbstractScrollArea, QApplication, QFormLayout, QHBoxLayout,
    QHeaderView, QLabel, QLineEdit, QPushButton,
    QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget)

class Ui_FindGitsApp(object):
    def setupUi(self, FindGitsApp):
        if not FindGitsApp.objectName():
            FindGitsApp.setObjectName(u"FindGitsApp")
        FindGitsApp.resize(1036, 464)
        self.actionactionone = QAction(FindGitsApp)
        self.actionactionone.setObjectName(u"actionactionone")
        self.actionactionone.setCheckable(True)
        self.treeWidget = QTreeWidget(FindGitsApp)
        self.treeWidget.setObjectName(u"treeWidget")
        self.treeWidget.setGeometry(QRect(10, 10, 500, 400))
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.treeWidget.sizePolicy().hasHeightForWidth())
        self.treeWidget.setSizePolicy(sizePolicy)
        self.treeWidget.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        self.treeWidget.setSortingEnabled(True)
        self.formLayoutWidget = QWidget(FindGitsApp)
        self.formLayoutWidget.setObjectName(u"formLayoutWidget")
        self.formLayoutWidget.setGeometry(QRect(510, 20, 511, 99))
        self.formLayout = QFormLayout(self.formLayoutWidget)
        self.formLayout.setObjectName(u"formLayout")
        self.formLayout.setContentsMargins(5, 0, 0, 0)
        self.foobarLabel = QLabel(self.formLayoutWidget)
        self.foobarLabel.setObjectName(u"foobarLabel")

        self.formLayout.setWidget(3, QFormLayout.LabelRole, self.foobarLabel)

        self.urlLineEdit = QLineEdit(self.formLayoutWidget)
        self.urlLineEdit.setObjectName(u"urlLineEdit")
        self.urlLineEdit.setReadOnly(True)

        self.formLayout.setWidget(3, QFormLayout.FieldRole, self.urlLineEdit)

        self.idLabel = QLabel(self.formLayoutWidget)
        self.idLabel.setObjectName(u"idLabel")

        self.formLayout.setWidget(1, QFormLayout.LabelRole, self.idLabel)

        self.idLineEdit = QLineEdit(self.formLayoutWidget)
        self.idLineEdit.setObjectName(u"idLineEdit")
        sizePolicy1 = QSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.idLineEdit.sizePolicy().hasHeightForWidth())
        self.idLineEdit.setSizePolicy(sizePolicy1)
        self.idLineEdit.setMaximumSize(QSize(64, 16777215))
        self.idLineEdit.setBaseSize(QSize(33, 0))
        self.idLineEdit.setMaxLength(4)
        self.idLineEdit.setAlignment(Qt.AlignCenter)
        self.idLineEdit.setReadOnly(True)

        self.formLayout.setWidget(1, QFormLayout.FieldRole, self.idLineEdit)

        self.layoutWidget = QWidget(FindGitsApp)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.layoutWidget.setGeometry(QRect(10, 420, 491, 29))
        self.horizontalLayout = QHBoxLayout(self.layoutWidget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.pushButton = QPushButton(self.layoutWidget)
        self.pushButton.setObjectName(u"pushButton")

        self.horizontalLayout.addWidget(self.pushButton)

        self.pushButton_2 = QPushButton(self.layoutWidget)
        self.pushButton_2.setObjectName(u"pushButton_2")
        self.pushButton_2.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_2)

        self.pushButton_3 = QPushButton(self.layoutWidget)
        self.pushButton_3.setObjectName(u"pushButton_3")
        self.pushButton_3.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_3)

        self.pushButton_4 = QPushButton(self.layoutWidget)
        self.pushButton_4.setObjectName(u"pushButton_4")
        self.pushButton_4.setEnabled(False)

        self.horizontalLayout.addWidget(self.pushButton_4)

        self.pushButton_5 = QPushButton(self.layoutWidget)
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
        ___qtreewidgetitem.setText(1, QCoreApplication.translate("FindGitsApp", u"path", None));
        ___qtreewidgetitem.setText(0, QCoreApplication.translate("FindGitsApp", u"id", None));
        self.foobarLabel.setText(QCoreApplication.translate("FindGitsApp", u"url", None))
        self.idLabel.setText(QCoreApplication.translate("FindGitsApp", u"id", None))
        self.pushButton.setText(QCoreApplication.translate("FindGitsApp", u"Folders", None))
        self.pushButton_2.setText(QCoreApplication.translate("FindGitsApp", u"btn-one", None))
        self.pushButton_3.setText(QCoreApplication.translate("FindGitsApp", u"btn-two", None))
        self.pushButton_4.setText(QCoreApplication.translate("FindGitsApp", u"btn-three", None))
        self.pushButton_5.setText(QCoreApplication.translate("FindGitsApp", u"btn-four", None))
    # retranslateUi

