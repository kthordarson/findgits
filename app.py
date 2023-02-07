from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QAction, QBrush, QColor, QConicalGradient,
    QCursor, QFont, QFontDatabase, QGradient,
    QIcon, QImage, QKeySequence, QLinearGradient,
    QPainter, QPalette, QPixmap, QRadialGradient,
    QTransform)
from PySide6.QtWidgets import (QApplication, QHeaderView, QMainWindow, QMenu,
    QMenuBar, QSizePolicy, QStatusBar, QTreeWidget,
    QTreeWidgetItem, QVBoxLayout, QWidget)

class xFindGitsAppUi(object):
	def setupUi(self, Form):
		self.Form = Form
		self.Form.setObjectName("FindGitsApp")
		self.Form.resize(719, 544)
		self.treeWidget = QtWidgets.QTreeWidget(self.Form)
		self.treeWidget.setGeometry(QtCore.QRect(80, 80, 256, 192))
		self.treeWidget.setObjectName("treeWidget")
		self.item_0 = QtWidgets.QTreeWidgetItem(self.treeWidget)
		# item_0.setCheckState(0, QtCore.Qt.Unchecked)
		QtCore.QMetaObject.connectSlotsByName(self.Form)

	def retranslateUi(self):
		#_translate = QtCore.QCoreApplication.translate
		self.Form.setWindowTitle("FindGits")
		# self.treeWidget.headerItem().setText(0, _translate("Form", "header1"))
		self.treeWidget.headerItem().setText(0, "gitfolders")
		__sortingEnabled = self.treeWidget.isSortingEnabled()
		self.treeWidget.setSortingEnabled(False)
		#self.treeWidget.topLevelItem(0).setText(0, "toplitem1")
		#self.treeWidget.topLevelItem(0).child(0).setText(0,   "toplitem1ch1")
		self.treeWidget.setSortingEnabled(__sortingEnabled)

	def populate_gitfolders(self, gitfolders):
		for k in gitfolders:
			item_1 = QtWidgets.QTreeWidgetItem(self.item_0)
			# item_1.setCheckState(0, QtCore.Qt.Unchecked)
			item_1.setText(0, f"item{k}")
		self.retranslateUi()

class FindGitsAppUi(object):
	def setupUi(self, Mainwindow):
		self.Mainwindow = Mainwindow
		if not self.Mainwindow.objectName():
			self.Mainwindow.setObjectName(u"self.Mainwindow")
		self.Mainwindow.resize(800, 600)
		self.actionQuit = QtWidgets.QAction(self.Mainwindow)
		self.actionQuit.setObjectName(u"actionQuit")
		self.centralwidget = QtWidgets.QWidget(self.Mainwindow)
        #self.centralwidget.setObjectName(u"centralwidget")
        #self.verticalLayoutWidget = QWidget(self.centralwidget)
        #self.verticalLayoutWidget.setObjectName(u"verticalLayoutWidget")
        # self.verticalLayoutWidget.setGeometry(QRect(19, 9, 761, 541))
        #self.verticalLayout = QVBoxLayout(self.verticalLayoutWidget)
        #self.verticalLayout.setObjectName(u"verticalLayout")
        #self.verticalLayout.setContentsMargins(0, 0, 0, 0)
		#self.Form = self.Mainwindow
		self.Mainwindow.setObjectName("FindGitsApp")
		self.Mainwindow.resize(719, 544)
		self.treeWidget = QtWidgets.QTreeWidget(self.Mainwindow)
		self.treeWidget.setObjectName(u"treeWidget")
		self.item_0 = QtWidgets.QTreeWidgetItem(self.treeWidget)

		#self.verticalLayout.addWidget(self.treeWidget)

		# self.Mainwindow.setCentralWidget(self.centralwidget)
		self.menubar = QtWidgets.QMenuBar(self.Mainwindow)
		self.menubar.setObjectName(u"menubar")
		# self.menubar.setGeometry(QRect(0, 0, 800, 24))
		self.menuFile = QtWidgets.QMenu(self.menubar)
		self.menuFile.setObjectName(u"menuFile")
		# self.Mainwindow.setMenuBar(self.menubar)
		self.statusbar = QtWidgets.QStatusBar(self.Mainwindow)
		self.statusbar.setObjectName(u"statusbar")
		# self.Mainwindow.setStatusBar(self.statusbar)

		self.menubar.addAction(self.menuFile.menuAction())
		self.menuFile.addAction(self.actionQuit)

		self.retranslateUi()

		#QtWidgets.QMetaObject.connectSlotsByName(self.Mainwindow)
	# setupUi

	def retranslateUi(self):
		self.Mainwindow.setWindowTitle(QCoreApplication.translate("self.Mainwindow", u"self.Mainwindow", None))
		self.actionQuit.setText(QCoreApplication.translate("self.Mainwindow", u"Quit", None))
		___qtreewidgetitem = self.treeWidget.headerItem()
		___qtreewidgetitem.setText(0, QCoreApplication.translate("self.Mainwindow", u"gitfolders", None));
		self.menuFile.setTitle(QCoreApplication.translate("self.Mainwindow", u"File", None))
	# retranslateUi

	def populate_gitfolders(self, gitfolders):
		for k in gitfolders:
			item_1 = QtWidgets.QTreeWidgetItem(self.item_0)
			# item_1.setCheckState(0, QtCore.Qt.Unchecked)
			item_1.setText(0, f"item{k}")
		self.retranslateUi()
