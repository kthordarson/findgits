from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_Form(object):
	def setupUi(self, Form):
		Form.setObjectName("Form")
		Form.resize(719, 544)
		self.treeWidget = QtWidgets.QTreeWidget(Form)
		self.treeWidget.setGeometry(QtCore.QRect(80, 80, 256, 192))
		self.treeWidget.setObjectName("treeWidget")
		item_0 = QtWidgets.QTreeWidgetItem(self.treeWidget)
		item_0.setCheckState(0, QtCore.Qt.Unchecked)
		item_1 = QtWidgets.QTreeWidgetItem(item_0)
		item_1.setCheckState(0, QtCore.Qt.Unchecked)
		item_1 = QtWidgets.QTreeWidgetItem(item_0)
		item_1.setCheckState(0, QtCore.Qt.Unchecked)
		item_1 = QtWidgets.QTreeWidgetItem(item_0)
		item_1.setCheckState(0, QtCore.Qt.Unchecked)
		item_1 = QtWidgets.QTreeWidgetItem(item_0)
		item_1.setCheckState(0, QtCore.Qt.Unchecked)
		item_1 = QtWidgets.QTreeWidgetItem(item_0)
		item_1.setCheckState(0, QtCore.Qt.Unchecked)
		self.retranslateUi(Form)
		QtCore.QMetaObject.connectSlotsByName(Form)

	def retranslateUi(self, Form):
		_translate = QtCore.QCoreApplication.translate
		Form.setWindowTitle(_translate("Form", "Form"))
		self.treeWidget.headerItem().setText(0, _translate("Form", "header1"))
		__sortingEnabled = self.treeWidget.isSortingEnabled()
		self.treeWidget.setSortingEnabled(False)
		self.treeWidget.topLevelItem(0).setText(0, _translate("Form", "toplitem1"))
		# self.treeWidget.topLevelItem(0).child(0).setText(0, _translate("Form", "toplitem1ch1"))
		self.treeWidget.setSortingEnabled(__sortingEnabled)


class FindGitsApp():
	pass