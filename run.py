import os, sys
from PyQt5.QtCore import QCoreApplication
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QApplication

from app import FindGitsApp, Ui_Form

class graphAnalysis(QDialog, Ui_Form):
    def __init__(self):
        super(graphAnalysis, self).__init__()
        self.setupUi(self)
        # 点击父节点
        self.treeWidget.itemChanged.connect(self.handleChanged)

    def handleChanged(self, item, column):
        count = item.childCount()
        if item.checkState(column) == Qt.Checked:
            for index in range(count):
                item.child(index).setCheckState(0, Qt.Checked)
        if item.checkState(column) == Qt.Unchecked:
            for index in range(count):
                item.child(index).setCheckState(0, Qt.Unchecked)

def xmain():
	app = QApplication()
	fgapp = FindGitsApp()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = graphAnalysis()
    w.show()
    sys.exit(app.exec_())
