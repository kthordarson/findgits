#!/usr/bin/python3

import os, sys
from loguru import logger
from ui_main import Ui_FindGitsApp
from dbstuff import GitRepo, GitFolder, GitParentPath, get_engine, get_dupes
from utils import get_folder_list

from sqlalchemy.orm import sessionmaker
from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform, QStandardItemModel, QStandardItem)
# from PySide6.QtWidgets import (QApplication, QSizePolicy, QWidget)
from PySide6.QtWidgets import (QApplication, QHeaderView, QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget, QListWidgetItem, QTableWidgetItem)

class MainApp(QWidget, Ui_FindGitsApp):
    def __init__(self, session):
        self.session = session
        super(MainApp, self).__init__()
        self.setupUi(self)
        #self.treeWidget = QTreeWidget(self)
        # self.treeWidget.itemChanged.connect(self.handleChanged)
        self.treeWidget.itemClicked.connect(self.tree_item_clicked)
        self.folderButton.clicked.connect(self.folderButton_clicked)
        self.getdupes_button.clicked.connect(self.getdupes_button_clicked)
        # self.checkBox_showdupes.stateChanged.connect(self.checkBox_showdupes_changed)
        self.checkBox_showdupes.toggled.connect(self.checkBox_showdupes_toggle)
        self.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
        #self.tableWidget.setColumnCount(2)
        #self.tableWidget.setHorizontalHeaderLabels(['id', 'giturl'])
        self.gitrepos = session.query(GitRepo).all()
        self.gitfolders = session.query(GitFolder).all()
        self.parent_folders = session.query(GitParentPath).all()
        self.dupes = get_dupes(self.session)
        #__qtreewidgetitem = self.treeWidget.headerItem()
        #__qtreewidgetitem.setText(0, u"id")
        #__qtreewidgetitem.setText(1, u"path")
        #self.treeWidget.setHeaderItem(__qtreewidgetitem)
        logger.debug(f'[init] gr={len(self.gitrepos)} gf={len(self.gitfolders)} pf={len(self.parent_folders)}')

    # def checkBox_showdupes_changed(self, state):
    #     logger.debug(f'[showdupes] {state} {type(state)} checkState:{self.checkBox_showdupes.checkState()} isChecked:{self.checkBox_showdupes.isChecked()}')
    #     self.retranslateUi(self)

    def searchpaths_button_clicked(self, widget):
        pass
    def checkBox_showdupes_toggle(self, *args):
        logger.debug(f'[showdupes] toggle checkState:{self.checkBox_showdupes.checkState()} isChecked:{self.checkBox_showdupes.isChecked()} args={args} {type(args)}')
        # self.retranslateUi(self)

    def getdupes_button_clicked(self, widget):
        self.checkBox_showdupes.setEnabled(False)
        self.treeWidget.clear()
        self.treeWidget.setColumnCount(3)
        self.treeWidget.headerItem().setText(0, "id")
        self.treeWidget.headerItem().setText(1, "count")
        self.treeWidget.headerItem().setText(2, "giturl")
        for d in self.dupes:
            item0 = QTreeWidgetItem(self.treeWidget)
            item0.setText(0, f"{d.get('gitid')}")
            item0.setText(1, f"{d.get('count')}")
            item0.setText(2, f"{d.get('giturl')}")
            for f in d.get('folders'):
                item1 = QTreeWidgetItem(item0)
                item1.setText(0, f"{f.get('folderid')}")
                item1.setText(2, f"{f.get('git_path')}")
        #self.retranslateUi(self)

    def tree_item_clicked(self, widget):
        try:
            repo = [k for k in self.gitrepos if k.git_path==widget.text(1)][0]
        except IndexError as e:
            #logger.error(f'[tic] indexerror {e} widget={widget.text(0)} {widget.text(1)}')
            return
        #repoid_item = QTableWidgetItem(f'{repo.id}') #}\nurl: {repo.giturl}\nbranch: {repo.branch}\npath: {repo.git_path}')
        self.idLineEdit.setText(f'{repo.id}')
        self.urlLineEdit.setText(f'{repo.giturl}')
        # repourl_item = QTableWidgetItem(f'{repo.giturl}')
        # row = self.tableWidget.rowCount()
        # self.tableWidget.insertRow(row)
        # self.tableWidget.setItem(row,0,repoid_item)
        # self.tableWidget.setItem(row,1,repourl_item)
        # self.tableWidget.resizeColumnsToContents()
        logger.debug(f'[h] {self} id: {widget.text(0)} path: {widget.text(1)} repo={repo.giturl}')

    def populate_gitrepos(self):
        self.treeWidget.clear()
        for k in self.gitrepos:
            item_1 = QTreeWidgetItem(self.treeWidget)
            item_1.setText(0, f"{k.id}")
            item_1.setText(1, f"{k.giturl}")
        self.retranslateUi(self)

    def folderButton_clicked(self, widget):
        self.checkBox_showdupes.setEnabled(True)
        self.treeWidget.clear()
        self.treeWidget.setColumnCount(2)
        self.treeWidget.headerItem().setText(0, "id")
        self.treeWidget.headerItem().setText(1, "path")
        for p in self.parent_folders:
            item0 = QTreeWidgetItem(self.treeWidget)
            item0.setText(0, f"{p.id}")
            item0.setText(1, f"{p.folder}")
            gitfolders = session.query(GitFolder).filter(GitFolder.parent_id == p.id).all()
            for k in gitfolders:
                item_1 = QTreeWidgetItem(item0)
                item_1.setText(0, f"{k.id}")
                item_1.setText(1, f"{k.git_path}")
        #self.treeWidget.view.setResizeMode(QHeaderView.ResizeToContents)
        #self.treeWidget.header().setStretchLastSection(False)
        #self.retranslateUi(self)

    # def handleChanged(self, item, column):
    #     count = item.childCount()
    #     # logger.debug(f'[h] {self} count={count} i: {item} c: {column}')
    #     if item.checkState(column) == Qt.Checked:
    #         for index in range(count):
    #             item.child(index).setCheckState(0, Qt.Checked)
    #     if item.checkState(column) == Qt.Unchecked:
    #         for index in range(count):
    #             item.child(index).setCheckState(0, Qt.Unchecked)

if __name__ == '__main__':
    engine = get_engine(dbtype='sqlite')
    Session = sessionmaker(bind=engine)
    session = Session()
    app = QApplication(sys.argv)
    w = MainApp(session)
    w.show()
    sys.exit(app.exec())
