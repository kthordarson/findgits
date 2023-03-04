#!/usr/bin/python3

import os, sys
from loguru import logger
from ui_main import Ui_FindGitsApp
from dbstuff import GitRepo, GitFolder, GitParentPath, get_engine, get_dupes
from sqlalchemy import and_, text
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
        self.treeWidget.itemClicked.connect(self.tree_item_clicked)
        self.folderButton.clicked.connect(self.folderButton_clicked)
        self.getdupes_button.clicked.connect(self.getdupes_button_clicked)
        self.checkBox_filterdupes.toggled.connect(self.checkBox_filterdupes_toggle)
        self.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
        self.gitshow_button.clicked.connect(self.gitshow_button_clicked)
        self.gitlog_button.clicked.connect(self.gitlog_button_clicked)
        self.gitstatus_button.clicked.connect(self.gitstatus_button_clicked)


        self.dupefilter = False

    def gitshow_button_clicked(self, widget):
        pass

    def gitlog_button_clicked(self, widget):
        pass

    def gitstatus_button_clicked(self, widget):
        pass

    def searchpaths_button_clicked(self, widget):
        pass

    def checkBox_filterdupes_toggle(self, *args):
        self.dupefilter = self.checkBox_filterdupes.isChecked()
        self.folderButton_clicked(self)

    def getdupes_button_clicked(self, widget):
        self.checkBox_filterdupes.setEnabled(False)
        self.treeWidget.clear()
        self.treeWidget.setColumnCount(3)
        self.treeWidget.headerItem().setText(0, "id")
        self.treeWidget.headerItem().setText(1, "count")
        self.treeWidget.headerItem().setText(2, "giturl")
        dupes = get_dupes(self.session)
        for d in dupes:
            item0 = QTreeWidgetItem(self.treeWidget)
            item0.setText(0, f"{d.id}")
            item0.setText(1, f"{d.count}")
            item0.setText(2, f"{d.giturl}")
            # try:
            #     for f in d.get('folders'):
            #         item1 = QTreeWidgetItem(item0)
            #         item1.setText(0, f"{f.get('gitfolder_id')}")
            #         item1.setText(2, f"{f.get('git_path')}")
            # except TypeError as e:
            #     logger.error(e)
        #self.retranslateUi(self)

    def tree_item_clicked(self, widget):
        try:
            repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
        except IndexError as e:
            logger.error(f'[tic] indexerror {e} widget={widget.text(0)} {widget.text(1)}')
            return
        self.idLineEdit.setText(f'{repo.id}')
        self.urlLineEdit.setText(f'{repo.giturl}')
        self.dupecountlabel.setText(f'Dupes: {repo.dupe_count}')
        itemdupes = self.session.query(GitRepo).filter(GitRepo.giturl == repo.giturl).all()
        folderdupes = [session.query(GitFolder).filter(GitFolder.id == d.gitfolder_id).first() for d in itemdupes]
        self.dupetree.clear()
        for f in folderdupes:
            item1 = QTreeWidgetItem(self.dupetree)
            item1.setText(0, f"{f.git_path}")

    def populate_gitrepos(self):
        self.treeWidget.clear()
        gitrepos = session.query(GitRepo).all()
        for k in gitrepos:
            item_1 = QTreeWidgetItem(self.treeWidget)
            item_1.setText(0, f"{k.id}")
            item_1.setText(1, f"{k.giturl}")
        self.retranslateUi(self)

    def folderButton_clicked(self, widget):
        self.checkBox_filterdupes.setEnabled(True)
        self.treeWidget.clear()
        self.treeWidget.setColumnCount(3)
        self.treeWidget.headerItem().setText(0, "id")
        self.treeWidget.headerItem().setText(1, "path")
        self.treeWidget.headerItem().setText(2, "dupes")
        if self.dupefilter:
            gitfolders = session.query(GitFolder).filter(GitFolder.dupe_flag == self.dupefilter).all()
        else:
            gitfolders = session.query(GitFolder).all()
        for k in gitfolders:
            item_1 = QTreeWidgetItem(self.treeWidget)
            item_1.setText(0, f"{k.id}")
            item_1.setText(1, f"{k.git_path}")
            item_1.setText(2, f"{k.dupe_flag}")

if __name__ == '__main__':
    engine = get_engine(dbtype='mysql')
    Session = sessionmaker(bind=engine)
    session = Session()
    app = QApplication(sys.argv)
    w = MainApp(session)
    w.show()
    sys.exit(app.exec())
