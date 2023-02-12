import os, sys
from PyQt5.QtCore import QCoreApplication
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QApplication
from loguru import logger
from ui_main import Ui_FindGitsApp
from dbstuff import GitRepo, GitFolder, send_to_db, get_engine, db_init, send_gitfolder_to_db, get_folder_entries, get_repo_entries
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
        self.treeWidget.itemChanged.connect(self.handleChanged)
        self.treeWidget.itemClicked.connect(self.tree_item_clicked)
        self.pushButton.clicked.connect(self.on_button_clicked)
        self.tableWidget.setColumnCount(2)
        self.tableWidget.setHorizontalHeaderLabels(['gitrepoid', 'giturl'])
        # self.getgit()

    def tree_item_clicked(self, widget):
        try:
            repo = [k for k in self.gitrepos if k.git_path==widget.text(1)][0]
        except IndexError as e:
            logger.error(f'[tic] indexerror {e} widget={widget.text(0)} {widget.text(1)}')
            return
        repoid_item = QTableWidgetItem(f'{repo.gitrepoid}') #}\nurl: {repo.giturl}\nbranch: {repo.branch}\npath: {repo.git_path}')
        self.label_repoid.setText(f'{repo.gitrepoid}')
        self.label_repourl.setText(f'{repo.giturl}')
        repourl_item = QTableWidgetItem(f'{repo.giturl}')
        #repo_item.setText()
        row = self.tableWidget.rowCount()
        self.tableWidget.insertRow(row)
        self.tableWidget.setItem(row,0,repoid_item)
        self.tableWidget.setItem(row,1,repourl_item)
        self.tableWidget.resizeColumnsToContents()
        logger.debug(f'[h] {self} id: {widget.text(0)} path: {widget.text(1)} repo={repo.giturl}')

    def getgit(self):
        repos = session.query(GitRepo).all()
        # self.populate_gitfolders(repos)

    def populate_gitrepos(self, gitrepos):
        #self.item_0 = QTreeWidgetItem(self.treeWidget)
        for k in gitrepos:
            item_1 = QTreeWidgetItem(self.treeWidget)
            # item_1.setCheckState(0, QtCore.Qt.Unchecked)
            item_1.setText(0, f"{k.gitrepoid}")
            item_1.setText(1, f"{k.giturl}")
        self.retranslateUi(self)

    def populate_gitfolders(self, gitfolders):
        #self.item_0 = QTreeWidgetItem(self.treeWidget)
        for k in gitfolders:
            item_1 = QTreeWidgetItem(self.treeWidget)
            # item_1.setCheckState(0, QtCore.Qt.Unchecked)
            item_1.setText(0, f"{k.folderid}")
            item_1.setText(1, f"{k.git_path}")
        self.retranslateUi(self)

    def on_button_clicked(self, widget):
        self.gitrepos = session.query(GitRepo).all()
        self.gitfolders = session.query(GitFolder).all()
        print(f"[on_button_clicked] repos {len(self.gitrepos)} folders = {len(self.gitfolders)}")
        self.populate_gitfolders(self.gitfolders)

    def handleChanged(self, item, column):
        count = item.childCount()
        # logger.debug(f'[h] {self} count={count} i: {item} c: {column}')
        if item.checkState(column) == Qt.Checked:
            for index in range(count):
                item.child(index).setCheckState(0, Qt.Checked)
        if item.checkState(column) == Qt.Unchecked:
            for index in range(count):
                item.child(index).setCheckState(0, Qt.Unchecked)

if __name__ == '__main__':
    engine = get_engine(dbtype='sqlite')
    Session = sessionmaker(bind=engine)
    session = Session()
    app = QApplication(sys.argv)
    w = MainApp(session)
    w.show()
    sys.exit(app.exec())
