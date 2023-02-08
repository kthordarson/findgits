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
    QPalette, QPixmap, QRadialGradient, QTransform)
# from PySide6.QtWidgets import (QApplication, QSizePolicy, QWidget)
from PySide6.QtWidgets import (QApplication, QHeaderView, QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget)

class MainApp(QWidget, Ui_FindGitsApp):
    def __init__(self, session):
        self.session = session
        super(MainApp, self).__init__()
        self.setupUi(self)
        self.treeWidget.itemChanged.connect(self.handleChanged)
        self.treeWidget.itemClicked.connect(self.tree_item_clicked)
        self.pushButton.clicked.connect(self.on_button_clicked)
        # self.getgit()

    def tree_item_clicked(self, widget):
        logger.debug(f'[h] {self} i: {widget}')

    def getgit(self):
        repos = session.query(GitRepo).all()
        # self.populate_gitfolders(repos)

    def populate_gitfolders(self, gitfolders):
        #self.item_0 = QTreeWidgetItem(self.treeWidget)
        for k in gitfolders:
            item_1 = QTreeWidgetItem(self.treeWidget)
            # item_1.setCheckState(0, QtCore.Qt.Unchecked)
            item_1.setText(0, f"{k.gitrepoid}")
            item_1.setText(1, f"{k.giturl}")
        self.retranslateUi(self)

    def on_button_clicked(self, widget):
        repos = session.query(GitRepo).all()
        print(f"self.populate_gitfolders(repos) {len(repos)}")
        self.populate_gitfolders(repos)

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
