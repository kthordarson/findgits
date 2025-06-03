#!/usr/bin/python3

import os
import sys
from argparse import ArgumentParser
from loguru import logger
from ui_main import Ui_FindGitsApp
from ui_mainwindow import Ui_MainWindow
from dbstuff import GitRepo, GitFolder, get_engine, get_dupes, db_get_dupes
from sqlalchemy import and_, text
from sqlalchemy.orm import sessionmaker
from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale, QMetaObject, QObject, QPoint, QRect, QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor, QFont, QFontDatabase, QGradient, QIcon, QImage, QKeySequence, QLinearGradient, QPainter, QPalette, QPixmap, QRadialGradient, QTransform, QStandardItemModel, QStandardItem)
# from PySide6.QtWidgets import (QApplication, QSizePolicy, QWidget)
from PySide6.QtWidgets import (QMainWindow, QApplication, QFormLayout, QLabel, QLineEdit,QHeaderView, QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget, QListWidgetItem, QTableWidgetItem)

# QWidget, Ui_FindGitsApp):
class MainApp(QMainWindow):
	def __init__(self, session, parent=None):
		self.session = session
		# super(MainApp, self).__init__()
		super(MainApp, self).__init__(parent=parent)
		self.ui = Ui_MainWindow()
		self.ui.setupUi(self)
		# self.setupUi(self)
		self.ui.repotree.itemClicked.connect(self.repo_item_clicked)
		self.ui.folderButton.clicked.connect(self.folderButton_clicked)
		self.ui.getdupes_button.clicked.connect(self.getdupes_button_clicked)
		# self.checkBox_filterdupes.toggled.connect(self.checkBox_filterdupes_toggle)
		self.ui.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
		# self.ui.gitshow_button.clicked.connect(self.gitshow_button_clicked)
		# self.ui.gitlog_button.clicked.connect(self.gitlog_button_clicked)
		# self.ui.gitstatus_button.clicked.connect(self.gitstatus_button_clicked)
		self.dupefilter = False
		self.repotree_populate()

	def gitshow_button_clicked(self, widget):
		pass

	def gitlog_button_clicked(self, widget):
		pass

	def gitstatus_button_clicked(self, widget):
		pass

	def searchpaths_button_clicked(self, widget):
		pass

	# def checkBox_filterdupes_toggle(self, *args):
	# 	self.dupefilter = self.checkBox_filterdupes.isChecked()
	# 	self.folderButton_clicked(self)

	def getdupes_button_clicked(self, widget):
		# self.checkBox_filterdupes.setEnabled(False)
		self.ui.repotree.clear()
		self.ui.repotree.setColumnCount(3)
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "count")
		self.ui.repotree.headerItem().setText(2, "git_url")
		dupes = get_dupes(self.session)
		for d in dupes:
			item0 = QTreeWidgetItem(self.ui.repotree)
			item0.setText(0, f"{d.id}")
			item0.setText(1, f"{d.count}")
			item0.setText(2, f"{d.git_url}")

	def repo_item_clicked(self, widget):  # show info about selected repo
		repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
		if not repo:
			logger.error(f'repo_item_clicked: no repo found for id {widget.text(0)}')
			return
		else:
			duperepos = session.query(GitRepo).where(text(f'git_url like "{repo.git_url}"')).all()
			dupe_locations = [session.query(GitFolder.git_path).filter(GitFolder.id == k.id).first() for k in duperepos]
			logger.debug(f'repo_item_clicked {repo} {len(duperepos)} path: {len(dupe_locations)}')
			self.ui.idLabel.setText(QCoreApplication.translate("FindGitsApp", u"id", None))
			self.ui.idLineEdit.setText(QCoreApplication.translate("FindGitsApp", f"{repo.id}", None))
			self.ui.dupe_paths_widget.clear()
			self.ui.dupe_paths_widget.setColumnCount(1)
			self.ui.dupe_paths_widget.headerItem().setText(0, "path")
			for dp in dupe_locations:
				item0 = QTreeWidgetItem(self.ui.dupe_paths_widget)
				item0.setText(0, f"{dp[0]}")

	def populate_gitrepos(self):
		self.ui.repotree.clear()
		gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.ui.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.git_url}")
		self.ui.retranslateUi(self)

	def folderButton_clicked(self, widget):  # change to folder tree view
		if widget:
			repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
			logger.debug(f'folderButton_clicked {repo=}')
		# self.checkBox_filterdupes.setEnabled(True)

	def repotree_populate(self):
		gpf = []
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "folder")
		self.ui.repotree.headerItem().setText(2, "repos")
		self.ui.repotree.headerItem().setText(3, "folder_size")
		# self.ui.repotree.data()
		# items = QTreeWidgetItem(self.ui.repotree)
		for k in gpf:
			item = QTreeWidgetItem(self.ui.repotree)
			item.setText(0, f"{k.id}")
			item.setText(1, f"{k.folder}")
			item.setText(2, f"{k.repo_count}")
			item.setText(3, f"{k.folder_size:,}")
			git_paths = session.query(GitFolder).filter(GitFolder.searchpath_id == k.id).all()
			for g in git_paths:
				item1 = QTreeWidgetItem(item)
				item1.setText(0, f"{g.id}")
				item1.setText(1, f"{g.git_path}")
				item1.setText(3, f"{g.folder_size:,}")
		self.ui.repotree.resizeColumnToContents(0)
		self.ui.repotree.resizeColumnToContents(1)
		# self.ui.repotree.addTopLevelItem(item_1)
# QTreeWidget treeWidget = new QTreeWidget();
# treeWidget->setColumnCount(1);
# QList<QTreeWidgetItem > items;
# for (int i = 0; i < 10; ++i)
#       items.append(new QTreeWidgetItem((QTreeWidget*)0, QStringList(QString("item: %1").arg(i))));
# treeWidget->insertTopLevelItems(0, items);

	def xrepotree_populate(self):
		self.ui.repotree.clear()
		self.ui.repotree.setColumnCount(3)
		[self.ui.repotree.headerItem().setText(k[0],k[1]) for k in enumerate(["id", "dupes", "url"])]
		# self.repotree.headerItem().setText(0, "id")
		# self.repotree.headerItem().setText(1, "path")
		# self.repotree.headerItem().setText(2, "dupe")
		# self.repotree.headerItem().setText(3, "dupe_count")
		if self.dupefilter:
			gitrepos = session.query(GitRepo).filter(GitRepo.dupe_flag == self.dupefilter).all()
		else:
			gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.ui.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.dupe_count}")
			item_1.setText(2, f"{k.git_url}")
		self.ui.repotree.resizeColumnToContents(0)
		self.ui.repotree.resizeColumnToContents(1)
		self.ui.repotree.resizeColumnToContents(2)
		# self.dupecountlabel.setText(f'Dupes: {k.dupe_count}')

if __name__ == '__main__':
	myparse = ArgumentParser(description="findgits")
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
	myparse.add_argument('--dbsqlitefile', help='sqlitedb filename', default='gitrepo.db', dest='dbsqlitefile', action='store', metavar='dbsqlitefile')
	args = myparse.parse_args()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	app = QApplication(sys.argv)
	w = MainApp(session)
	w.show()
	sys.exit(app.exec())
