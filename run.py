#!/usr/bin/python3

import os, sys
from argparse import ArgumentParser
from loguru import logger
from ui_main import Ui_FindGitsApp
from dbstuff import GitRepo, GitFolder, GitParentPath, get_engine, get_dupes, db_get_dupes
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
from PySide6.QtWidgets import (QApplication, QFormLayout, QLabel, QLineEdit,QHeaderView, QSizePolicy, QTreeWidget, QTreeWidgetItem, QWidget, QListWidgetItem, QTableWidgetItem)


class MainApp(QWidget, Ui_FindGitsApp):
	def __init__(self, session):
		self.session = session
		super(MainApp, self).__init__()
		self.setupUi(self)
		self.repotree.itemClicked.connect(self.repo_item_clicked)
		self.folderButton.clicked.connect(self.folderButton_clicked)
		self.getdupes_button.clicked.connect(self.getdupes_button_clicked)
		# self.checkBox_filterdupes.toggled.connect(self.checkBox_filterdupes_toggle)
		self.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
		self.gitshow_button.clicked.connect(self.gitshow_button_clicked)
		self.gitlog_button.clicked.connect(self.gitlog_button_clicked)
		self.gitstatus_button.clicked.connect(self.gitstatus_button_clicked)
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
		self.repotree.clear()
		self.repotree.setColumnCount(3)
		self.repotree.headerItem().setText(0, "id")
		self.repotree.headerItem().setText(1, "count")
		self.repotree.headerItem().setText(2, "git_url")
		dupes = get_dupes(self.session)
		for d in dupes:
			item0 = QTreeWidgetItem(self.repotree)
			item0.setText(0, f"{d.id}")
			item0.setText(1, f"{d.count}")
			item0.setText(2, f"{d.git_url}")
			# try:
			#     for f in d.get('folders'):
			#         item1 = QTreeWidgetItem(item0)
			#         item1.setText(0, f"{f.get('gitfolder_id')}")
			#         item1.setText(2, f"{f.get('git_path')}")
			# except TypeError as e:
			#     logger.error(e)
		# self.retranslateUi(self)

	def repo_item_clicked(self, widget):
		repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
		duperepos = session.query(GitRepo).where(text(f'git_url like "{repo.git_url}"')).all()
		dupe_locations = [session.query(GitFolder.git_path).filter(GitFolder.id == k.gitfolder_id).first() for k in duperepos]
		logger.debug(f'repo_item_clicked {repo} {len(duperepos)} path: {len(dupe_locations)}')
		self.idLabel.setText(QCoreApplication.translate("FindGitsApp", u"id", None))
		self.idLineEdit.setText(QCoreApplication.translate("FindGitsApp", f"{repo.id}", None))
		self.dupe_paths_widget.clear()
		self.dupe_paths_widget.setColumnCount(1)
		self.dupe_paths_widget.headerItem().setText(0, "path")
		for dp in dupe_locations:
			item0 = QTreeWidgetItem(self.dupe_paths_widget)
			item0.setText(0, f"{dp[0]}")

	def populate_gitrepos(self):
		self.repotree.clear()
		gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.git_url}")
		self.retranslateUi(self)

	def folderButton_clicked(self, widget):
		if widget:
			repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
			logger.debug(f'folderButton_clicked {repo}')
		# self.checkBox_filterdupes.setEnabled(True)

	def repotree_populate(self):
		self.repotree.clear()
		self.repotree.setColumnCount(3)
		[self.repotree.headerItem().setText(k[0],k[1]) for k in enumerate(["id", "dupes", "url"])]
		# self.repotree.headerItem().setText(0, "id")
		# self.repotree.headerItem().setText(1, "path")
		# self.repotree.headerItem().setText(2, "dupe")
		# self.repotree.headerItem().setText(3, "dupe_count")
		if self.dupefilter:
			gitrepos = session.query(GitRepo).filter(GitRepo.dupe_flag == self.dupefilter).all()
		else:
			gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.dupe_count}")
			item_1.setText(2, f"{k.git_url}")
		self.repotree.resizeColumnToContents(0)
		self.repotree.resizeColumnToContents(1)
		self.repotree.resizeColumnToContents(2)
			# self.dupecountlabel.setText(f'Dupes: {k.dupe_count}')


if __name__ == '__main__':
	myparse = ArgumentParser(description="findgits")
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dbsqlitefile', help='sqlitedb filename', default='gitrepo.db', dest='dbsqlitefile', action='store', metavar='dbsqlitefile')
	args = myparse.parse_args()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	app = QApplication(sys.argv)
	w = MainApp(session)
	w.show()
	sys.exit(app.exec())
