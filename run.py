#!/usr/bin/python3

import sys
from argparse import ArgumentParser
from loguru import logger
from ui_mainwindow import Ui_MainWindow
from dbstuff import GitRepo, GitFolder, get_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from PySide6.QtCore import (QCoreApplication)
from PySide6.QtWidgets import (QMainWindow, QApplication, QTreeWidgetItem)

# QWidget, Ui_FindGitsApp):
class MainApp(QMainWindow):
	def __init__(self, session, parent=None):
		self.session = session
		# super(MainApp, self).__init__()
		super(MainApp, self).__init__(parent=parent)
		self.ui = Ui_MainWindow()
		self.ui.setupUi(self)
		self.ui.repotree.itemClicked.connect(self.repo_item_clicked)
		self.ui.folderButton.clicked.connect(self.folderButton_clicked)
		self.ui.getdupes_button.clicked.connect(self.getdupes_button_clicked)
		# self.ui.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
		self.dupefilter = False
		self.populate_gitrepos()

	def getdupes_button_clicked(self, widget):
		# self.checkBox_filterdupes.setEnabled(False)
		self.ui.repotree.clear()
		self.ui.repotree.setColumnCount(3)
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "count")
		self.ui.repotree.headerItem().setText(2, "git_url")
		dupes = []  # get_dupes(self.session)
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
			# logger.debug(f'repo_item_clicked {repo} {len(duperepos)} path: {len(dupe_locations)}')
			self.ui.idLabel.setText(QCoreApplication.translate("FindGitsApp", u"id", None))
			self.ui.idLineEdit.setText(QCoreApplication.translate("FindGitsApp", f"{repo.id}", None))

	def populate_gitrepos(self):
		self.ui.repotree.clear()
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "folder")
		self.ui.repotree.headerItem().setText(2, "repos")
		self.ui.repotree.headerItem().setText(3, "folder_size")

		gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.ui.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.git_url}")
		self.ui.repotree.resizeColumnToContents(0)
		self.ui.repotree.resizeColumnToContents(1)
		self.ui.retranslateUi(self)

	def folderButton_clicked(self, widget):  # change to folder tree view
		if widget:
			repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
			# logger.debug(f'folderButton_clicked {repo=}')
		# self.checkBox_filterdupes.setEnabled(True)

if __name__ == '__main__':
	myparse = ArgumentParser(description="findgits")
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
	myparse.add_argument('--db_file', help='sqlitedb filename', default='gitrepo.db', dest='db_file', action='store', metavar='db_file')
	args = myparse.parse_args()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	app = QApplication(sys.argv)
	w = MainApp(session)
	w.show()
	sys.exit(app.exec())
