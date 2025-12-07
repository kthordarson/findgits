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
from PySide6.QtWidgets import QTextEdit
from PySide6.QtCore import Qt, QRect, QMetaObject  # type: ignore
from PySide6.QtWidgets import QHeaderView

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
		# self.ui.actionExit.triggered.connect(QApplication.quit)
		self.ui.actionExit.triggered.connect(self.close_application)
		# self.ui.searchpaths_button.clicked.connect(self.searchpaths_button_clicked)
		self.dupefilter = False

		self.detailsTextEdit = QTextEdit(self.ui.centralwidget)
		self.detailsTextEdit.setObjectName(u"detailsTextEdit")
		self.detailsTextEdit.setGeometry(QRect(930, 120, 240, 500))
		self.detailsTextEdit.setReadOnly(True)
		self.ui.dupe_paths_widget.hide()
		self.populate_gitrepos()

	def close_application(self):
		"""Close the application cleanly"""
		# Optional: Add cleanup code here (close session, save state, etc.)
		if self.session:
			self.session.close()
		QApplication.quit()

	def old_getdupes_button_clicked(self, widget):
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

	def getdupes_button_clicked(self, widget):
		pass

	def repo_item_clicked(self, widget):  # show info about selected repo
		repo = session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
		if not repo:
			logger.error(f'repo_item_clicked: no repo found for id {widget.text(0)}')
			return
		else:
			duperepos = session.query(GitRepo).where(text(f'git_url like "{repo.git_url}"')).all()
			dupe_locations = [session.query(GitFolder.git_path).filter(GitFolder.id == k.id).first() for k in duperepos]
			if len(duperepos) > 1 or len(dupe_locations) > 1:
				logger.debug(f'repo_item_clicked {repo} duperepos: {len(duperepos)} dupe_locations: {len(dupe_locations)}')
			self.ui.idLabel.setText(QCoreApplication.translate("FindGitsApp", u"id", None))
			self.ui.idLineEdit.setText(QCoreApplication.translate("FindGitsApp", f"{repo.id}", None))
			# Update status bar with summary info
			status_text = f"Repo: {repo.full_name}"
			if repo.language:
				status_text += f" | Language: {repo.language}"
			if repo.stargazers_count:
				status_text += f" | stargazers: {repo.stargazers_count}"
			if repo.forks_count:
				status_text += f" | forks: {repo.forks_count}"
			self.statusBar().showMessage(status_text)

			# Build detailed info text
			details = []
			details.append(f"Repository: {repo.full_name or 'N/A'}")
			details.append(f"Owner: {repo.github_owner or 'N/A'}")
			details.append(f"Description: {repo.description or 'No description'}")
			details.append(f"")
			details.append(f"--- URLs ---")
			details.append(f"HTML: {repo.html_url or 'N/A'}")
			details.append(f"Clone: {repo.clone_url or 'N/A'}")
			details.append(f"SSH: {repo.ssh_url or 'N/A'}")
			details.append(f"")
			details.append(f"--- Stats ---")
			details.append(f"Stars: {repo.stargazers_count or 0}")
			details.append(f"Watchers: {repo.watchers_count or 0}")
			details.append(f"Forks: {repo.forks_count or 0}")
			details.append(f"Open Issues: {repo.open_issues_count or 0}")
			details.append(f"Size: {repo.size or 0} KB")
			details.append(f"Language: {repo.language or 'Unknown'}")
			details.append(f"")
			details.append(f"--- Dates ---")
			details.append(f"Created: {repo.created_at or 'N/A'}")
			details.append(f"Updated: {repo.updated_at or 'N/A'}")
			details.append(f"Pushed: {repo.pushed_at or 'N/A'}")
			details.append(f"")
			details.append(f"--- Flags ---")
			details.append(f"Private: {'Yes' if repo.private else 'No'}")
			details.append(f"Fork: {'Yes' if repo.fork else 'No'}")
			details.append(f"Archived: {'Yes' if repo.archived else 'No'}")
			details.append(f"Starred: {'Yes' if repo.is_starred else 'No'}")
			details.append(f"")
			details.append(f"--- Local Info ---")
			details.append(f"Local Path: {repo.local_path or 'Not cloned'}")
			details.append(f"Branch: {repo.branch or 'N/A'}")
			details.append(f"Default Branch: {repo.default_branch or 'N/A'}")

			# Get folder info if available
			folders = self.session.query(GitFolder).filter(GitFolder.gitrepo_id == repo.id).all()
			if folders:
				details.append(f"")
				details.append(f"--- Local Folders ({len(folders)}) ---")
				for folder in folders:
					folder_size = f"{folder.folder_size / 1024:.1f} KB" if folder.folder_size else "Unknown"
					details.append(f"  • {folder.git_path}")
					details.append(f"    Size: {folder_size}, Files: {folder.file_count or 0}")

			# Show duplicates if any
			if len(duperepos) > 1:
				details.append(f"")
				details.append(f"--- Duplicates ({len(duperepos)}) ---")
				for dupe in duperepos:
					if dupe.id != repo.id:
						details.append(f"  • ID {dupe.id}: {dupe.local_path or 'No local path'}")

			# Display details - if you have a details text widget, use it
			self.detailsTextEdit.setPlainText('\n'.join(details))

	def populate_gitrepos(self):
		self.ui.repotree.clear()
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "name")
		self.ui.repotree.headerItem().setText(2, "description")
		self.ui.repotree.headerItem().setText(3, "size")

		gitrepos = session.query(GitRepo).all()
		for k in gitrepos:
			item_1 = QTreeWidgetItem(self.ui.repotree)
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.full_name}")
			item_1.setText(2, f"{k.description}")
			item_1.setText(3, f"{k.size}")
		header = self.ui.repotree.header()
		header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Auto-resize
		header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)           # Fill available space
		header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)       # User can resize
		header.setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)       # User can resize
		self.ui.repotree.resizeColumnToContents(0)
		self.ui.repotree.setColumnWidth(1,100)
		self.ui.repotree.setColumnWidth(2,100)
		self.ui.repotree.resizeColumnToContents(3)
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
