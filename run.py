#!/usr/bin/python3

import sys
from argparse import ArgumentParser
from loguru import logger
from ui_mainwindow import Ui_MainWindow
from dbstuff import GitRepo, GitFolder, get_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from PySide6.QtCore import (QCoreApplication)
from PySide6.QtWidgets import (QMainWindow, QApplication, QTreeWidgetItem, QPushButton)
from PySide6.QtWidgets import QTextBrowser  # Changed from QTextEdit
from PySide6.QtCore import Qt, QRect  # , QMetaObject  # type: ignore
from PySide6.QtWidgets import QHeaderView

class NumericTreeWidgetItem(QTreeWidgetItem):
	"""QTreeWidgetItem with proper numeric sorting for specified columns"""

	def __init__(self, parent=None, numeric_columns: list[int] | None = None):
		if parent:
			super().__init__(parent)
		else:
			super().__init__()
		self.numeric_columns = numeric_columns or []

	def __lt__(self, other):
		column = self.treeWidget().sortColumn()
		if column in self.numeric_columns:
			# Get numeric value from UserRole data, fallback to 0
			self_value = self.data(column, Qt.ItemDataRole.UserRole) or 0
			other_value = other.data(column, Qt.ItemDataRole.UserRole) or 0
			return self_value < other_value
		# For non-numeric columns, compare text directly instead of calling super()
		return self.text(column).lower() < other.text(column).lower()


# QWidget, Ui_FindGitsApp):
class MainApp(QMainWindow):
	def __init__(self, session, parent=None):
		self.session = session
		super(MainApp, self).__init__(parent=parent)
		self.ui = Ui_MainWindow()
		self.ui.setupUi(self)
		self.ui.repotree.itemClicked.connect(self.repo_item_clicked)
		self.ui.folderButton.clicked.connect(self.folderButton_clicked)
		self.ui.getdupes_button.clicked.connect(self.getdupes_button_clicked)
		self.ui.actionExit.triggered.connect(self.close_application)
		self.dupefilter = False
		self.hide_not_cloned = False  # Track filter state

		# Add filter button for not-cloned repos
		self.filterNotClonedButton = QPushButton(self.ui.centralwidget)
		self.filterNotClonedButton.setObjectName(u"filterNotClonedButton")
		# Position after btn_four - adjust coordinates as needed
		# self.filterNotClonedButton.setGeometry(QRect(400, 10, 200, 30))
		self.filterNotClonedButton.setMinimumWidth(250)
		self.filterNotClonedButton.setStyleSheet("text-align: left; ")
		self.filterNotClonedButton.setText("Hide Not Cloned")
		self.filterNotClonedButton.adjustSize()  # Resize to fit text
		self.filterNotClonedButton.clicked.connect(self.toggle_not_cloned_filter)
		self.ui.horizontalLayout.addWidget(self.filterNotClonedButton)

		# Use QTextBrowser instead of QTextEdit for clickable links
		self.detailsTextEdit = QTextBrowser(self.ui.centralwidget)
		self.detailsTextEdit.setObjectName(u"detailsTextEdit")
		self.detailsTextEdit.setGeometry(QRect(930, 120, 240, 500))
		self.detailsTextEdit.setReadOnly(True)
		# Enable clickable links - this works with QTextBrowser
		self.detailsTextEdit.setOpenExternalLinks(True)
		self.ui.dupe_paths_widget.hide()
		# In __init__, after creating detailsTextEdit:
		self.detailsTextEdit.setStyleSheet("""
			QTextEdit {
				font-family: monospace;
				font-size: 11px;
			}
			a {
				color: #0366d6;
				text-decoration: none;
			}
			a:hover {
				text-decoration: underline;
			}
		""")

		self.populate_gitrepos()

	def toggle_not_cloned_filter(self):
		"""Toggle visibility of repos without local path"""
		self.hide_not_cloned = not self.hide_not_cloned
		if self.hide_not_cloned:
			self.filterNotClonedButton.setText("Show Not Cloned")
		else:
			self.filterNotClonedButton.setText("Hide Not Cloned")
		self.filterNotClonedButton.adjustSize()  # Resize to fit text
		self.populate_gitrepos()

	def close_application(self):
		"""Close the application cleanly"""
		# Optional: Add cleanup code here (close session, save state, etc.)
		if self.session:
			self.session.close()
		QApplication.quit()

	def getdupes_button_clicked(self, widget):
		pass

	def repo_item_clicked(self, widget):  # show info about selected repo
		repo = self.session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
		if not repo:
			logger.error(f'repo_item_clicked: no repo found for id {widget.text(0)}')
			return

		duperepos = self.session.query(GitRepo).where(text(f'git_url like "{repo.git_url}"')).all()
		dupe_locations = [self.session.query(GitFolder.git_path).filter(GitFolder.id == k.id).first() for k in duperepos]
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

		# Helper function to make URLs clickable
		def make_link(url: str | None, display_text: str | None = None) -> str:
			if not url:
				return 'N/A'
			display_text = url
			return f'<a href="{url}">{display_text}</a>'

		def make_file_link(path: str | None) -> str:
			"""Make a file:// link for local paths"""
			if not path:
				return 'Not cloned'
			return f'<a href="file://{path}">{path}</a>'

		# Build detailed info text as HTML
		details = []
		details.append(f"<b>Repository:</b> {repo.full_name or 'N/A'}")
		details.append(f"<b>Owner:</b> {repo.github_owner or 'N/A'}")
		details.append(f"<b>Description:</b> {repo.description or 'No description'}")
		details.append("")
		details.append("<b>--- URLs ---</b>")
		details.append(f"<b>HTML:</b> {make_link(repo.html_url)}")
		details.append(f"<b>Clone:</b> {make_link(repo.clone_url, repo.clone_url)}")
		details.append(f"<b>SSH:</b> {repo.ssh_url or 'N/A'}")
		details.append("")
		details.append("<b>--- Stats ---</b>")
		details.append(f"<b>Stars:</b> {repo.stargazers_count or 0}")
		details.append(f"<b>Watchers:</b> {repo.watchers_count or 0}")
		details.append(f"<b>Forks:</b> {repo.forks_count or 0}")
		details.append(f"<b>Open Issues:</b> {repo.open_issues_count or 0}")
		details.append(f"<b>Size:</b> {repo.size or 0} KB")
		details.append(f"<b>Language:</b> {repo.language or 'UnknownLang'}")
		details.append("")
		details.append("<b>--- Dates ---</b>")
		details.append(f"<b>Created:</b> {repo.created_at or 'N/A'}")
		details.append(f"<b>Updated:</b> {repo.updated_at or 'N/A'}")
		details.append(f"<b>Pushed:</b> {repo.pushed_at or 'N/A'}")
		details.append("")
		details.append("<b>--- Flags ---</b>")
		details.append(f"<b>Private:</b> {'Yes' if repo.private else 'No'}")
		details.append(f"<b>Fork:</b> {'Yes' if repo.fork else 'No'}")
		details.append(f"<b>Archived:</b> {'Yes' if repo.archived else 'No'}")
		details.append(f"<b>Starred:</b> {'Yes' if repo.is_starred else 'No'}")
		details.append("")
		details.append("<b>--- Local Info ---</b>")
		details.append(f"<b>Local Path:</b> {make_file_link(repo.local_path)}")
		details.append(f"<b>Branch:</b> {repo.branch or 'N/A'}")
		details.append(f"<b>Default Branch:</b> {repo.default_branch or 'N/A'}")

		# Get folder info if available
		folders = self.session.query(GitFolder).filter(GitFolder.gitrepo_id == repo.id).all()
		if folders:
			details.append("")
			details.append(f"<b>--- Local Folders ({len(folders)}) ---</b>")
			for folder in folders:
				folder_size = f"{folder.folder_size / 1024:.1f} KB" if folder.folder_size else "UnknownSize"
				details.append(f"  • {make_file_link(folder.git_path)}")
				details.append(f"    Size: {folder_size}, Files: {folder.file_count or 0}")

		# Show duplicates if any
		if len(duperepos) > 1:
			details.append("")
			details.append(f"<b>--- Duplicates ({len(duperepos)}) ---</b>")
			for dupe in duperepos:
				if dupe.id != repo.id:
					details.append(f"  • ID {dupe.id}: {make_file_link(dupe.local_path)}")

		# Display details as HTML - use <br> for line breaks
		html_content = '<br>'.join(details)
		self.detailsTextEdit.setHtml(html_content)

	def populate_gitrepos(self):
		self.ui.repotree.clear()
		self.ui.repotree.headerItem().setText(0, "id")
		self.ui.repotree.headerItem().setText(1, "name")
		self.ui.repotree.headerItem().setText(2, "description")
		self.ui.repotree.headerItem().setText(3, "size")
		self.ui.repotree.headerItem().setText(4, "folder size")
		self.ui.repotree.headerItem().setText(5, "updated_at")
		self.ui.repotree.setSortingEnabled(True)

		# Build query based on filter state
		query = self.session.query(GitRepo)
		if self.hide_not_cloned:
			query = query.filter(GitRepo.local_path.isnot(None))  # type: ignore
			query = query.filter(GitRepo.local_path != '')  # type: ignore
			query = query.filter(GitRepo.local_path != '[notcloned]')  # type: ignore

		gitrepos = query.all()

		# Update status bar with count
		total_count = self.session.query(GitRepo).count()
		filtered_count = len(gitrepos)
		if self.hide_not_cloned:
			self.statusBar().showMessage(f"Showing {filtered_count} cloned repos (hiding {total_count - filtered_count} not cloned)")
		else:
			self.statusBar().showMessage(f"Showing all {total_count} repos")

		for k in gitrepos:
			item_1 = NumericTreeWidgetItem(self.ui.repotree, numeric_columns=[0, 3])
			item_1.setText(0, f"{k.id}")
			item_1.setText(1, f"{k.full_name}")
			item_1.setText(2, f"{k.description}")
			item_1.setText(3, f"{k.size}")
			item_1.setText(5, f"{k.updated_at}")
			item_1.setData(0, Qt.ItemDataRole.UserRole, k.id)
			item_1.setData(3, Qt.ItemDataRole.UserRole, k.size or 0)
			total_size = sum(f.folder_size or 0 for f in k.git_folders) if k.git_folders else 0
			if total_size > 0:
				folder_size_mb = total_size / (1024 * 1024)
				item_1.setText(4, f"{folder_size_mb:.2f} MB")
			else:
				item_1.setText(4, "N/A")
			item_1.setData(4, Qt.ItemDataRole.UserRole, total_size)
		header = self.ui.repotree.header()
		header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
		header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
		header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
		header.setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)
		header.setSectionResizeMode(4, QHeaderView.ResizeMode.Interactive)

		self.ui.repotree.sortByColumn(0, Qt.SortOrder.AscendingOrder)

		self.ui.repotree.resizeColumnToContents(0)
		self.ui.repotree.setColumnWidth(1, 100)
		self.ui.repotree.setColumnWidth(2, 100)
		self.ui.repotree.resizeColumnToContents(3)
		self.ui.repotree.resizeColumnToContents(4)
		self.ui.retranslateUi(self)

	def folderButton_clicked(self, widget):  # change to folder tree view
		if widget:
			repo = self.session.query(GitRepo).filter(GitRepo.id == widget.text(0)).first()
			logger.debug(f'folderButton_clicked {repo=}')
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
