from __future__ import annotations
import os
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List
from loguru import logger
import sqlalchemy
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from utils import valid_git_folder, get_remote
# from git_tasks import get_git_show

# Base = declarative_base()

class MissingConfigException(Exception):
	pass


class MissingGitFolderException(Exception):
	pass


class Base(DeclarativeBase):
	pass


# GPP folders that contain more than one git repo
# --addpath to GPP to db
# todo: if a subfolder of a GPP contains other git repos, make those a GPP and add to db
#       example: GPP folder ~/development2 has one called games, which should be a GPP


class GitFolder(Base):
	""" A folder containing one git repo """
	__tablename__ = 'git_path'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	# gitrepo_id = Column('gitrepo_id', Integer)  # id of gitrepo found in this folder
	gitrepo_id = Column(Integer, ForeignKey('gitrepo.id'))  # Change this line
	git_path = Column('git_path', String(255))
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_count = Column('scan_count', Integer)
	scan_time = Column('scan_time', Float)
	folder_size = Column('folder_size', BigInteger)
	file_count = Column('file_count', BigInteger)
	subdir_count = Column('subdir_count', BigInteger)

	git_path_ctime = Column('git_path_ctime', DateTime)
	git_path_atime = Column('git_path_atime', DateTime)
	git_path_mtime = Column('git_path_mtime', DateTime)
	dupe_flag = Column('dupe_flag', Boolean)
	dupe_count = Column('dupe_count', BigInteger)
	valid = Column(Boolean, default=True)
	scanned = Column[bool]
	repo: Mapped["GitRepo"] = relationship("GitRepo", back_populates="git_folders")

	def __init__(self, git_path: str, gitrepo_id):
		self.git_path = str(git_path)
		self.gitrepo_id = gitrepo_id
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.scan_count = 0
		self.dupe_flag = False
		self.dupe_count = 0
		self.is_parent = False
		self.folder_size = 0
		self.file_count = 0
		self.subdir_count = 0
		self.scanned = False
		self.get_folder_time()

	def __repr__(self):
		return f'<GitFolder {self.id} git_path={self.git_path}>'

	def get_folder_time(self):
		""" Get stats for this git_path"""
		if not os.path.exists(self.git_path):  # redundant check, but just in case?
			self.valid = False
			raise MissingGitFolderException(f'{self} does not exist')
		t0 = datetime.now()
		self.last_scan = datetime.now()
		stat = os.stat(self.git_path)
		self.git_path_ctime = datetime.fromtimestamp(stat.st_ctime)
		self.git_path_atime = datetime.fromtimestamp(stat.st_atime)
		self.git_path_mtime = datetime.fromtimestamp(stat.st_mtime)

	def get_folder_stats(self, id, git_path):
		t0 = datetime.now()
		self.folder_size = get_directory_size(git_path)
		self.file_count = get_subfilecount(git_path)
		self.subdir_count = get_subdircount(git_path)
		scan_time = (datetime.now() - t0).total_seconds()
		self.scanned = True

# todo: make this better, should only be linked to one git_path
class GitRepo(Base):
	""" A git repo, linked to one git_path  """
	__tablename__ = 'gitrepo'
	id: Mapped[int] = mapped_column(primary_key=True)
	git_url = Column('git_url', String(255))
	github_owner = Column('github_owner', String(255))
	github_repo_name = Column('github_repo_name', String(255))
	local_path = Column('local_path', String(255))
	remote = Column('remote', String(255))
	branch = Column('branch', String(255))
	dupe_flag = Column('dupe_flag', Boolean)
	dupe_count = Column('dupe_count', BigInteger)
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_count = Column('scan_count', Integer)
	config_ctime = Column('config_ctime', DateTime)
	config_atime = Column('config_atime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	valid = Column(Boolean, default=True)
	scanned = Column[bool]
	git_folders: Mapped[List["GitFolder"]] = relationship("GitFolder", back_populates="repo")

	def __init__(self, git_url, local_path):
		# self.git_path = git_path.git_path
		self.git_url = git_url
		self.local_path = local_path
		self.github_repo_name = git_url.split('/')[-1].replace('.git', '')
		if 'http' in git_url:
			self.github_owner = git_url.split('/')[-2]
		elif 'git@github.com' in git_url:
			self.github_owner = git_url.split(':')[1].split('/')[0]
		else:
			self.github_owner = '[unknown]'
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_count = 0
		self.dupe_flag = False
		self.scanned = False
		if local_path != '[notcloned]':
			self.get_repo_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} url: {self.git_url} localpath: {self.local_path} >'

	def get_repo_stats(self):
		""" Collect stats and read config for this git repo """
		# c = self.
		self.scanned = True
		git_config_file = '.git/config'
		st = os.stat(git_config_file)
		self.config_ctime = datetime.fromtimestamp(st.st_ctime)
		self.config_atime = datetime.fromtimestamp(st.st_atime)
		self.config_mtime = datetime.fromtimestamp(st.st_mtime)
		conf = ConfigParser(strict=False)  # todo: make this better
		c = conf.read(git_config_file)
		remote_section = None
		if conf.has_section('remote "origin"'):
			try:
				remote_section = [k for k in conf.sections() if 'remote' in k][0]
			except IndexError as e:
				logger.error(f'[err] {self} {e} git_config_file={git_config_file} conf={conf.sections()}')
				# self.valid = False
			if remote_section:
				self.remote = remote_section.split(' ')[1].replace('"', '')
				branch_section = None
				try:
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
				except IndexError as e:
					pass  # logger.warning(f'{e} {self} {conf.sections()}')
				if branch_section:
					self.branch = branch_section.split(' ')[1].replace('"', '')
				try:
					# git_url = [k for k in conf['remote "origin"'].items()][0][1]
					self.git_url = conf[remote_section]['url']
					if not self.git_url:
						logger.warning(f'missing git_url {self.local_path} git_config_file={git_config_file}')
						self.git_url = '[no remote]'
				except TypeError as e:
					logger.warning(f'[!] {self} typeerror {e} git_config_file={git_config_file} ')
					# self.valid = False
				except KeyError as e:
					logger.warning(f'[!] {self} KeyError {e} git_config_file={git_config_file}')
					# self.valid = False
		else:
			self.git_url = get_remote(self.local_path)
			logger.warning(f'missing config remote origin {self} path: {self.local_path} url: {self.git_url} git_config_file={git_config_file}')
			# self.valid = False


def db_init(engine: sqlalchemy.Engine) -> None:
	Base.metadata.create_all(bind=engine)


def drop_database(engine: sqlalchemy.Engine) -> None:
	logger.warning(f'[drop] all engine={engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)


def get_engine(args) -> sqlalchemy.Engine:
	"""
	Get a db engine, uses os.getenv for db credentials
	Parameters: dbtype (str) - mysql/postgresql/sqlite
	Returns: sqlalchemy Engine
	"""
	if args.dbmode == 'mysql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = os.getenv('gitdbNAME')
		if not dbuser or not dbpass or not dbhost or not dbname:
			raise AttributeError('[db] missing db env variables')
		dburl = f"mysql+pymysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8mb4"
		return create_engine(dburl)
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgresql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = os.getenv('gitdbNAME')
		if not dbuser or not dbpass or not dbhost or not dbname:
			raise AttributeError('[db] missing db env variables')
		dburl = f"postgresql://{dbuser}:{dbpass}@{dbhost}/{dbname}"
		return create_engine(dburl)
	elif args.dbmode == 'sqlite':
		return create_engine(f'sqlite:///{args.dbsqlitefile}', echo=False, connect_args={'check_same_thread': False})
	else:
		raise TypeError(f'[db] unknown dbtype {args} ')

def get_directory_size(directory: str) -> int:
	# directory = Path(directory)
	total = 0
	try:
		for entry in os.scandir(directory):
			if entry.is_symlink():
				break
			if entry.is_file():
				try:
					total += entry.stat().st_size
				except FileNotFoundError as e:
					logger.warning(f'[err] {e} dir:{directory} ')
					continue
			elif entry.is_dir():
				try:
					total += get_directory_size(entry.path)
				except FileNotFoundError as e:
					logger.warning(f'[err] {e} dir:{directory} ')
	except NotADirectoryError as e:
		logger.warning(f'[err] {e} dir:{directory} ')
		return os.path.getsize(directory)
	# except (PermissionError, FileNotFoundError) as e:
	# 	logger.warning(f'[err] {e} {type(e)} dir:{directory} ')
	# 	return 0
	# logger.debug(f'[*] get_directory_size {directory} {total} bytes')
	return total


def get_subfilecount(directory: str) -> int:
	directory = Path(directory)
	try:
		filecount = len([k for k in directory.glob('**/*') if k.is_file()])
	except PermissionError as e:
		logger.warning(f'[err] {e} d:{directory}')
		return 0
	return filecount


def get_subdircount(directory: str) -> int:
	directory = Path(directory)
	dc = 0
	try:
		dc = len([k for k in directory.glob('**/*') if k.is_dir()])
	except (PermissionError, FileNotFoundError) as e:
		logger.warning(f'[err] {e} d:{directory}')
	return dc

def get_dupes(session: Session) -> list:
	"""
	Get a list of duplicate git repos.
	A duplicate is defined as a git repo with the same git_url
	Paramets: session (sessionmaker) - sqlalchemy session
	Returns: list of tuples (id, git_url, count)
	"""
	sql = text('select id, git_url, count(*) as count from gitrepo group by git_url having count(*)>1;')
	# sql = text('select * from dupeview;')
	dupes = session.execute(sql).all()
	return dupes

def insert_update_git_folder(git_folder_path, session):
	"""
	Insert a new GitFolder or update an existing one in the database

	Parameters:
		git_folder_path (str): Path to the git folder (containing .git directory)
		session: SQLAlchemy session

	Returns:
		GitFolder: The inserted or updated GitFolder object
	"""

	git_folder_path = str(Path(git_folder_path))

	# Ensure this is a valid git folder before proceeding
	if not valid_git_folder(os.path.join(git_folder_path, '.git')):
		logger.warning(f'{git_folder_path} is not a valid git folder')
		return None

	# Check if folder already exists in database
	git_folder = session.query(GitFolder).filter(GitFolder.git_path == git_folder_path).first()

	# Get remote URL for this repository
	remote_url = get_remote(git_folder_path)
	if not remote_url:
		logger.warning(f'Could not determine remote URL for {git_folder_path}')
		return None

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()
	if not git_repo:
		git_repo = GitRepo(remote_url, git_folder_path)
		git_repo.scan_count = 1
		session.add(git_repo)
		session.commit()
		logger.debug(f'Created new GitRepo: {git_repo}')

	if git_folder:
		# Update existing GitFolder
		git_folder.scan_count += 1
		git_folder.last_scan = datetime.now()
		git_folder.gitrepo_id = git_repo.id
		git_folder.get_folder_time()
		git_folder.get_folder_stats(git_folder.id, git_folder.git_path)
		logger.debug(f'Updated GitFolder: {git_folder}')
	else:
		# Create new GitFolder
		git_folder = GitFolder(git_folder_path, git_repo.id)
		git_folder.scan_count = 1
		session.add(git_folder)
		logger.debug(f'Created new GitFolder: {git_folder}')

	# Save changes
	session.commit()
	return git_folder

def xcheck_update_dupes(session) -> dict:
	"""
	Check for duplicate GitRepo entries (same git_url) and update their dupe_flag and dupe_count.
	A duplicate repository is one with the same git_url in multiple locations.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary of results containing:
			- total_repos: Total number of repositories
			- unique_repos: Number of unique repositories
			- dupe_repos: Number of repositories that are duplicates
			- dupes_updated: Number of repositories updated
	"""

	# Get all repositories
	all_repos = session.query(GitRepo).all()
	total_repos = len(all_repos)

	# Reset duplicate flags on all repos
	for repo in all_repos:
		repo.dupe_flag = False
		repo.dupe_count = 0

	# Get list of duplicates (repos with same git_url)
	dupes = get_dupes(session)
	dupe_urls = set()
	dupes_updated = 0

	# Process each duplicate group
	for dupe in dupes:
		dupe_id = dupe.id
		dupe_url = dupe.git_url
		dupe_count = dupe.count
		dupe_urls.add(dupe_url)

		# Find all repos with this URL
		same_url_repos = session.query(GitRepo).filter(GitRepo.git_url == dupe_url).all()

		# Update their dupe flags
		for repo in same_url_repos:
			repo.dupe_flag = True
			repo.dupe_count = dupe_count
			dupes_updated += 1

	# Commit the changes
	session.commit()

	# Prepare result summary
	result = {
		'total_repos': total_repos,
		'unique_repos': total_repos - len(dupes),
		'dupe_repos': len(dupe_urls),
		'dupes_updated': dupes_updated
	}

	# logger.info(f"Found {result['dupe_repos']} duplicate repo URLs among {total_repos} total repos")
	return result

def insert_update_starred_repo(github_repo, session):
	"""
	Insert a new GitRepo or update an existing one in the database

	Parameters:
		github_repo : repository object from GitHub API
		session: SQLAlchemy session

	"""

	git_folder_path = '[notcloned]'

	# Get remote URL for this repository
	remote_url = f'https://github.com/{github_repo}'
	if not remote_url:
		logger.warning(f'Could not determine remote URL for {git_folder_path}')
		return None

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()
	logger.debug(f'GitRepo: {git_repo} remote_url: {remote_url}')
	if not git_repo:
		git_repo = GitRepo(remote_url, git_folder_path)
		git_repo.scan_count = 1
		session.add(git_repo)
		session.commit()
		logger.debug(f'Created new GitRepo: {git_repo} remote_url: {remote_url} github_repo: {github_repo}')

	# Save changes
	session.commit()

def check_update_dupes(session) -> dict:
	"""
	Check for duplicate GitRepo entries (same git_url) and update their dupe_flag and dupe_count.
	A duplicate repository is one with the same git_url in multiple locations.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary of results containing:
			- total_repos: Total number of repositories
			- unique_repos: Number of unique repositories
			- dupe_repos: Number of repositories that are duplicates
			- dupes_updated: Number of repositories updated
	"""

	# Get all repositories
	all_repos = session.query(GitRepo).all()
	total_repos = len(all_repos)

	# Reset duplicate flags on all repos
	for repo in all_repos:
		repo.dupe_flag = False
		repo.dupe_count = 0

	# Get list of duplicates (repos with same git_url)
	dupes = get_dupes(session)
	dupe_urls = set()
	dupes_updated = 0

	# Process each duplicate group
	for dupe in dupes:
		dupe_id = dupe.id
		dupe_url = dupe.git_url
		dupe_count = dupe.count
		dupe_urls.add(dupe_url)

		# Find all repos with this URL
		same_url_repos = session.query(GitRepo).filter(GitRepo.git_url == dupe_url).all()

		# Update their dupe flags
		for repo in same_url_repos:
			repo.dupe_flag = True
			repo.dupe_count = dupe_count
			dupes_updated += 1

	# Commit the changes
	session.commit()

	# Prepare result summary
	result = {
		'total_repos': total_repos,
		'unique_repos': total_repos - len(dupes),
		'dupe_repos': len(dupe_urls),
		'dupes_updated': dupes_updated
	}

	# logger.info(f"Found {result['dupe_repos']} duplicate repo URLs among {total_repos} total repos")
	return result

if __name__ == '__main__':
	pass
