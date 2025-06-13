from __future__ import annotations
import os
from datetime import datetime
from typing import List
from loguru import logger
import sqlalchemy
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from utils import ensure_datetime
from utils import get_directory_size, get_subfilecount, get_subdircount
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
		self.get_folder_stats()

	def __repr__(self):
		return f'<GitFolder {self.id} git_path={self.git_path}>'

	def get_folder_time(self):
		""" Get stats for this git_path"""
		if not os.path.exists(self.git_path):  # redundant check, but just in case?
			self.valid = False
			raise MissingGitFolderException(f'{self} does not exist')
		# t0 = datetime.now()
		self.last_scan = datetime.now()
		stat = os.stat(self.git_path)
		self.git_path_ctime = ensure_datetime(datetime.fromtimestamp(stat.st_ctime))
		self.git_path_atime = ensure_datetime(datetime.fromtimestamp(stat.st_atime))
		self.git_path_mtime = ensure_datetime(datetime.fromtimestamp(stat.st_mtime))

	def get_folder_stats(self):
		t0 = datetime.now()
		self.folder_size = get_directory_size(self.git_path)
		self.file_count = get_subfilecount(self.git_path)
		self.subdir_count = get_subdircount(self.git_path)
		self.scan_time = (datetime.now() - t0).total_seconds()
		self.last_scan = datetime.now()
		self.scanned = True
		self.scan_count += 1

# todo: make this better, should only be linked to one git_path
class GitRepo(Base):
	""" A git repo, linked to one or more git_paths """
	__tablename__ = 'gitrepo'
	__table_args__ = (
		sqlalchemy.UniqueConstraint('git_url', name='uix_git_url'),
	)

	id: Mapped[int] = mapped_column(primary_key=True)

	# Basic repository information
	git_url = Column('git_url', String(255))
	github_repo_name = Column('github_repo_name', String(255))
	github_owner = Column('github_owner', String(255))
	local_path = Column('local_path', String(255))
	remote = Column('remote', String(255))
	branch = Column('branch', String(255))

	# GitHub API fields
	node_id = Column('node_id', String(255))
	full_name = Column('full_name', String(255))
	private = Column('private', Boolean)
	html_url = Column('html_url', String(255))
	description = Column('description', String(1024))
	fork = Column('fork', Boolean)
	clone_url = Column('clone_url', String(255))
	ssh_url = Column('ssh_url', String(255))
	git_url_api = Column('git_url_api', String(255))
	svn_url = Column('svn_url', String(255))
	homepage = Column('homepage', String(255))

	# Statistics
	size = Column('size', BigInteger)
	stargazers_count = Column('stargazers_count', Integer)
	watchers_count = Column('watchers_count', Integer)
	forks_count = Column('forks_count', Integer)
	open_issues_count = Column('open_issues_count', Integer)
	language = Column('language', String(100))

	# Repository features
	has_issues = Column('has_issues', Boolean)
	has_projects = Column('has_projects', Boolean)
	has_downloads = Column('has_downloads', Boolean)
	has_wiki = Column('has_wiki', Boolean)
	has_pages = Column('has_pages', Boolean)
	has_discussions = Column('has_discussions', Boolean)
	archived = Column('archived', Boolean)
	disabled = Column('disabled', Boolean)
	allow_forking = Column('allow_forking', Boolean)
	is_template = Column('is_template', Boolean)
	web_commit_signoff_required = Column('web_commit_signoff_required', Boolean)

	# Topics - stored as comma-separated string
	topics = Column('topics', String(1024))
	visibility = Column('visibility', String(50))
	default_branch = Column('default_branch', String(100))

	# License information
	license_key = Column('license_key', String(100))
	license_name = Column('license_name', String(255))
	license_url = Column('license_url', String(255))

	# Timestamps
	created_at = Column('created_at', DateTime)
	updated_at = Column('updated_at', DateTime)
	pushed_at = Column('pushed_at', DateTime)

	# Our internal tracking fields
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

	# Relationships
	git_folders: Mapped[List["GitFolder"]] = relationship("GitFolder", back_populates="repo")

	def __init__(self, git_url, local_path, repo_data=None):
		# Initialize with minimal information
		self.git_url = git_url
		self.local_path = local_path
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_count = 0
		self.dupe_flag = False
		self.scanned = False

		# Extract repository name from URL
		if git_url.endswith('/'):
			git_url = git_url[:-1]
		self.github_repo_name = git_url.split('/')[-1].replace('.git', '')

		# Extract owner from URL
		if 'http' in git_url:
			self.github_owner = git_url.split('/')[-2]
		elif 'git@github.com' in git_url:
			self.github_owner = git_url.split(':')[1].split('/')[0]
		else:
			self.github_owner = '[unknown]'

		# If repo_data is provided (from GitHub API), populate additional fields
		if repo_data and isinstance(repo_data, dict):
			logger.debug(f'got repo_data for {self} from GitHub API')
			self.node_id = repo_data.get('node_id')
			self.full_name = repo_data.get('full_name')
			self.private = repo_data.get('private')
			self.html_url = repo_data.get('html_url')
			self.description = repo_data.get('description')
			self.fork = repo_data.get('fork')
			self.clone_url = repo_data.get('clone_url')
			self.ssh_url = repo_data.get('ssh_url')
			self.git_url_api = repo_data.get('git_url')
			self.svn_url = repo_data.get('svn_url')
			self.homepage = repo_data.get('homepage')

			# Statistics
			self.size = repo_data.get('size')
			self.stargazers_count = repo_data.get('stargazers_count')
			self.watchers_count = repo_data.get('watchers_count')
			self.forks_count = repo_data.get('forks_count')
			self.open_issues_count = repo_data.get('open_issues_count')
			self.language = repo_data.get('language')

			# Repository features
			self.has_issues = repo_data.get('has_issues')
			self.has_projects = repo_data.get('has_projects')
			self.has_downloads = repo_data.get('has_downloads')
			self.has_wiki = repo_data.get('has_wiki')
			self.has_pages = repo_data.get('has_pages')
			self.has_discussions = repo_data.get('has_discussions')
			self.archived = repo_data.get('archived')
			self.disabled = repo_data.get('disabled')
			self.allow_forking = repo_data.get('allow_forking')
			self.is_template = repo_data.get('is_template')
			self.web_commit_signoff_required = repo_data.get('web_commit_signoff_required')

			# Topics
			if repo_data.get('topics'):
				self.topics = ','.join(repo_data.get('topics'))

			self.visibility = repo_data.get('visibility')
			self.default_branch = repo_data.get('default_branch')

			# License information
			if repo_data.get('license'):
				self.license_key = repo_data.get('license', {}).get('key')
				self.license_name = repo_data.get('license', {}).get('name')
				self.license_url = repo_data.get('license', {}).get('url')

			# Try to parse timestamps
			try:
				if repo_data.get('created_at'):
					self.created_at = datetime.strptime(repo_data.get('created_at'), '%Y-%m-%dT%H:%M:%SZ')
				if repo_data.get('updated_at'):
					self.updated_at = datetime.strptime(repo_data.get('updated_at'), '%Y-%m-%dT%H:%M:%SZ')
				if repo_data.get('pushed_at'):
					self.pushed_at = datetime.strptime(repo_data.get('pushed_at'), '%Y-%m-%dT%H:%M:%SZ')
			except ValueError as e:
				logger.warning(f"Error parsing timestamps: {e}")

	def __repr__(self):
		return f'<GitRepo id={self.id} git_url: {self.git_url} localpath: {self.local_path} owner: {self.github_owner} name: {self.github_repo_name}>'

class CacheEntry(Base):
	""" A table for storing cache data from GitHub API responses """
	__tablename__ = 'cache_entries'

	id: Mapped[int] = mapped_column(primary_key=True)
	cache_key = Column('cache_key', String(255), unique=True)  # Unique identifier for this cache entry
	cache_type = Column('cache_type', String(50))  # Type of cache (starred_repos, repo_metadata, etc)
	data = Column('data', String(10485760))  # JSON data stored as string (10MB limit)
	timestamp = Column('timestamp', DateTime)  # When this entry was created/updated

	def __init__(self, cache_key, cache_type, data):
		self.cache_key = cache_key
		self.cache_type = cache_type
		self.data = data
		self.timestamp = datetime.now()

	def __repr__(self):
		return f'<CacheEntry {self.id} key={self.cache_key} type={self.cache_type}>'

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

def get_cache_entry(session, cache_key, cache_type):
	"""Get a cache entry from the database"""
	return session.query(CacheEntry).filter_by(cache_key=cache_key, cache_type=cache_type).first()

def set_cache_entry(session, cache_key, cache_type, data):
	"""Set or update a cache entry in the database"""
	entry = get_cache_entry(session, cache_key, cache_type)
	if entry:
		# Update existing entry
		entry.data = data
		entry.timestamp = datetime.now()
	else:
		# Create new entry
		entry = CacheEntry(cache_key, cache_type, data)
		session.add(entry)

	# The caller is responsible for committing the session
	return entry

if __name__ == '__main__':
	pass

