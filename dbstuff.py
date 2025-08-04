from __future__ import annotations
import os
import pandas as pd
from datetime import datetime
from typing import Optional, List
from loguru import logger
import sqlalchemy
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import combinations

from utils import ensure_datetime
from utils import get_directory_size, get_subfilecount, get_subdircount

# find dupes
# sqlite3 gitrepo.db 'select gitrepo.github_repo_name, count(git_path.git_path) as count,gitrepo.git_url from gitrepo inner join git_path on gitrepo.id=git_path.gitrepo_id group by github_repo_name order by count ' -table

BLANK_REPO_DATA = {
	"id": None,
	"node_id": None,
	"name": 'BLANK_REPO_DATA',
	"full_name": 'BLANK_REPO_DATA',
	"owner": {"login": 'BLANK_REPO_DATA'},
	"private": False,
	"html_url": "https://github.com/",
	"description": "Repository BLANK_REPO_DATA",
	"fork": False,
	"url": 'BLANK_REPO_DATA',
	"created_at": None,
	"updated_at": None,
	"pushed_at": None,
	"git_url": "git://github.com/BLANK_REPO_DATA.git",
	"ssh_url": "git@github.com:BLANK_REPO_DATA.git",
	"clone_url": "https://github.com/BLANK_REPO_DATA.git",
	"svn_url": "https://github.com/BLANK_REPO_DATA",
	"homepage": None,
	"size": 0,
	"stargazers_count": 0,
	"watchers_count": 0,
	"language": None,
	"forks_count": 0,
	"forks": 0,
	"open_issues_count": 0,
	"open_issues": 0,
	"watchers": 0,
	"default_branch": "main",
	"temp_clone_token": None,
	"network_count": 0,
	"subscribers_count": 0,
	"archived": False,
	"disabled": False,
	"license": None,
	"topics": [],
	"visibility": "unknown",
	"error_code": -1,
	"_unavailable": True}  # Flag to indicate this is default data

class MissingConfigException(Exception):
	pass

class MissingGitFolderException(Exception):
	pass

class Base(DeclarativeBase):
	pass

class GitFolder(Base):
	""" A folder containing one git repo """
	__tablename__ = 'git_path'
	id: Mapped[int] = mapped_column(primary_key=True)
	gitrepo_id = Column(Integer, ForeignKey('gitrepo.id'))
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
	dupe_count = Column('dupe_count', BigInteger)
	valid = Column(Boolean, default=True)
	scanned = Column(Boolean, default=False)  # Fixed this line
	is_starred = Column(Boolean, default=False)
	star_id = Column(Integer, ForeignKey('gitstars.id'), nullable=True)
	list_id = Column(Integer, ForeignKey('gitlists.id'), nullable=True)

	# Relationships
	repo: Mapped["GitRepo"] = relationship("GitRepo", back_populates="git_folders")
	# star_entry: Mapped[Optional["GitStar"]] = relationship("GitStar", foreign_keys=[star_id])
	# list_entry: Mapped[Optional["GitList"]] = relationship("GitList", foreign_keys=[list_id])
	star_entry: Mapped[Optional["GitStar"]] = relationship(
		"GitStar",
		foreign_keys=[star_id],
		overlaps="git_list"  # Avoid relationship conflicts
	)
	list_entry: Mapped[Optional["GitList"]] = relationship(
		"GitList",
		foreign_keys=[list_id],
		overlaps="starred_repos"  # Avoid relationship conflicts
	)

	def __init__(self, git_path: str, gitrepo_id):
		self.git_path = str(git_path)
		self.gitrepo_id = gitrepo_id
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.scan_count = 0
		self.dupe_count = 0
		self.is_parent = False
		self.folder_size = 0
		self.file_count = 0
		self.subdir_count = 0
		self.scanned = False
		self.get_folder_stats()

	def __repr__(self):
		return f'<GitFolder {self.id} git_path={self.git_path}>'

	def get_folder_time(self):
		""" Get stats for this git_path"""
		if not os.path.exists(self.git_path):  # redundant check, but just in case?
			self.valid = False
			logger.error(f'{self} does not exist')
			return
		# t0 = datetime.now()
		self.last_scan = datetime.now()

	def get_folder_stats(self):
		t0 = datetime.now()
		if not os.path.exists(self.git_path):  # redundant check, but just in case?
			self.valid = False
			logger.error(f'{self} does not exist')
			return
		self.folder_size = get_directory_size(self.git_path)
		self.file_count = get_subfilecount(self.git_path)
		self.subdir_count = get_subdircount(self.git_path)
		self.scan_time = (datetime.now() - t0).total_seconds()
		self.last_scan = datetime.now()
		self.scanned = True
		self.scan_count += 1
		stat = os.stat(self.git_path)
		self.git_path_ctime = ensure_datetime(datetime.fromtimestamp(stat.st_ctime))
		self.git_path_atime = ensure_datetime(datetime.fromtimestamp(stat.st_atime))
		self.git_path_mtime = ensure_datetime(datetime.fromtimestamp(stat.st_mtime))

# todo: make this better, should only be linked to one git_path
class GitRepo(Base):
	""" A git repo, linked to one or more git_paths """
	__tablename__ = 'gitrepo'

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
	dupe_count = Column('dupe_count', BigInteger)
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_count = Column('scan_count', Integer)
	error_code = Column('error_code', Integer)
	config_ctime = Column('config_ctime', DateTime)
	config_atime = Column('config_atime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	valid = Column(Boolean, default=True)
	scanned = Column(Boolean, default=False)  # Fixed this line

	is_starred = Column('is_starred', Boolean, default=False)
	starred_at = Column('starred_at', DateTime, nullable=True)

	# Relationships - specify foreign_keys explicitly to resolve ambiguity
	git_folders: Mapped[List["GitFolder"]] = relationship("GitFolder", back_populates="repo")
	star_entry: Mapped[Optional["GitStar"]] = relationship(
		"GitStar",
		back_populates="repo",
		uselist=False,
		foreign_keys="GitStar.gitrepo_id"  # Specify which FK to use
	)

	def __init__(self, git_url, local_path, repo_data=None):
		# Initialize with minimal information
		self.git_url = git_url
		self.local_path = local_path
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_count = 0
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

		self.update_config_times()

		# If repo_data is provided (from GitHub API), populate additional fields
		if repo_data and isinstance(repo_data, dict):
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

	def update_config_times(self):
		""" Update the config_ctime, config_atime, config_mtime based on the local git config file """
		if self.local_path == '[notcloned]':
			self.config_ctime = None
			self.config_atime = None
			self.config_mtime = None
			return
		if not os.path.exists(self.local_path):
			logger.error(f'Local path {self.local_path} does not exist')
			return
		config_path = os.path.join(self.local_path, '.git', 'config')
		if not os.path.exists(config_path):
			logger.error(f'Git config file {config_path} does not exist')
			return
		stat = os.stat(config_path)
		self.config_ctime = ensure_datetime(datetime.fromtimestamp(stat.st_ctime))
		self.config_atime = ensure_datetime(datetime.fromtimestamp(stat.st_atime))
		self.config_mtime = ensure_datetime(datetime.fromtimestamp(stat.st_mtime))

class CacheEntry(Base):
	""" A table for storing cache data from GitHub API responses """
	__tablename__ = 'cache_entries'

	id: Mapped[int] = mapped_column(primary_key=True)
	cache_key = Column('cache_key', String(255), unique=True)  # Unique identifier for this cache entry
	cache_type = Column('cache_type', String(50))  # Type of cache (starred_repos, repo_metadata, etc)
	data = Column('data', String(10485760))  # JSON data stored as string (10MB limit)
	timestamp = Column('timestamp', DateTime)  # When this entry was created/updated
	last_scan = Column('last_scan', DateTime)  # When this entry was last scanned

	def __init__(self, cache_key, cache_type, data):
		self.cache_key = cache_key
		self.cache_type = cache_type
		self.data = data
		self.timestamp = datetime.now()
		self.last_scan = datetime.now()

	def __repr__(self):
		return f'<CacheEntry {self.id} key={self.cache_key} type={self.cache_type}>'

class RepoInfo:
	def __init__(self, owner, name):
		self.github_owner = owner
		self.github_repo_name = name

class GitStar(Base):
	"""A starred repo, linked to a GitRepo"""
	__tablename__ = 'gitstars'
	id: Mapped[int] = mapped_column(primary_key=True)
	gitrepo_id = Column(Integer, ForeignKey('gitrepo.id'), unique=True)
	# Link to lists that contain this starred repo
	gitlist_id = Column('gitlist_id', Integer, ForeignKey('gitlists.id'), nullable=True)
	starred_at = Column(DateTime)
	stargazers_count = Column(Integer)
	description = Column(String(1024))
	full_name = Column(String(255))
	html_url = Column(String(255))

	# Relationships - specify foreign_keys explicitly
	repo: Mapped["GitRepo"] = relationship(
		"GitRepo",
		back_populates="star_entry",
		foreign_keys=[gitrepo_id]
	)
	# git_list: Mapped[Optional["GitList"]] = relationship("GitList", back_populates="starred_repos")
	git_list: Mapped[Optional["GitList"]] = relationship("GitList", back_populates="starred_repos", foreign_keys=[gitlist_id])

class GitList(Base):
	"""A starred repo list, containing multiple GitStars"""
	__tablename__ = 'gitlists'
	id: Mapped[int] = mapped_column(primary_key=True)
	list_name = Column(String(255))
	list_description = Column(String(1024))
	list_url = Column(String(255))
	repo_count = Column(Integer, default=0)
	created_at = Column('created_at', DateTime, default=datetime.now)

	# Relationships - one list can contain many starred repos
	# starred_repos: Mapped[List["GitStar"]] = relationship("GitStar", back_populates="git_list")
	starred_repos: Mapped[List["GitStar"]] = relationship("GitStar", back_populates="git_list", foreign_keys="GitStar.gitlist_id")

# Add relationships to GitRepo and GitStar
# GitRepo.star_entry = relationship("GitStar", back_populates="repo", uselist=False)
# GitStar.lists = relationship("GitList", back_populates="star")

class RepoCacheExpanded(Base):
	__tablename__ = 'repo_cache_expanded'
	id: Mapped[int] = mapped_column(primary_key=True)
	repo_id = Column(BigInteger, unique=True)
	node_id = Column(String(255))
	name = Column(String(255))
	full_name = Column(String(255))
	private = Column(Boolean)
	owner_login = Column(String(255))
	owner_id = Column(BigInteger)
	owner_node_id = Column(String(255))
	owner_avatar_url = Column(String(255))
	html_url = Column(String(255))
	description = Column(String(1024))
	fork = Column(Boolean)
	url = Column(String(255))
	created_at = Column(DateTime)
	updated_at = Column(DateTime)
	pushed_at = Column(DateTime)
	git_url = Column(String(255))
	ssh_url = Column(String(255))
	clone_url = Column(String(255))
	svn_url = Column(String(255))
	homepage = Column(String(255))
	size = Column(BigInteger)
	stargazers_count = Column(Integer)
	watchers_count = Column(Integer)
	language = Column(String(100))
	has_issues = Column(Boolean)
	has_projects = Column(Boolean)
	has_downloads = Column(Boolean)
	has_wiki = Column(Boolean)
	has_pages = Column(Boolean)
	has_discussions = Column(Boolean)
	forks_count = Column(Integer)
	archived = Column(Boolean)
	disabled = Column(Boolean)
	open_issues_count = Column(Integer)
	license_key = Column(String(100))
	license_name = Column(String(255))
	license_url = Column(String(255))
	allow_forking = Column(Boolean)
	is_template = Column(Boolean)
	web_commit_signoff_required = Column(Boolean)
	topics = Column(String(1024))
	visibility = Column(String(50))
	forks = Column(Integer)
	open_issues = Column(Integer)
	watchers = Column(Integer)
	default_branch = Column(String(100))
	network_count = Column(Integer)
	subscribers_count = Column(Integer)
	# Add more fields as needed

	def __init__(self, repo_json):
		self.repo_id = repo_json.get("id")
		self.node_id = repo_json.get("node_id")
		self.name = repo_json.get("name")
		self.full_name = repo_json.get("full_name")
		self.private = repo_json.get("private")
		owner = repo_json.get("owner", {})
		self.owner_login = owner.get("login")
		self.owner_id = owner.get("id")
		self.owner_node_id = owner.get("node_id")
		self.owner_avatar_url = owner.get("avatar_url")
		self.html_url = repo_json.get("html_url")
		self.description = repo_json.get("description")
		self.fork = repo_json.get("fork")
		self.url = repo_json.get("url")
		self.created_at = ensure_datetime(repo_json.get("created_at"))
		self.updated_at = ensure_datetime(repo_json.get("updated_at"))
		self.pushed_at = ensure_datetime(repo_json.get("pushed_at"))
		self.git_url = repo_json.get("git_url")
		self.ssh_url = repo_json.get("ssh_url")
		self.clone_url = repo_json.get("clone_url")
		self.svn_url = repo_json.get("svn_url")
		self.homepage = repo_json.get("homepage")
		self.size = repo_json.get("size")
		self.stargazers_count = repo_json.get("stargazers_count")
		self.watchers_count = repo_json.get("watchers_count")
		self.language = repo_json.get("language")
		self.has_issues = repo_json.get("has_issues")
		self.has_projects = repo_json.get("has_projects")
		self.has_downloads = repo_json.get("has_downloads")
		self.has_wiki = repo_json.get("has_wiki")
		self.has_pages = repo_json.get("has_pages")
		self.has_discussions = repo_json.get("has_discussions")
		self.forks_count = repo_json.get("forks_count")
		self.archived = repo_json.get("archived")
		self.disabled = repo_json.get("disabled")
		self.open_issues_count = repo_json.get("open_issues_count")
		self.allow_forking = repo_json.get("allow_forking")
		self.is_template = repo_json.get("is_template")
		self.web_commit_signoff_required = repo_json.get("web_commit_signoff_required")
		self.topics = ",".join(repo_json.get("topics", []))
		self.visibility = repo_json.get("visibility")
		self.forks = repo_json.get("forks")
		self.open_issues = repo_json.get("open_issues")
		self.watchers = repo_json.get("watchers")
		self.default_branch = repo_json.get("default_branch")
		self.network_count = repo_json.get("network_count")
		self.subscribers_count = repo_json.get("subscribers_count")

		license = repo_json.get("license") or {}
		self.license_key = license.get("key")
		self.license_name = license.get("name")
		self.license_url = license.get("url")


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
		return create_engine(f'sqlite:///{args.db_file}', echo=False, connect_args={'check_same_thread': False})
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

def check_git_dates(session, create_heatmap=False):
	df = pd.DataFrame(session.execute(text('select git_path.git_path, git_path.git_path_ctime, git_path.git_path_atime, git_path.git_path_mtime, gitrepo.created_at, gitrepo.updated_at, gitrepo.pushed_at from git_path inner join gitrepo on git_path.gitrepo_id=gitrepo.id ')).tuples())
	# Convert timestamp columns to datetime if not already
	timestamp_columns = ['git_path_ctime', 'git_path_atime', 'git_path_mtime', 'created_at', 'updated_at', 'pushed_at']
	for col in timestamp_columns:
		df[col] = pd.to_datetime(df[col])
	# Calculate time differences (in days)
	df['mtime_to_ctime'] = (df['git_path_ctime'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
	df['atime_to_mtime'] = (df['git_path_atime'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
	df['mtime_to_updated_at'] = (df['updated_at'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
	df['mtime_to_pushed_at'] = (df['pushed_at'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
	df['created_at_to_pushed_at'] = (df['pushed_at'] - df['created_at']).dt.total_seconds() / (60 * 60 * 24)

	# Display the differences
	print(df[['git_path', 'mtime_to_ctime', 'atime_to_mtime', 'mtime_to_updated_at', 'mtime_to_pushed_at', 'created_at_to_pushed_at']].head())

	# Compute correlation matrix for timestamps
	correlation_matrix = df[timestamp_columns].corr()
	print(correlation_matrix)

	# Group by a simplified path (e.g., extract project name) and analyze
	df['project'] = df['git_path'].str.split('/').str[-1]
	print(df.groupby('project')[timestamp_columns].mean())

	if create_heatmap:
		# Create a list of all unique pairs of timestamp columns
		pairs = list(combinations(timestamp_columns, 2))

		# Compute absolute time differences (in days) for each pair
		diff_data = []
		for index, row in df.iterrows():
			row_diffs = {}
			for col1, col2 in pairs:
				pair_name = f"{col1}_to_{col2}"
				diff = abs((row[col1] - row[col2]).total_seconds() / (60 * 60 * 24))  # Convert to days
				row_diffs[pair_name] = diff
			diff_data.append(row_diffs)
		# Create a DataFrame of differences, indexed by git_path
		diff_df = pd.DataFrame(diff_data, index=df['git_path'])
		# Create a heatmap
		plt.figure(figsize=(12, 8))
		sns.heatmap(diff_df, annot=False, fmt=".1f", cmap="YlOrRd", cbar_kws={'label': 'Days'})
		plt.title('Heatmap of Absolute Time Differences Between Timestamps')
		plt.xlabel('Timestamp Pairs')
		plt.ylabel('Repository Path')
		plt.xticks(rotation=45, ha='right')
		plt.tight_layout()

		# Save or display the heatmap
		plt.savefig('timestamp_differences_heatmap.png')
		plt.show()

def mark_repo_as_starred(session, repo_id, list_name=None):
	"""Mark a repository as starred and optionally link to a list"""
	git_repo = session.query(GitRepo).filter(GitRepo.id == repo_id).first()
	if not git_repo:
		return False

	git_repo.is_starred = True
	git_repo.starred_at = datetime.now()

	# Create GitStar entry if it doesn't exist
	git_star = session.query(GitStar).filter(GitStar.gitrepo_id == repo_id).first()
	if not git_star:
		git_star = GitStar()
		git_star.gitrepo_id = repo_id
		git_star.starred_at = datetime.now()
		session.add(git_star)
		session.flush()

	# Link to list if provided
	if list_name:
		git_list = session.query(GitList).filter(GitList.list_name == list_name).first()
		if git_list:
			git_star.gitlist_id = git_list.id

	session.commit()
	return True


if __name__ == '__main__':
	pass

