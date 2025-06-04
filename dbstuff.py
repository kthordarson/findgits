from __future__ import annotations
import aiohttp
import asyncio
import aiofiles
import os
import json
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
from utils import valid_git_folder, get_remote, flatten
from gitstars import get_git_stars, get_git_lists, update_repo_cache, get_auth_param, CACHE_DIR
import requests
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

		# If path is a real location, get local stats
		if local_path != '[notcloned]':
			self.get_repo_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} git_url: {self.git_url} localpath: {self.local_path} owner: {self.github_owner} name: {self.github_repo_name}>'

	def get_repo_stats(self):
		"""Collect stats and read config for this git repo"""
		# Existing implementation...

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

async def insert_update_starred_repo(github_repo, session):
	"""
	Insert a new GitRepo or update an existing one in the database

	Parameters:
		github_repo: repository object from GitHub API or owner/repo string
		session: SQLAlchemy session
	"""
	git_folder_path = '[notcloned]'

	# Check if github_repo is a string (URL or owner/repo) or a dict
	if isinstance(github_repo, dict):
		# Already have full repo data
		repo_data = github_repo
		remote_url = repo_data.get('html_url') or f"https://github.com/{repo_data.get('full_name')}"
	else:
		# Normalize the path by removing leading/trailing slashes
		clean_path = github_repo.strip('/')

		# It's a string, construct proper GitHub URL
		if 'github.com' in clean_path:
			# It's already a URL
			if clean_path.startswith('http'):
				remote_url = clean_path
			else:
				remote_url = f'https://{clean_path}'
		else:
			# It's just an owner/repo path
			remote_url = f'https://github.com/{clean_path}'

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()

	if not git_repo:
		# Get full repository data from GitHub API
		repo_data = await update_repo_cache(clean_path)
		# Create new repository with all available data
		git_repo = GitRepo(remote_url, git_folder_path, repo_data)
		git_repo.scan_count = 1
		session.add(git_repo)
		logger.debug(f'Created new GitRepo with full data: {git_repo} remote_url: {remote_url}')
	else:
		logger.info(f'update GitRepo: {git_repo} remote_url: {remote_url}')
		# Update existing repository if we have repo_data
		if repo_data:
			git_repo.last_scan = datetime.now()
			git_repo.scan_count += 1

			# Update with API data
			if repo_data.get('description'):
				git_repo.description = repo_data.get('description')
			if repo_data.get('stargazers_count'):
				git_repo.stargazers_count = repo_data.get('stargazers_count')
			if repo_data.get('language'):
				git_repo.language = repo_data.get('language')

			# Add other fields you want to update
			logger.debug(f'Updated existing GitRepo with API data: {git_repo.github_repo_name}')

	# Save changes
	session.commit()
	return git_repo

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

def xinsert_update_starred_repo(github_repo_data, session):
	"""
	Insert a new GitRepo or update an existing one in the database from GitHub API data

	Parameters:
		github_repo_data: Repository object from GitHub API
		session: SQLAlchemy session
	"""
	remote_url = github_repo_data.get('clone_url') or github_repo_data.get('html_url')
	if not remote_url:
		logger.warning(f"Could not determine URL for repo: {github_repo_data.get('full_name')}")
		return None

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()

	if not git_repo:
		git_repo = GitRepo(remote_url, '[notcloned]', github_repo_data)
		git_repo.scan_count = 1
		session.add(git_repo)
		logger.debug(f'Created new GitRepo from API data: {git_repo.github_repo_name}')
	else:
		# Update with latest data
		git_repo.last_scan = datetime.now()
		git_repo.scan_count += 1

		# Update fields from API data
		if github_repo_data.get('description'):
			git_repo.description = github_repo_data.get('description')
		# Update more fields as needed...

	session.commit()
	return git_repo

async def populate_starred_repos(session, max_pages=90, use_cache=True):
	"""
	Update existing GitRepo entries in database with detailed information from GitHub starred repositories.

	Parameters:
		session: SQLAlchemy session
		max_pages: Maximum number of pages to fetch (0 for all)
		use_cache: Whether to use cached data if available

	Returns:
		dict: Summary statistics about the operation
	"""
	from datetime import datetime
	import os

	# Fetch starred repositories from GitHub API
	logger.info(f"Fetching starred repositories (max_pages={max_pages}, use_cache={use_cache})")
	starred_repos, stars_dict = await get_git_stars(max=max_pages, use_cache=use_cache)

	stats = {
		"total_db_repos": 0,
		"total_starred_repos": len(starred_repos),
		"matched": 0,
		"updated": 0,
		"not_found": 0,
		"errors": 0
	}

	# Create lookup dictionaries for faster matching
	github_repos_by_url = {}
	github_repos_by_name = {}

	for repo_data in starred_repos:
		# Index by various URLs
		if repo_data.get('clone_url'):
			github_repos_by_url[repo_data['clone_url']] = repo_data
		if repo_data.get('html_url'):
			github_repos_by_url[repo_data['html_url']] = repo_data
		if repo_data.get('ssh_url'):
			github_repos_by_url[repo_data['ssh_url']] = repo_data
		if repo_data.get('git_url'):
			github_repos_by_url[repo_data['git_url']] = repo_data

		# Index by name
		if repo_data.get('full_name'):
			github_repos_by_name[repo_data['full_name'].lower()] = repo_data
		if repo_data.get('name'):
			github_repos_by_name[repo_data['name'].lower()] = repo_data

	# Process all repositories in the database
	try:
		db_repos = session.query(GitRepo).all()
		stats["total_db_repos"] = len(db_repos)
		logger.info(f"Processing {stats['total_db_repos']} database repositories github_repos_by_url:{len(github_repos_by_url)}")

		for db_repo in db_repos:
			try:
				# Try to find matching GitHub data
				repo_data = None

				# Try matching by URL first
				if db_repo.git_url and db_repo.git_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.git_url} {stats["matched"]}')
				if db_repo.git_url and db_repo.git_url.replace('.git','') in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url.replace('.git','')]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.git_url.replace('.git','')} {stats["matched"]}')
				elif db_repo.clone_url and db_repo.clone_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.clone_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.clone_url} {stats["matched"]}')
				elif db_repo.html_url and db_repo.html_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.html_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.html_url} {stats["matched"]}')
				elif db_repo.ssh_url and db_repo.ssh_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.ssh_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.ssh_url} {stats["matched"]}')

				# Then try by name
				elif db_repo.full_name and db_repo.full_name.lower() in github_repos_by_name:
					repo_data = github_repos_by_name[db_repo.full_name.lower()]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.full_name.lower()} {stats["matched"]}')
				elif db_repo.github_repo_name:
					# logger.debug(f"Trying to match by name: {db_repo.github_repo_name} {stats['matched']}")
					# Try owner/name format
					full_name = f"{db_repo.github_owner}/{db_repo.github_repo_name}".lower()
					if full_name in github_repos_by_name:
						repo_data = github_repos_by_name[full_name]
						stats["matched"] += 1
						# logger.debug(f"match by name: {db_repo.github_repo_name} {len(repo_data)} {stats['matched']}")
					# Try just the name
					elif db_repo.github_repo_name.lower() in github_repos_by_name:
						repo_data = github_repos_by_name[db_repo.github_repo_name.lower()]
						stats["matched"] += 1
						# logger.debug(f"match by name: {db_repo.github_repo_name} {len(repo_data)} {stats['matched']}")

				# If we found matching data, update the database entry
				if repo_data:
					# Update the entry with all GitHub data
					db_repo.last_scan = datetime.now()
					db_repo.scan_count += 1

					# Update with all API data
					db_repo.node_id = repo_data.get('node_id')
					db_repo.full_name = repo_data.get('full_name')
					db_repo.private = repo_data.get('private')
					db_repo.html_url = repo_data.get('html_url')
					db_repo.description = repo_data.get('description')
					db_repo.fork = repo_data.get('fork')
					db_repo.clone_url = repo_data.get('clone_url')
					db_repo.ssh_url = repo_data.get('ssh_url')
					db_repo.git_url_api = repo_data.get('git_url')
					db_repo.svn_url = repo_data.get('svn_url')
					db_repo.homepage = repo_data.get('homepage')

					# Statistics
					db_repo.size = repo_data.get('size')
					db_repo.stargazers_count = repo_data.get('stargazers_count')
					db_repo.watchers_count = repo_data.get('watchers_count')
					db_repo.forks_count = repo_data.get('forks_count')
					db_repo.open_issues_count = repo_data.get('open_issues_count')
					db_repo.language = repo_data.get('language')

					# Repository features
					db_repo.has_issues = repo_data.get('has_issues')
					db_repo.has_projects = repo_data.get('has_projects')
					db_repo.has_downloads = repo_data.get('has_downloads')
					db_repo.has_wiki = repo_data.get('has_wiki')
					db_repo.has_pages = repo_data.get('has_pages')
					db_repo.has_discussions = repo_data.get('has_discussions')
					db_repo.archived = repo_data.get('archived')
					db_repo.disabled = repo_data.get('disabled')
					db_repo.allow_forking = repo_data.get('allow_forking')
					db_repo.is_template = repo_data.get('is_template')
					db_repo.web_commit_signoff_required = repo_data.get('web_commit_signoff_required')

					# Topics
					if repo_data.get('topics'):
						db_repo.topics = ','.join(repo_data.get('topics'))

					db_repo.visibility = repo_data.get('visibility')
					db_repo.default_branch = repo_data.get('default_branch')

					# License information
					if repo_data.get('license'):
						db_repo.license_key = repo_data.get('license', {}).get('key')
						db_repo.license_name = repo_data.get('license', {}).get('name')
						db_repo.license_url = repo_data.get('license', {}).get('url')

					# Try to parse timestamps
					try:
						if repo_data.get('created_at'):
							db_repo.created_at = datetime.strptime(repo_data.get('created_at'), '%Y-%m-%dT%H:%M:%SZ')
						if repo_data.get('updated_at'):
							db_repo.updated_at = datetime.strptime(repo_data.get('updated_at'), '%Y-%m-%dT%H:%M:%SZ')
						if repo_data.get('pushed_at'):
							db_repo.pushed_at = datetime.strptime(repo_data.get('pushed_at'), '%Y-%m-%dT%H:%M:%SZ')
					except ValueError as e:
						logger.warning(f"Error parsing timestamps: {e}")

					# Owner information if available
					if repo_data.get('owner') and isinstance(repo_data['owner'], dict):
						if not db_repo.github_owner:
							db_repo.github_owner = repo_data['owner'].get('login')

					stats["updated"] += 1
					# logger.debug(f"Updated repository: {db_repo.id} - {db_repo.github_repo_name}")
				else:
					# No matching GitHub data found
					stats["not_found"] += 1
					logger.warning(f"No GitHub data for id: {db_repo.id} db_repo: {db_repo}")

				# Commit periodically to avoid large transactions
				if (stats["updated"] + stats["not_found"]) % 100 == 0:
					session.commit()
					logger.warning(f"Progress: {stats['updated'] + stats['not_found']}/{stats['total_db_repos']} repositories processed")

			except Exception as e:
				logger.error(f"Error processing repo {db_repo.id} - {db_repo.github_repo_name}: {e}")
				stats["errors"] += 1

		# Final commit
		session.commit()
		logger.info(f"Finished processing repositories: {stats}")

	except Exception as e:
		logger.error(f"Error fetching database repositories: {e}")
		stats["errors"] += 1
		return {"errors": stats["errors"], "message": str(e)}

	return stats

async def fetch_missing_repo_data(session, update_all=False):
	"""
	Fetch GitHub API data for repositories that weren't found in starred repos
	"""

	stats = {
		"total_repos": 0,
		"processed": 0,
		"updated": 0,
		"failed": 0,
		"skipped": 0,
		"from_cache": 0
	}

	# Load cache first
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'
	cache_data = {'repos': []}

	if os.path.exists(stars_cache_file):
		try:
			async with aiofiles.open(stars_cache_file, 'r') as f:
				cache_content = await f.read()
				cache_data = json.loads(cache_content)
				logger.info(f"Loaded {len(cache_data['repos'])} repositories from cache")
		except Exception as e:
			logger.error(f"Failed to load cache: {e}")

	# Build lookup dictionary for cached repos
	cache_repos_by_name = {}
	for repo in cache_data.get('repos', []):
		if repo.get('full_name'):
			cache_repos_by_name[repo['full_name'].lower()] = repo
		if repo.get('name'):
			# Also index by name only as fallback
			cache_repos_by_name[repo['name'].lower()] = repo

	# Get GitHub auth token
	auth = get_auth_param()
	if not auth:
		logger.error('fetch_missing_repo_data: no auth provided')
		return stats

	# Setup GitHub API headers
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	# Query repos from database that need updating
	if update_all:
		db_repos = session.query(GitRepo).all()
	else:
		# Only get repos that are missing key GitHub data
		db_repos = session.query(GitRepo).filter((GitRepo.node_id.is_(None)) | (GitRepo.full_name.is_(None)) | (GitRepo.description.is_(None))).all()

	stats["total_repos"] = len(db_repos)
	logger.info(f"Found {stats['total_repos']} repositories that need GitHub data")

	# Process repositories in batches for efficiency
	batch_size = 10
	for i in range(0, len(db_repos), batch_size):
		batch = db_repos[i:i+batch_size]
		tasks = []

		async with aiohttp.ClientSession() as api_session:
			for repo in batch:
				tasks.append(process_repo(repo, api_session, headers, cache_repos_by_name, cache_data, stats, session))

			await asyncio.gather(*tasks)

			# Commit after each batch
			session.commit()
			logger.info(f"Progress: {i+len(batch)}/{stats['total_repos']} repositories processed")

	# Final cache update
	try:
		if not os.path.exists(CACHE_DIR):
			os.makedirs(CACHE_DIR)

		async with aiofiles.open(stars_cache_file, 'w') as f:
			cache_data['timestamp'] = str(datetime.now())
			await f.write(json.dumps(cache_data, indent=4))
			logger.info("Final update to cache file with new data")
	except Exception as e:
		logger.error(f"Failed to update cache: {e}")

	# Final commit
	session.commit()
	logger.info(f"Finished fetching repository data: {stats}")

	return stats

async def process_repo(repo, api_session, headers, cache_repos_by_name, cache_data, stats, session):
	"""Helper function to process a single repository"""
	stats["processed"] += 1

	# Skip if no owner/name information is available
	if not repo.github_owner or not repo.github_repo_name:
		stats["skipped"] += 1
		logger.warning(f"Skipping repo with missing owner/name: {repo}")
		return

	# Construct the repository path
	repo_path = f"{repo.github_owner}/{repo.github_repo_name}"
	api_url = f"https://api.github.com/repos/{repo_path}"

	# First check if repo exists in cache
	repo_data = None
	repo_key = repo_path.lower()

	if repo_key in cache_repos_by_name:
		repo_data = cache_repos_by_name[repo_key]
		stats["from_cache"] += 1
	else:
		# Not in cache, make API request
		logger.info(f"Fetching GitHub data for {repo_path} ({stats['processed']}/{stats['total_repos']})")

		try:
			# Make the API request
			async with api_session.get(api_url, headers=headers) as response:
				if response.status == 200:
					repo_data = await response.json()

					# Add to cache
					cache_data['repos'].append(repo_data)
					cache_repos_by_name[repo_path.lower()] = repo_data

				elif response.status == 404:
					logger.warning(f"Repository not found on GitHub: {repo_path} (possibly private, renamed or deleted)")
					stats["failed"] += 1
					return
				else:
					error_text = await response.text()
					logger.error(f"GitHub API error ({response.status}): {error_text}")
					stats["failed"] += 1
					return

		except Exception as e:
			logger.error(f"Error processing {repo_path}: {e}")
			stats["failed"] += 1
			return

	# If we have data (either from cache or API), update the repository
	if repo_data:
		try:
			# Update repository with data
			update_repo_from_data(repo, repo_data)
			stats["updated"] += 1

		except Exception as e:
			logger.error(f"Error updating repository {repo_path}: {e}")
			stats["failed"] += 1

def update_repo_from_data(repo, repo_data):
	"""Update a repository with data from GitHub API"""
	repo.last_scan = datetime.now()
	repo.scan_count += 1

	# Update all fields with API data
	repo.node_id = repo_data.get('node_id')
	repo.full_name = repo_data.get('full_name')
	repo.private = repo_data.get('private')
	repo.html_url = repo_data.get('html_url')
	repo.description = repo_data.get('description')
	repo.fork = repo_data.get('fork')
	repo.clone_url = repo_data.get('clone_url')
	repo.ssh_url = repo_data.get('ssh_url')
	repo.git_url_api = repo_data.get('git_url')
	repo.svn_url = repo_data.get('svn_url')
	repo.homepage = repo_data.get('homepage')

	# Statistics
	repo.size = repo_data.get('size')
	repo.stargazers_count = repo_data.get('stargazers_count')
	repo.watchers_count = repo_data.get('watchers_count')
	repo.forks_count = repo_data.get('forks_count')
	repo.open_issues_count = repo_data.get('open_issues_count')
	repo.language = repo_data.get('language')

	# Repository features
	repo.has_issues = repo_data.get('has_issues')
	repo.has_projects = repo_data.get('has_projects')
	repo.has_downloads = repo_data.get('has_downloads')
	repo.has_wiki = repo_data.get('has_wiki')
	repo.has_pages = repo_data.get('has_pages')
	repo.has_discussions = repo_data.get('has_discussions')
	repo.archived = repo_data.get('archived')
	repo.disabled = repo_data.get('disabled')
	repo.allow_forking = repo_data.get('allow_forking')
	repo.is_template = repo_data.get('is_template')
	repo.web_commit_signoff_required = repo_data.get('web_commit_signoff_required')

	# Topics
	if repo_data.get('topics'):
		repo.topics = ','.join(repo_data.get('topics'))

	repo.visibility = repo_data.get('visibility')
	repo.default_branch = repo_data.get('default_branch')

	# License information
	if repo_data.get('license'):
		repo.license_key = repo_data.get('license', {}).get('key')
		repo.license_name = repo_data.get('license', {}).get('name')
		repo.license_url = repo_data.get('license', {}).get('url')

	# Parse timestamps
	try:
		if repo_data.get('created_at'):
			repo.created_at = datetime.strptime(repo_data.get('created_at'), '%Y-%m-%dT%H:%M:%SZ')
		if repo_data.get('updated_at'):
			repo.updated_at = datetime.strptime(repo_data.get('updated_at'), '%Y-%m-%dT%H:%M:%SZ')
		if repo_data.get('pushed_at'):
			repo.pushed_at = datetime.strptime(repo_data.get('pushed_at'), '%Y-%m-%dT%H:%M:%SZ')
	except ValueError as e:
		logger.warning(f"Error parsing timestamps: {e}")

if __name__ == '__main__':
	pass
