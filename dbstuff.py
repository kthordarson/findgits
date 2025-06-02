from __future__ import annotations
import glob
import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from pathlib import Path
# from typing import List
from subprocess import Popen, PIPE
from loguru import logger
import sqlalchemy
# from sqlalchemy import orm
from sqlalchemy import func
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session

from utils import (get_directory_size, get_subdircount, get_subfilecount, format_bytes)
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

# todo: remove git_path and use searchpath_id to get parent path string
class SearchPath(Base):
	__tablename__ = 'searchpath'
	id: Mapped[int] = mapped_column(primary_key=True)
	folder = Column('path', String(255))
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('scan_time', Float)
	scanned = Column[bool]
	scan_count = Column('scan_count', Integer)
	repo_count = Column('repo_count', Integer)
	folder_count = Column(Integer)
	folder_size = Column(BigInteger)
	file_count = Column(BigInteger)

	def __repr__(self):
		return f'<SearchPath {self.id} {self.folder}>'

	def __init__(self, folder: str):
		self.folder = folder
		self.scan_count = 0
		self.repo_count = 0
		self.folder_count = 0
		self.folder_size = 0
		self.file_count = 0

	def get_git_folders(self) -> dict:
		"""
		Scans this gitparentpath for all sub gitfolders
		Returns: dict with gitparentpath id, list of gitfolders and scantime
		"""
		t0 = datetime.now()
		self.last_scan = t0
		self.scanned = True
		git_folder_list = []
		for gitfolder in glob.glob(self.folder + '/**/.git', recursive=False, include_hidden=True):
			if Path(gitfolder).is_dir() and gitfolder != self.folder + '/':
				git_folder_list.append(Path(gitfolder).parent)
		self.scan_time = (datetime.now() - t0).total_seconds()
		logger.info(f'[gff] {self} {len(git_folder_list)} folders found in {self.folder} scan_time: {self.scan_time}')
		return {'SearchPath': self.id, 'res': git_folder_list, 'scan_time': self.scan_time}

class GitFolder(Base):
	""" A folder containing one git repo """
	__tablename__ = 'gitfolder'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	searchpath_id: Mapped[int] = mapped_column(ForeignKey('searchpath.id'))
	gitrepo_id = Column('gitrepo_id', Integer)  # id of gitrepo found in this folder
	git_path = Column('git_path', String(255))
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_count = Column('scan_count', Integer)
	scan_time = Column('scan_time', Float)
	folder_size = Column('folder_size', BigInteger)
	file_count = Column('file_count', BigInteger)
	subdir_count = Column('subdir_count', BigInteger)

	gitfolder_ctime = Column('gitfolder_ctime', DateTime)
	gitfolder_atime = Column('gitfolder_atime', DateTime)
	gitfolder_mtime = Column('gitfolder_mtime', DateTime)
	dupe_flag = Column('dupe_flag', Boolean)
	dupe_count = Column('dupe_count', BigInteger)
	valid = Column(Boolean, default=True)
	scanned = Column[bool]

	def __init__(self, gitfolder: str, searchpath: SearchPath, gitrepo_id):
		self.git_path = str(gitfolder)
		self.searchpath_id = searchpath.id
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
		return f'<GitFolder {self.id} gitpath={self.git_path}>'

	def scan_subfolders(self):
		"""
		scan subfolders for more git repos, if found, tag as parent
		todo make that folder a GPP and add to db
		"""
		sub_git_folders = [str(Path(k).parent) for k in glob.glob(self.git_path + '/**/.git', recursive=True, include_hidden=True) if Path(k).is_dir()]
		if len(sub_git_folders) > 1:
			self.is_parent = True
			logger.info(f'{self.git_path} is a parent folder with {len(sub_git_folders)} subfolders]')

	def get_folder_time(self):
		""" Get stats for this gitfolder"""
		if not os.path.exists(self.git_path):  # redundant check, but just in case?
			self.valid = False
			raise MissingGitFolderException(f'{self} does not exist')
		t0 = datetime.now()
		self.last_scan = datetime.now()
		stat = os.stat(self.git_path)
		self.gitfolder_ctime = datetime.fromtimestamp(stat.st_ctime)
		self.gitfolder_atime = datetime.fromtimestamp(stat.st_atime)
		self.gitfolder_mtime = datetime.fromtimestamp(stat.st_mtime)

	def get_folder_stats(self, id, git_path):
		t0 = datetime.now()
		self.folder_size = get_directory_size(git_path)
		self.file_count = get_subfilecount(git_path)
		self.subdir_count = get_subdircount(git_path)
		scan_time = (datetime.now() - t0).total_seconds()
		self.scanned = True
		return {'id': id, 'folder_size': self.folder_size, 'file_count': self.file_count, 'subdir_count': self.subdir_count, 'scan_time': scan_time}


# todo: make this better, should only be linked to one gitfolder
class GitRepo(Base):
	""" A git repo, linked to one gitfolder  """
	__tablename__ = 'gitrepo'
	id: Mapped[int] = mapped_column(primary_key=True)
	searchpath_id: Mapped[int] = mapped_column(ForeignKey('searchpath.id'))
	git_url = Column('git_url', String(255))
	# git_path = Column('git_path', String(255))
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

	# git_path: Mapped[List["GitFolder"]] = relationship()
	# gitfolder = relationship("GitFolder", backref="git_path")

	def __init__(self, remoteurl):  # , gitfolder: GitFolder
		# self.git_path = gitfolder.git_path
		self.git_url = remoteurl
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_count = 0
		self.dupe_flag = False
		self.scanned = False
		self.get_repo_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} url: {self.git_url} >'

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
				self.valid = False
			if remote_section:
				self.remote = remote_section.split(' ')[1].replace('"', '')
				branch_section = None
				try:
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
				except IndexError as e:
					logger.warning(f'{e} {self} {conf.sections()}')
				if branch_section:
					self.branch = branch_section.split(' ')[1].replace('"', '')
				try:
					# git_url = [k for k in conf['remote "origin"'].items()][0][1]
					self.git_url = conf[remote_section]['url']
					if not self.git_url:
						logger.warning(f'missing git_url {self.git_path} git_config_file={git_config_file}')
						self.valid = False
				except TypeError as e:
					logger.warning(f'[!] {self} typeerror {e} git_config_file={git_config_file} ')
					self.valid = False
				except KeyError as e:
					logger.warning(f'[!] {self} KeyError {e} git_config_file={git_config_file}')
					self.valid = False
		else:
			logger.warning(f'missing config remote origin {self} git_config_file={git_config_file}')
			self.valid = False


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

def check_dupe_status(session) -> None:
	sql = text('select id, git_url,  count(*) as count from gitrepo group by git_url having count(*)>1;')
	# sql = text('select * from dupeview;')
	sqldupes = session.execute(sql).all()
	for dupe in sqldupes:
		dupe_repo = session.query(GitRepo).filter(GitRepo.id == dupe.id).filter(GitRepo.dupe_flag is False).first()
		if dupe_repo:
			dupe_repo.dupe_flag = True
			dupe_repo.dupe_count = dupe.count
			session.add(dupe_repo)
			dupe_folder = session.query(GitFolder).filter(GitFolder.dupe_flag is False).first()
			if dupe_folder:
				dupe_folder.dupe_flag = True
				dupe_folder.dupe_count = dupe.count
				session.add(dupe_folder)
	session.commit()

def db_get_dupes(session, repo_url):
	dupes = session.query(
		GitRepo.id.label('id'),
		GitRepo.git_url.label('git_url'),
		func.count(GitRepo.git_url).label("count")).group_by(GitRepo.git_url).having(func.count(GitRepo.git_url) > 1).order_by(func.count(GitRepo.git_url).desc()).filter(GitRepo.git_url == repo_url).all()
	return dupes


def db_dupe_info(session: Session, maxdupes=30) -> None:
	"""
	Get dupe info from db
	Parameters: session (sessionmaker) - sqlalchemy session, maxdupes int - max number of dupes to show
	Returns: None
	"""
	dupes = []
	total_dupes = 0
	try:
		dupes = session.query(
			GitRepo.id.label('id'),
			GitRepo.git_url.label('git_url'),
			func.count(GitRepo.git_url).label("count")). \
			group_by(GitRepo.git_url). \
			having(func.count(GitRepo.git_url) > 1). \
			order_by(func.count(GitRepo.git_url).desc()). \
			limit(maxdupes).all()
	except ProgrammingError as e:
		logger.error(e)
	if dupes == []:
		print('[db] No dupes')
	else:
		total_dupes = len(dupes)
		# for d in dupes:
		# dcount = session.query(GitRepo).filter(GitRepo.git_url == d.git_url).count()
		print(f"{'id': <5}{'pid': <5}{'repo': <55}{'dupes': <5}")
		for gpe in dupes:
			print(f'{gpe.id:<5}{gpe.git_url:<55}{gpe.count:<5}')
		print(f'{"=" * 20} {total_dupes}')


def get_db_info(session):
	git_parent_entries = session.query(SearchPath).all()  # filter(SearchPath.id == str(args.listpaths)).all()
	# slowscans = session.query(GitFolder).order_by(GitFolder.scan_time.desc()).limit(10).all()
	# git_parent_scantimesum = sum([k.scan_time for k in session.query(SearchPath).all()])
	# allfolderscantimesum = sum([k.scan_time for k in session.query(GitFolder).all()])
	total_size = 0
	# total_time = 0
	# print(f"{'gpe.id':<3}{'gpe.folder':<30}{'fc:<5'}{'rc:<5'}{'f_size':<10}{'f_scantime':<10}")
	print(f"{'id':<3}{'folder': <31}{'subfolders': <11} {'repos': <5} {'size': <10} {'scantime': <15}")
	for gpe in git_parent_entries:
		fc = session.query(GitFolder).filter(GitFolder.searchpath_id == gpe.id).count()
		f_size = sum([k.folder_size for k in session.query(GitFolder).filter(GitFolder.searchpath_id == gpe.id).all()])
		total_size += f_size
		# f_scantime = sum([k.scan_time for k in session.query(GitFolder).filter(GitFolder.searchpath_id == gpe.id).all()])
		# total_time += f_scantime
		rc = session.query(GitRepo).filter(GitRepo.searchpath_id == gpe.id).count()
		# scant = str(timedelta(seconds=f_scantime))
		try:
			gpscant = str(timedelta(seconds=gpe.scan_time))
		except TypeError as e:
			# logger.error(f'[gdbi] {e} {gpe}')
			gpscant = '0'
		print(f'{gpe.id:<3}{gpe.folder:<31}{fc:<11} {rc:<5} {format_bytes(f_size):<10} {gpscant:<14}')
	# tt = str(timedelta(seconds=total_time))
	print(f'{"=" * 90}')
	print(f'{format_bytes(total_size):>52} ')
	dupes = get_dupes(session)
	print(f'dupes: {len(dupes)}')
	for d in dupes:
		print(f'duperepo id={d.id} url {d.git_url} count: {d.count}')


def gitfolder_to_gitparent(gitfolder: GitFolder, session: Session) -> SearchPath:
	"""
	Convert a gitfolder to a gitparent. Used when gitfolder contains more than one git repo
	Removes the gitfolder and any repos from that folder from db, create a new SearchPath and return it
	Parameters: gitfolder (GitFolder) - gitfolder to convert
	"""
	repos_to_del = session.query(GitRepo).filter(GitRepo.id == gitfolder.id).all()
	for r in repos_to_del:
		logger.warning(f'[gtp] removing {r} from db')
		session.delete(r)
	session.delete(gitfolder)
	logger.warning(f'[gtp] removing {gitfolder} from db')
	# logger.debug(f'[gto] removed {gitfolder}')
	gpp = SearchPath(gitfolder.git_path)
	logger.info(f'[gtp] new {gpp} created')
	return gpp

def show_dupe_info(dupes, session: Session):
	"""
	show info about dupes
	Parameters: dupes: list - list of dupes
	"""
	dupe_counter = 0
	for d in dupes:
		repdupe = session.query(GitRepo).filter(GitRepo.git_url == d.git_url).all()
		dupe_counter += len(repdupe)
		print(f'[d] gitrepo url:{d.git_url} has {len(repdupe)} dupes found in:')
		for r in repdupe:
			grepo = None   # session.query(GitRepo).filter(GitRepo.git_path == r.git_path).first()
			# g_show = get_git_show(grepo)
			# lastcommitdate = g_show["last_commit"]
			# timediff = grepo.config_ctime - lastcommitdate
			# timediff2 = datetime.now() - lastcommitdate
			# print(f'\tid:{grepo.id} path={r.git_path} age {timediff.days} days td2={timediff2.days}')
	print(f'[getdupes] {dupe_counter} dupes found')


def get_remote(gitfolder: str) -> str:
	"""
	Get the remote url of a git folder
	Parameters: gitfolder: str - path to git folder
	Returns: str - remote url
	"""
	os.chdir(gitfolder)
	cmdstr = ['git', 'remote', '-v']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	remote_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	remote_url = '[no remote]'
	try:
		remote_url = remote_out[0].split()[1]
	except IndexError as e:
		logger.warning(f'[gr] {e} {type(e)} {gitfolder=} remote_out: {remote_out}')
	return remote_url

if __name__ == '__main__':
	pass
