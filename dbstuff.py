import os
from pathlib import Path
from subprocess import Popen, PIPE
from configparser import ConfigParser
from datetime import datetime, timedelta
from typing import List

from loguru import logger
from sqlalchemy import Engine
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from utils import (get_directory_size, get_subdircount, get_subfilecount)

# Base = declarative_base()

class MissingConfigException(Exception):
	pass


class MissingGitFolderException(Exception):
	pass


class Base(DeclarativeBase):
	pass


class GitParentPath(Base):
	__tablename__ = 'gitparentpath'
	id: Mapped[int] = mapped_column(primary_key=True)
	folder = Column('folder', String(255))
	gitfolders: Mapped[List['GitFolder']] = relationship()
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('scan_time', Float)

	def __init__(self, folder):
		self.folder = folder
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.scan_time = 0.0

	def __repr__(self):
		return f'GSP id={self.id} {self.folder}'


class GitFolder(Base):
	__tablename__ = 'gitfolder'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	git_path = Column('git_path', String(255))
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	# gitparent = relationship("GitParentPath", backref="git_path")
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('scan_time', Float)
	scan_count = Column('scan_count', Integer)

	folder_size = Column('folder_size', BigInteger)
	file_count = Column('file_count', BigInteger)
	subdir_count = Column('subdir_count', BigInteger)

	gitfolder_ctime = Column('gitfolder_ctime', DateTime)
	gitfolder_atime = Column('gitfolder_atime', DateTime)
	gitfolder_mtime = Column('gitfolder_mtime', DateTime)
	dupe_flag = Column('dupe_flag', Boolean)

	def __init__(self, gitfolder: str, gsp: GitParentPath):
		self.parent_path = str(gsp.folder)
		self.parent_id = gsp.id
		self.git_path = str(gitfolder)
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.get_stats()
		self.scan_time = 0.0
		self.scan_count = 0
		self.dupe_flag = False

	def __repr__(self):
		return f'GitFolder {self.git_path} size={self.folder_size} fc={self.file_count} sc={self.subdir_count}'

	def refresh(self):
		self.scan_count += 1
		self.last_scan = datetime.now()
		self.get_stats()
		return f'[refresh] {self}'

	# logger.debug(f'[r] {self}')

	def rescan(self):
		# self.last_scan = datetime.now()
		try:
			self.get_stats()
		except FileNotFoundError as e:
			errmsg = f'{self} {e}'
			logger.warning(errmsg)
			return errmsg

	# raise FileNotFoundError(errmsg)

	# return GitRepo(self)

	def get_stats(self):
		if os.path.exists(self.git_path):
			stat = os.stat(self.git_path)
			self.gitfolder_ctime = datetime.fromtimestamp(stat.st_ctime)
			self.gitfolder_atime = datetime.fromtimestamp(stat.st_atime)
			self.gitfolder_mtime = datetime.fromtimestamp(stat.st_mtime)

			self.folder_size = get_directory_size(self.git_path)
			self.file_count = get_subfilecount(self.git_path)
			self.subdir_count = get_subdircount(self.git_path)
		else:
			raise FileNotFoundError(f'{self} does not exist')


class GitRepo(Base):
	__tablename__ = 'gitrepo'
	id: Mapped[int] = mapped_column(primary_key=True)
	# gitfolder_id = Column('gitfolder_id', Integer)
	# parent_id = Column('parent_id', Integer)
	gitfolder_id: Mapped[int] = mapped_column(ForeignKey('gitfolder.id'))
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	# parentid = Column('parentid', BigInteger)
	giturl = Column('giturl', String(255))
	git_path = Column('git_path', String(255))
	remote = Column('remote', String(255))
	branch = Column('branch', String(255))
	dupe_flag = Column('dupe_flag', Boolean)
	dupe_count = Column('dupe_count', BigInteger)
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('scan_time', Float)
	scan_count = Column('scan_count', Integer)

	git_config_file = Column('git_config_file', String(255))
	config_ctime = Column('config_ctime', DateTime)
	config_atime = Column('config_atime', DateTime)
	config_mtime = Column('config_mtime', DateTime)

	commitmsg_file = Column('commitmsg_file', String(255))
	commitmsg_mtime = Column('commitmsg_mtime', DateTime)
	commitmsg_ctime = Column('commitmsg_ctime', DateTime)
	commitmsg_atime = Column('commitmsg_atime', DateTime)

	# git_path: Mapped[List["GitFolder"]] = relationship()
	# gitfolder = relationship("GitFolder", backref="git_path")

	def __init__(self, gitfolder: GitFolder):
		self.gitfolder_id = gitfolder.id
		self.parent_id = gitfolder.parent_id
		self.git_config_file = str(gitfolder.git_path) + '/.git/config'
		self.commitmsg_file = f'{self.git_path}/.git/COMMIT_EDITMSG'
		self.git_path = gitfolder.git_path
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.scan_time = 0.0
		self.scan_count = 0
		self.dupe_flag = False
		self.get_stats()
		self.read_git_config()

	def __repr__(self):
		return f'GitRepo id={self.id} gitfolder_id={self.gitfolder_id} url: {self.giturl} remote: {self.remote} branch: {self.branch}'

	def refresh(self):
		self.get_stats()
		self.read_git_config()
		self.scan_count += 1

	# logger.info(f'[refresh] {self}')

	def get_stats(self):
		if os.path.exists(self.git_config_file):
			st = os.stat(self.git_config_file)
			self.config_ctime = datetime.fromtimestamp(st.st_ctime)
			self.config_atime = datetime.fromtimestamp(st.st_atime)
			self.config_mtime = datetime.fromtimestamp(st.st_mtime)

		if os.path.exists(self.commitmsg_file):
			st = os.stat(self.commitmsg_file)
			self.commitmsg_ctime = datetime.fromtimestamp(st.st_ctime)
			self.commitmsg_atime = datetime.fromtimestamp(st.st_atime)
			self.commitmsg_mtime = datetime.fromtimestamp(st.st_mtime)

	def read_git_config(self):
		if not os.path.exists(self.git_config_file):
			return
		else:
			#c = self.
			conf = ConfigParser(strict=False)
			c = conf.read(self.git_config_file)
			remote_section = None
			if conf.has_section('remote "origin"'):
				try:
					remote_section = [k for k in conf.sections() if 'remote' in k][0]
				except IndexError as e:
					logger.error(f'[err] {self} {e} git_config_file={self.git_config_file} conf={conf.sections()}')
				if remote_section:
					self.remote = remote_section.split(' ')[1].replace('"', '')
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
					self.branch = branch_section.split(' ')[1].replace('"', '')
					try:
						# giturl = [k for k in conf['remote "origin"'].items()][0][1]
						self.giturl = conf[remote_section]['url']
					except TypeError as e:
						logger.warning(f'[!] {self} typeerror {e} git_config_file={self.git_config_file} ')
					except KeyError as e:
						logger.warning(f'[!] {self} KeyError {e} git_config_file={self.git_config_file}')


class DupeViewX(Base):
	__tablename__ = 'dupeviewx'
	id: Mapped[int] = mapped_column(primary_key=True)
	gitfolder_id = Column('gitfolder_id', Integer)
	giturl = Column('giturl', String(255))
	count = Column('count', Integer)


def db_init(engine: Engine) -> None:
	Base.metadata.create_all(bind=engine)


def drop_database(engine: Engine) -> None:
	logger.warning(f'[drop] all engine={engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)


def get_folder_entries(session: sessionmaker, parent_item: GitParentPath) -> dict:
	parentid = parent_item.id
	gfe = [k for k in session.query(GitFolder).filter(GitFolder.parent_id == parentid).all()]
	return {'parentid': parent_item, 'gfe': gfe}


def get_repo_entries(session: sessionmaker) -> list:
	return session.query(GitRepo).all()


def get_engine(dbtype: str) -> Engine:
	if dbtype == 'mysql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = os.getenv('gitdbNAME')
		if not dbuser or not dbpass or not dbhost or not dbname:
			raise AttributeError(f'[db] missing db env variables')
		dburl = f"mysql+pymysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8mb4"
		return create_engine(dburl)
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	elif dbtype == 'postgresql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = os.getenv('gitdbNAME')
		if not dbuser or not dbpass or not dbhost or not dbname:
			raise AttributeError(f'[db] missing db env variables')
		dburl = f"postgresql://{dbuser}:{dbpass}@{dbhost}/{dbname}"
		return create_engine(dburl)
	elif dbtype == 'sqlite':
		return create_engine('sqlite:///gitrepo1.db', echo=False, connect_args={'check_same_thread': False})
	else:
		raise TypeError(f'[db] unknown dbtype {dbtype} ')


def xdupe_view_init(session) -> None:
	pass


def dupe_view_init(session: sessionmaker) -> None:
	drop_sql = text('DROP view if exists dupeview ;')
	session.execute(drop_sql)
	drop_sql = text('DROP view if exists nodupes;')
	session.execute(drop_sql)

	create_sql = text('CREATE VIEW dupeview as select id,gitfolder_id,giturl,git_path, count(*) as count from gitrepo group by giturl having count>1;')
	session.execute(create_sql)

	create_sql = text('CREATE VIEW nodupes as select id,gitfolder_id,giturl,count(*) as count from gitrepo group by giturl having count=1;')
	session.execute(create_sql)
	logger.info(f'[dupe] dupeview and nodupes created')

	sql = text('select * from nodupes;')
	nodupes = [k for k in session.execute(sql).fetchall()]
	logger.debug(f'[d] nodupes={len(nodupes)}')

	# clear dupe flags on repos and folder with only one entry
	for d in nodupes:
		g = session.query(GitRepo).filter(GitRepo.id == d.id).first()
		g.dupe_flag = False
		g.dupe_count = 0

		f = session.query(GitFolder).filter(GitFolder.id == d.gitfolder_id).first()
		f.dupe_flag = False
	# set dupe_flag on all folders with this giturl
	session.commit()

	sql = text('select * from dupeview;')
	dupes = [k for k in session.execute(sql).fetchall()]
	logger.debug(f'[d] dupes={len(dupes)}')

	# set dupe flags on repos and folder with more than one entry
	for d in dupes:
		g = session.query(GitRepo).filter(GitRepo.id == d.id).first()
		g.dupe_count = d.count
		g.dupe_flag = True
		# set dupe_flag on all repos with this giturl
		# todo fix this
		gf = session.query(GitFolder).filter(GitFolder.id == d.gitfolder_id).all()

		for f in gf:
			f.dupe_flag = True
	# set dupe_flag on all folders with this giturl
	session.commit()


def get_dupes(session: sessionmaker) -> list:
	sql = text('select * from dupeview;')
	dupes = session.execute(sql).all()

	sql = text('select * from nodupes;')
	nodupes = session.execute(sql).all()

	logger.info(f'[dupes] found {len(dupes)} dupes nodupes={len(nodupes)}')
	return dupes


