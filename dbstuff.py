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
	folder_count = Column(Integer)

	def __init__(self, folder):
		self.folder = folder
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.gfl = []
		self.folder_count = 0

	def __repr__(self):
		return f'<GSP id={self.id} {self.folder} st:{self.scan_time}>'

	def get_git_folders(self) -> dict:
		t0 = datetime.now()
		cmdstr = ['find', self.folder + '/', '-type', 'd', '-name', '.git']
		out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
		g_out = out.decode('utf8').split('\n')
		res = [Path(k).parent for k in g_out if os.path.exists(k + '/config')]
		self.scan_time = (datetime.now() - t0).total_seconds()
		# logger.debug(f'[get_folder_list] {datetime.now() - t0} gitparent={gitparent} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
		self.gfl = res
		return {'gitparent': self.id, 'res': res, 'scan_time': self.scan_time}



class GitFolder(Base):
	__tablename__ = 'gitfolder'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	git_path = Column('git_path', String(255))
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	# gitparent = relationship("GitParentPath", backref="git_path")
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
	valid = Column(Boolean, default=True)

	def __init__(self, gitfolder: str, gsp: GitParentPath):
		self.parent_path = str(gsp.folder)
		self.parent_id = gsp.id
		self.git_path = str(gitfolder)
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.scan_count = 0
		self.get_stats()
		self.dupe_flag = False

	def __repr__(self):
		return f'<GitFolder {self.git_path} size={self.folder_size} fc={self.file_count} sc={self.subdir_count} t:{self.scan_time}>'

	def get_stats(self):
		t0 = datetime.now()
		if os.path.exists(self.git_path):
			self.scan_count += 1
			self.last_scan = datetime.now()
			stat = os.stat(self.git_path)
			self.gitfolder_ctime = datetime.fromtimestamp(stat.st_ctime)
			self.gitfolder_atime = datetime.fromtimestamp(stat.st_atime)
			self.gitfolder_mtime = datetime.fromtimestamp(stat.st_mtime)

			self.folder_size = get_directory_size(self.git_path)
			self.file_count = get_subfilecount(self.git_path)
			self.subdir_count = get_subdircount(self.git_path)
			self.scan_time = (datetime.now() - t0).total_seconds()
		else:
			self.valid = False
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
	scan_count = Column('scan_count', Integer)

	git_config_file = Column('git_config_file', String(255))
	config_ctime = Column('config_ctime', DateTime)
	config_atime = Column('config_atime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	valid = Column(Boolean, default=True)

	# git_path: Mapped[List["GitFolder"]] = relationship()
	# gitfolder = relationship("GitFolder", backref="git_path")

	def __init__(self, gitfolder: GitFolder):
		self.gitfolder_id = gitfolder.id
		self.parent_id = gitfolder.parent_id
		self.git_config_file = str(gitfolder.git_path) + '/.git/config'
		self.git_path = gitfolder.git_path
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.scan_count = 0
		self.dupe_flag = False
		self.get_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} {self.giturl} fid={self.gitfolder_id} r:{self.remote}/{self.branch}>'

	def get_stats(self):
		if not os.path.exists(self.git_config_file):
			self.valid = False
			return
		else:
			#c = self.
			self.scan_count += 1
			st = os.stat(self.git_config_file)
			self.config_ctime = datetime.fromtimestamp(st.st_ctime)
			self.config_atime = datetime.fromtimestamp(st.st_atime)
			self.config_mtime = datetime.fromtimestamp(st.st_mtime)
			conf = ConfigParser(strict=False)
			c = conf.read(self.git_config_file)
			remote_section = None
			if conf.has_section('remote "origin"'):
				try:
					remote_section = [k for k in conf.sections() if 'remote' in k][0]
				except IndexError as e:
					logger.error(f'[err] {self} {e} git_config_file={self.git_config_file} conf={conf.sections()}')
					self.valid = False
				if remote_section:
					self.remote = remote_section.split(' ')[1].replace('"', '')
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
					self.branch = branch_section.split(' ')[1].replace('"', '')
					try:
						# giturl = [k for k in conf['remote "origin"'].items()][0][1]
						self.giturl = conf[remote_section]['url']
					except TypeError as e:
						logger.warning(f'[!] {self} typeerror {e} git_config_file={self.git_config_file} ')
						self.valid = False
					except KeyError as e:
						logger.warning(f'[!] {self} KeyError {e} git_config_file={self.git_config_file}')
						self.valid = False


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


def get_engine(dbtype: str) -> Engine:
	if dbtype == 'mysql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = 'gitdbdev'#os.getenv('gitdbNAME')
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
	sql = text('select id, giturl, count(*) as count from gitrepo group by giturl having count(*)>1;')
	#sql = text('select * from dupeview;')
	dupes = session.execute(sql).all()
	return dupes


