import os
from pathlib import Path
import glob
from subprocess import Popen, PIPE
from configparser import ConfigParser
from datetime import datetime, timedelta
from typing import List

from loguru import logger
from sqlalchemy import Engine, func
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from utils import (get_directory_size, get_subdircount, get_subfilecount, format_bytes)

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
class GitParentPath(Base):
	""" GitParentPath class, this is a folder that contains more than one git repos"""
	__tablename__ = 'gitparentpath'
	id: Mapped[int] = mapped_column(primary_key=True)
	folder = Column('folder', String(255))
	gitfolders: Mapped[List['GitFolder']] = relationship()
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('scan_time', Float)
	folder_count = Column(Integer)
	folder_size = Column(BigInteger)
	file_count = Column(BigInteger)
	repo_count = Column(Integer)

	def __init__(self, folder):
		self.folder = folder
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.gfl = []
		self.folder_count = 0
		self.folder_size = 0
		self.file_count = 0
		self.repo_count = 0

	def __repr__(self):
		return f'<GPP id={self.id} {self.folder} st:{self.scan_time}>'

	def get_git_folders(self) -> dict:
		"""
		Scans this gitparentpath for all sub gitfolders
		Returns: dict with gitparentpath id, list of gitfolders and scantime
		"""
		t0 = datetime.now()
		#cmdstr = ['find', self.folder + '/', '-type', 'd', '-name', '.git']
		#out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
		#g_out = out.decode('utf8').split('\n')
		g_out = glob.glob(str(Path(self.folder))+'/**/.git',recursive=True, include_hidden=True)
		res = [Path(k).parent for k in g_out if os.path.exists(k + '/config')]
		self.scan_time = (datetime.now() - t0).total_seconds()
		# logger.debug(f'[get_folder_list] {datetime.now() - t0} gitparent={gitparent} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
		self.gfl = res
		return {'gitparent': self.id, 'res': res, 'scan_time': self.scan_time}



# todo: remove git_path and use parent_id to get parent path string
class GitFolder(Base):
	""" A folder containing one git repo """
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
	is_parent = Column(Boolean, default=False)

	def __init__(self, gitfolder: str, gpp: GitParentPath):
		self.parent_path = str(gpp.folder)
		self.parent_id = gpp.id
		self.git_path = str(gitfolder)
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.scan_count = 0
		self.get_stats()
		self.dupe_flag = False
		self.is_parent = False

	def __repr__(self):
		return f'<GitFolder {self.git_path} size={self.folder_size} fc={self.file_count} sc={self.subdir_count} t:{self.scan_time}>'

	def get_stats(self):
		""" Get stats for this gitfolder"""
		t0 = datetime.now()
		if os.path.exists(self.git_path): # redundant check, but just in case?
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


# todo: make this better, should only be linked to one gitfolder and that gitfolder links to a gitparentpath
class GitRepo(Base):
	""" A git repo, linked to one gitfolder and one gitparentpath """
	__tablename__ = 'gitrepo'
	id: Mapped[int] = mapped_column(primary_key=True)
	# gitfolder_id = Column('gitfolder_id', Integer)
	# parent_id = Column('parent_id', Integer)
	gitfolder_id: Mapped[int] = mapped_column(ForeignKey('gitfolder.id'))
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	# parentid = Column('parentid', BigInteger)
	git_url = Column('git_url', String(255))
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
		self.last_scan = self.first_scan
		self.scan_count = 0
		self.dupe_flag = False
		self.get_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} {self.git_url} fid={self.gitfolder_id} r:{self.remote}/{self.branch}>'

	def get_stats(self):
		""" Collect stats and read config for this git repo """
		if not os.path.exists(self.git_config_file): # redundant check, but just in case?
			self.valid = False
			return
		else:
			#c = self.
			self.scan_count += 1
			st = os.stat(self.git_config_file)
			self.config_ctime = datetime.fromtimestamp(st.st_ctime)
			self.config_atime = datetime.fromtimestamp(st.st_atime)
			self.config_mtime = datetime.fromtimestamp(st.st_mtime)
			conf = ConfigParser(strict=False) # todo: make this better
			c = conf.read(self.git_config_file)
			remote_section = None
			if conf.has_section('remote "origin"'):
				try:
					remote_section = [k for k in conf.sections() if 'remote' in k][0]
				except IndexError as e:
					logger.error(f'[err] {self} {e} git_config_file={self.git_config_file} conf={conf.sections()}')
					self.valid = False
					return
				if remote_section:
					self.remote = remote_section.split(' ')[1].replace('"', '')
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
					self.branch = branch_section.split(' ')[1].replace('"', '')
					try:
						# git_url = [k for k in conf['remote "origin"'].items()][0][1]
						self.git_url = conf[remote_section]['url']
					except TypeError as e:
						logger.warning(f'[!] {self} typeerror {e} git_config_file={self.git_config_file} ')
						self.valid = False
					except KeyError as e:
						logger.warning(f'[!] {self} KeyError {e} git_config_file={self.git_config_file}')
						self.valid = False

def db_init(engine: Engine) -> None:
	Base.metadata.create_all(bind=engine)

def drop_database(engine: Engine) -> None:
	logger.warning(f'[drop] all engine={engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)

def get_engine(dbtype: str) -> Engine:
	"""
	Get a db engine, uses os.getenv for db credentials
	Parameters: dbtype (str) - mysql/postgresql/sqlite
	Returns: sqlalchemy Engine
	"""
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

def get_dupes(session: sessionmaker) -> list:
	"""
	Get a list of duplicate git repos.
	A duplicate is defined as a git repo with the same git_url
	Paramets: session (sessionmaker) - sqlalchemy session
	Returns: list of tuples (id, git_url, count)
	"""
	sql = text('select id, git_url, count(*) as count from gitrepo group by git_url having count(*)>1;')
	#sql = text('select * from dupeview;')
	dupes = session.execute(sql).all()
	return dupes

def db_dupe_info(session) -> None:
	"""
	Get some info about the db
	Parameters: session (sessionmaker) - sqlalchemy session
	Returns: None
	"""
	dupes = None
	try:
		dupes = session.query(GitRepo.id, GitRepo.git_url, GitRepo.parent_id, func.count(GitRepo.git_url).label("count")).\
			group_by(GitRepo.git_url).\
			having(func.count(GitRepo.git_url)>1).\
			order_by(func.count(GitRepo.git_url).desc()).\
			limit(10).all()
	except ProgrammingError as e:
		logger.error(e)
	if dupes == []:
		print(f'[db] No dupes')
	if dupes:
		total_dupes = len(dupes)
		for d in dupes:
			dcount = session.query(GitRepo).filter(GitRepo.git_url == d.git_url).count()
	print(f"{'id' : <5}{'fid' : <5}{'repo' : <55}{'dupes' : <5}")
	for gpe in dupes:
		fc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
		print(f'{gpe.id:<5}{gpe[2]:<5}{gpe[1]:<55}{gpe[3]:<5}')
	print(f'{"="*20} {total_dupes}')

def xget_db_info(session):
	git_parent_entries = session.query(GitParentPath).all() #filter(GitParentPath.id == str(args.listpaths)).all()
	total_size = 0
	total_time = 0
	#print(f"{'gpe.id':<3}{'gpe.folder':<30}{'fc:<5'}{'rc:<5'}{'f_size':<10}{'f_scantime':<10}")
	print(f"{'id' : <3}{'folder' : <31}{'fc' : <5}{'rc' : <5}{'size' : <10}{'scantime' : <10}")
	for gpe in git_parent_entries:
		fc = session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).count()
		f_size = sum([k.folder_size for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		total_size += f_size
		f_scantime = sum([k.scan_time for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		total_time += f_scantime
		rc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
		scant = str(timedelta(seconds=f_scantime))
		print(f'{gpe.id:<3}{gpe.folder:<31}{fc:<5}{rc:<5}{format_bytes(f_size):<10}{scant:<10}')
	print(f'[*] total_size={format_bytes(total_size)} total_time={timedelta(seconds=total_time)}')

def get_db_info(session):
	git_parent_entries = session.query(GitParentPath).all() #filter(GitParentPath.id == str(args.listpaths)).all()
	slowscans = session.query(GitFolder).order_by(GitFolder.scan_time.desc()).limit(10).all()
	git_parent_scantimesum = sum([k.scan_time for k in session.query(GitParentPath).all()])
	allfolderscantimesum = sum([k.scan_time for k in session.query(GitFolder).all()])
	total_size = 0
	total_time = 0
	#print(f"{'gpe.id':<3}{'gpe.folder':<30}{'fc:<5'}{'rc:<5'}{'f_size':<10}{'f_scantime':<10}")
	print(f"{'id' : <3}{'folder' : <31}{'fc' : <5}{'rc' : <5}{'size' : <10}{'scantime' : <16}{'gp_scantime' : <15}")
	for gpe in git_parent_entries:
		fc = session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).count()
		f_size = sum([k.folder_size for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		total_size += f_size
		f_scantime = sum([k.scan_time for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		total_time += f_scantime
		rc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
		scant = str(timedelta(seconds=f_scantime))
		gpscant = str(timedelta(seconds=gpe.scan_time))
		print(f'{gpe.id:<3}{gpe.folder:<31}{fc:<5}{rc:<5}{format_bytes(f_size):<10}{scant:<16}{gpscant:<14}')
	tt = str(timedelta(seconds=total_time))
	print(f'{"="*90}')
	print(f'{format_bytes(total_size):>52} {tt:>15}')

def gitfolder_to_gitparent(gitfolder:GitFolder, session:sessionmaker) -> GitParentPath:
	"""
	Convert a gitfolder to a gitparent. Used when gitfolder contains more than one git repo
	Removes the gitfolder and any repos from that folder from db, create a new gitparentpath and return it
	Parameters: gitfolder (GitFolder) - gitfolder to convert
	Returns: gitparent (GitParentPath)
	"""
	repos_to_del = session.query(GitRepo).filter(GitRepo.id == gitfolder.id).all()
	for r in repos_to_del:
		logger.warning(f'[gtp] removing {r} from db')
		session.delete(r)
	session.delete(gitfolder)
	logger.warning(f'[gtp] removing {gitfolder} from db')
	# logger.debug(f'[gto] removed {gitfolder}')
	gpp = GitParentPath(gitfolder.git_path)
	logger.info(f'[gtp] new {gpp} created')
	return gpp