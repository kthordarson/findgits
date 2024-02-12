import glob
import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from loguru import logger
from sqlalchemy import Engine, func
from sqlalchemy import (Integer, BigInteger, Boolean, Column, DateTime, Float, ForeignKey, String, create_engine, text)
from sqlalchemy.exc import (ProgrammingError)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from sqlalchemy.orm import Session

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

	def __init__(self, folder: str):
		self.folder = folder
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.git_folder_list = []
		self.folder_count = 0
		self.folder_size = 0
		self.file_count = 0
		self.repo_count = 0
		self.base_folders = []

	def __repr__(self):
		return f'<GPP id={self.id} {self.folder}>'

	def get_git_folders(self) -> dict:
		"""
		Scans this gitparentpath for all sub gitfolders
		Returns: dict with gitparentpath id, list of gitfolders and scantime
		"""
		t0 = datetime.now()
		logger.debug(f'[gfl] scanning {self.folder}')
		git_folder_list = [str(Path(k).parent) for k in glob.glob(self.folder + '/**/.git', recursive=True, include_hidden=True) if Path(k).is_dir() and k != self.folder + '/']
		# g_out = glob.glob(self.folder+'/**/.git',recursive=True, include_hidden=True)
		# res = [Path(k).parent for k in g_out if os.path.exists(k + '/config') if Path(k).is_dir()]
		self.scan_time = (datetime.now() - t0).total_seconds()
		logger.debug(f'[gfl] done scanning {self.folder} found {len(git_folder_list)} folders in {self.scan_time} seconds')
		# logger.debug(f'[get_folder_list] {datetime.now() - t0} gitparent={gitparent} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
		# self.gfl = res
		return {'gitparent': self.id, 'res': git_folder_list, 'scan_time': self.scan_time}


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
		self.parent_path = gpp.folder
		self.parent_id = gpp.id
		self.git_path = str(gitfolder)
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_time = 0.0
		self.scan_count = 0
		self.dupe_flag = False
		self.is_parent = False
		self.folder_size = 0
		self.file_count = 0
		self.subdir_count = 0
		self.get_folder_time()

	def __repr__(self):
		return f'<GitFolder {self.id} {self.git_path} >'

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
		folder_size = get_directory_size(git_path)
		file_count = get_subfilecount(git_path)
		subdir_count = get_subdircount(git_path)
		scan_time = (datetime.now() - t0).total_seconds()
		return {'id': id, 'folder_size': folder_size, 'file_count': file_count, 'subdir_count': subdir_count, 'scan_time': scan_time}


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

	config_ctime = Column('config_ctime', DateTime)
	config_atime = Column('config_atime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	valid = Column(Boolean, default=True)

	# git_path: Mapped[List["GitFolder"]] = relationship()
	# gitfolder = relationship("GitFolder", backref="git_path")

	def __init__(self, gitfolder: GitFolder):
		self.gitfolder_id = gitfolder.id
		self.parent_id = gitfolder.parent_id
		self.git_path = gitfolder.git_path
		self.first_scan = datetime.now()
		self.last_scan = self.first_scan
		self.scan_count = 0
		self.dupe_flag = False
		self.get_repo_stats()

	def __repr__(self):
		return f'<GitRepo id={self.id} {self.git_url} fid={self.gitfolder_id}>'

	def get_repo_stats(self):
		""" Collect stats and read config for this git repo """
		# c = self.
		self.scan_count += 1
		git_config_file = self.git_path + '/.git/config'
		if not os.path.exists(git_config_file):
			logger.warning(f'{self} configfile {git_config_file} does not exist')
		else:
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
					return
				if remote_section:
					self.remote = remote_section.split(' ')[1].replace('"', '')
					branch_section = [k for k in conf.sections() if 'branch' in k][0]
					self.branch = branch_section.split(' ')[1].replace('"', '')
					try:
						# git_url = [k for k in conf['remote "origin"'].items()][0][1]
						self.git_url = conf[remote_section]['url']
					except TypeError as e:
						logger.warning(f'[!] {self} typeerror {e} git_config_file={git_config_file} ')
						self.valid = False
					except KeyError as e:
						logger.warning(f'[!] {self} KeyError {e} git_config_file={git_config_file}')
						self.valid = False


def db_init(engine: Engine) -> None:
	Base.metadata.create_all(bind=engine)


def drop_database(engine: Engine) -> None:
	logger.warning(f'[drop] all engine={engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)


def get_engine(args) -> Engine:
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
			raise AttributeError(f'[db] missing db env variables')
		dburl = f"mysql+pymysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8mb4"
		return create_engine(dburl)
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	elif args.dbmode == 'postgresql':
		dbuser = os.getenv('gitdbUSER')
		dbpass = os.getenv('gitdbPASS')
		dbhost = os.getenv('gitdbHOST')
		dbname = os.getenv('gitdbNAME')
		if not dbuser or not dbpass or not dbhost or not dbname:
			raise AttributeError(f'[db] missing db env variables')
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


def db_dupe_info(session: Session, maxdupes=30) -> None:
	"""
	Get dupe info from db
	Parameters: session (sessionmaker) - sqlalchemy session, maxdupes int - max number of dupes to show
	Returns: None
	"""
	dupes = None
	total_dupes = 0
	try:
		dupes = session.query(
			GitRepo.id.label('repoid'),
			GitRepo.git_url.label('repourl'),
			GitRepo.gitfolder_id.label('folderid'),
			GitRepo.parent_id.label('parentid'),
			func.count(GitRepo.git_url).label("count")). \
			group_by(GitRepo.git_url). \
			having(func.count(GitRepo.git_url) > 1). \
			order_by(func.count(GitRepo.git_url).desc()). \
			limit(maxdupes).all()
	except ProgrammingError as e:
		logger.error(e)
	if dupes == []:
		print(f'[db] No dupes')
	else:
		total_dupes = len(dupes)
		# for d in dupes:
		# dcount = session.query(GitRepo).filter(GitRepo.git_url == d.git_url).count()
		print(f"{'id' : <5}{'pid' : <5}{'repo' : <55}{'dupes' : <5}")
		for gpe in dupes:
			# fc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
			print(f'{gpe.repoid:<5}{gpe.parentid:<5}{gpe.repourl:<55}{gpe.count:<5}')
		print(f'{"=" * 20} {total_dupes}')


def xget_db_info(session):
	git_parent_entries = session.query(GitParentPath).all()  # filter(GitParentPath.id == str(args.listpaths)).all()
	total_size = 0
	total_time = 0
	# print(f"{'gpe.id':<3}{'gpe.folder':<30}{'fc:<5'}{'rc:<5'}{'f_size':<10}{'f_scantime':<10}")
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
	git_parent_entries = session.query(GitParentPath).all()  # filter(GitParentPath.id == str(args.listpaths)).all()
	# slowscans = session.query(GitFolder).order_by(GitFolder.scan_time.desc()).limit(10).all()
	# git_parent_scantimesum = sum([k.scan_time for k in session.query(GitParentPath).all()])
	# allfolderscantimesum = sum([k.scan_time for k in session.query(GitFolder).all()])
	total_size = 0
	total_time = 0
	# print(f"{'gpe.id':<3}{'gpe.folder':<30}{'fc:<5'}{'rc:<5'}{'f_size':<10}{'f_scantime':<10}")
	print(f"{'id' : <3}{'folder' : <47}{'fc' : <5}{'rc' : <5}{'size' : <10}{'scantime' : <15}")
	for gpe in git_parent_entries:
		fc = session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).count()
		f_size = sum([k.folder_size for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		total_size += f_size
		# f_scantime = sum([k.scan_time for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
		# total_time += f_scantime
		rc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
		# scant = str(timedelta(seconds=f_scantime))
		gpscant = str(timedelta(seconds=gpe.scan_time))
		print(f'{gpe.id:<3}{gpe.folder:<47}{fc:<5}{rc:<5}{format_bytes(f_size):<10}{gpscant:<14}')
	# tt = str(timedelta(seconds=total_time))
	print(f'{"=" * 90}')
	print(f'{format_bytes(total_size):>52} ')


def gitfolder_to_gitparent(gitfolder: GitFolder, session: sessionmaker) -> GitParentPath:
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


if __name__ == '__main__':
	pass
