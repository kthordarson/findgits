from __future__ import annotations

import os
from pathlib import Path
import subprocess
from subprocess import Popen, PIPE

from configparser import ConfigParser, DuplicateSectionError
from datetime import datetime, timedelta
from typing import List

from loguru import logger
from sqlalchemy import Engine
from sqlalchemy import (BIGINT, DATE, DATETIME, TIME, Integer, BigInteger, Boolean,
                        Column, DateTime, Float, ForeignKey,
                        ForeignKeyConstraint, MetaData, Numeric, String, Table,
                        create_engine, inspect, select, text)
from sqlalchemy.exc import (ArgumentError, CompileError, DataError,
                            IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
#from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker)
from sqlalchemy.orm.exc import UnmappedInstanceError

from utils import (get_directory_size, get_subdircount, get_subfilecount)

#Base = declarative_base()

class MissingConfigException(Exception):
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
	scan_time = Column('last_scan', BigInteger)

	def __init__(self, folder):
		self.folder = folder
		self.first_scan = datetime.now()

	def __repr__(self):
		return f'GSP id={self.id} {self.folder}'

class GitFolder(Base):
	__tablename__ = 'gitfolder'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	git_path = Column('git_path', String(255))
	#parent_id = Column('parent_id', Integer)#
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	#gitparent = relationship("GitParentPath", backref="git_path")
	#parent_id = Column('parent_id', BigInteger)
	#parent_path = Column('patent_path', String(255))
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('last_scan', BigInteger)

	folder_size = Column('folder_size', BigInteger)
	file_count = Column('file_count', BigInteger)
	subdir_count = Column('subdir_count', BigInteger)

	gitfolder_ctime = Column('gitfolder_ctime', DateTime)
	commit_ctime = Column('commit_ctime', DateTime)
	config_ctime = Column('config_ctime', DateTime)

	gitfolder_atime = Column('gitfolder_atime', DateTime)
	commit_atime = Column('commit_atime', DateTime)
	config_atime = Column('config_atime', DateTime)

	gitfolder_mtime = Column('gitfolder_mtime', DateTime)
	commit_mtime = Column('commit_mtime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	commitmsg_file = Column('commitmsg_file', String(255))
	git_config_file = Column('git_config_file', String(255))
	dupe_flag = Column('dupe_flag', Boolean)

	def __init__(self, gitfolder:str, gsp:GitParentPath):
		self.parent_path = str(gsp.folder)
		self.parent_id = gsp.id
		self.git_path = str(gitfolder)
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.commitmsg_file = f'{self.git_path}/.git/COMMIT_EDITMSG'
		self.git_config_file = f'{self.git_path}/.git/config'
		self.get_stats()

	def __repr__(self):
		return f'GitFolder {self.git_path} size={self.folder_size} fc={self.file_count} sc={self.subdir_count}'

	def get_repo(self):
		pass
		#return GitRepo(self)

	def refresh(self):
		self.last_scan = datetime.now()
		self.get_stats()
		# logger.debug(f'[r] {self}')

	def rescan(self):
		#self.last_scan = datetime.now()
		pass
		#return GitRepo(self)

	def get_stats(self):
		stat = os.stat(self.git_path)
		self.gitfolder_ctime = datetime.fromtimestamp(stat.st_ctime)
		self.gitfolder_atime = datetime.fromtimestamp(stat.st_atime)
		self.gitfolder_mtime = datetime.fromtimestamp(stat.st_mtime)

		self.folder_size = get_directory_size(self.git_path)
		self.file_count = get_subfilecount(self.git_path)
		self.subdir_count = get_subdircount(self.git_path)
		if os.path.exists(self.commitmsg_file):
			stat = os.stat(self.commitmsg_file)
			self.commit_ctime = datetime.fromtimestamp(stat.st_ctime)
			self.commit_atime = datetime.fromtimestamp(stat.st_atime)
			self.commit_mtime = datetime.fromtimestamp(stat.st_mtime)

class GitRepo(Base):
	__tablename__ = 'gitrepo'
	id: Mapped[int] = mapped_column(primary_key=True)
	#gitfolder_id = Column('gitfolder_id', Integer)
	# parent_id = Column('parent_id', Integer)
	gitfolder_id: Mapped[int] = mapped_column(ForeignKey('gitfolder.id'))
	parent_id: Mapped[int] = mapped_column(ForeignKey('gitparentpath.id'))
	#parentid = Column('parentid', BigInteger)
	giturl = Column('giturl', String(255))
	git_path = Column('git_path', String(255))
	remote = Column('remote', String(255))
	branch = Column('branch', String(255))
	dupe_flag = Column('dupe_flag', Boolean)
	dupe_count = Column('dupe_count', BigInteger)
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)
	scan_time = Column('last_scan', BigInteger)
	#git_path: Mapped[List["GitFolder"]] = relationship()
	#gitfolder = relationship("GitFolder", backref="git_path")

	def __init__(self,  gitfolder:GitFolder):
		self.gitfolder_id = gitfolder.id
		self.parent_id = gitfolder.parent_id
		self.git_config_file = str(gitfolder.git_path) + '/.git/config'
		self.git_path = gitfolder.git_path
		self.conf = ConfigParser(strict=False)
		self.first_scan = datetime.now()
		try:
			self.read_git_config()
		except MissingConfigException as e:
			logger.warning(f'[!] {e}')
			#raise e

	def __repr__(self):
		return f'GitRepo id={self.id} gitfolder_id={self.gitfolder_id} url: {self.giturl} remote: {self.remote} branch: {self.branch}'

	def refresh(self):
		pass
		#logger.info(f'[refresh] {self}')

	def read_git_config(self):
		if not os.path.exists(self.git_config_file):
			raise MissingConfigException(f'git_config_file {self.git_config_file} does not exist')
		if self.conf:
			c = self.conf.read(self.git_config_file)
			st = os.stat(self.git_config_file)
			self.config_ctime = datetime.fromtimestamp(st.st_ctime)
			self.config_atime = datetime.fromtimestamp(st.st_atime)
			self.config_mtime = datetime.fromtimestamp(st.st_mtime)
			if self.conf.has_section('remote "origin"'):
				try:
					remote_section = [k for k in self.conf.sections() if 'remote' in k][0]
				except IndexError as e:
					logger.error(f'[err] {self} {e} git_config_file={self.git_config_file} conf={self.conf.sections()}')
				self.remote  = remote_section.split(' ')[1].replace('"','')
				branch_section = [k for k in self.conf.sections() if 'branch' in k][0]
				self.branch = branch_section.split(' ')[1].replace('"','')
				try:
					# giturl = [k for k in conf['remote "origin"'].items()][0][1]
					self.giturl = self.conf[remote_section]['url']
				except TypeError as e:
					logger.warning(f'[gconfig] {self} typeerror {e} git_config_file={self.git_config_file} ')
				except KeyError as e:
					logger.warning(f'[gconfig] {self} KeyError {e} git_config_file={self.git_config_file}')
			if not self.giturl:
				pass
				#raise MissingConfigException(f'[!] {self} giturl is empty self.git_config_file={self.git_config_file}')


def db_init(engine):
	Base.metadata.create_all(bind=engine)

def get_folder_entries(session:sessionmaker, parent_item:GitParentPath):
	parentid = parent_item.id
	gfe = [k for k in session.query(GitFolder).filter(GitFolder.parent_id == parentid).all()]
	return {'parentid':parent_item, 'gfe':gfe}

def get_repo_entries(session:sessionmaker):
	return session.query(GitRepo).all()

def get_engine(dbtype:str) -> Engine:
	dbuser = os.getenv('gitdbUSER')
	dbpass = os.getenv('gitdbPASS')
	dbhost = os.getenv('gitdbHOST')
	dbname = os.getenv('gitdbNAME')
	if dbtype == 'mysql':
		dburl = (f"mysql+pymysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8mb4")
		return create_engine(dburl)
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	if dbtype == 'postgresql':
		dburl = (f"postgresql://{dbuser}:{dbpass}@{dbhost}/{dbname}?autocommit=True")
		return create_engine(dburl)
	if dbtype == 'sqlite':
		return create_engine('sqlite:///gitrepo1.db', echo=False, connect_args={'check_same_thread': False})
	else:
		return None

def dupe_view_init(session):
	drop_sql = text('DROP VIEW if exists dupeview ;')
	session.execute(drop_sql)
	drop_sql = text('DROP VIEW if exists nodupes;')
	session.execute(drop_sql)

	create_sql = text('CREATE VIEW dupeview as select id,gitfolder_id,giturl,count(*) as count from gitrepo group by giturl having count>1;')
	session.execute(create_sql)

	create_sql = text('CREATE VIEW nodupes as select id,gitfolder_id,giturl,count(*) as count from gitrepo group by giturl having count=1;')
	session.execute(create_sql)
	logger.info(f'[dupe] dupeview and nodupes created')

	sql = text('select * from nodupes;')
	nodupes = [k for k in session.execute(sql).fetchall()]
	logger.debug(f'[d] nodupes={len(nodupes)}')

	# clear dupe flags on repos and folder with only one entry
	for d in nodupes:
		g = session.query(GitRepo).filter(GitRepo.id==d.id).first()
		g.dupe_flag = False
		g.dupe_count = 0

		f = session.query(GitFolder).filter(GitFolder.id==d.gitfolder_id).first()
		f.dupe_flag = False
		# set dupe_flag on all folders with this giturl
	session.commit()

	sql = text('select * from dupeview;')
	dupes = [k for k in session.execute(sql).fetchall()]
	logger.debug(f'[d] dupes={len(dupes)}')

	# set dupe flags on repos and folder with more than one entry
	for d in dupes:
		g = session.query(GitRepo).filter(GitRepo.id==d.id).first()
		g.dupe_count = d.count
		g.dupe_flag = True
		# set dupe_flag on all repos with this giturl
		# todo fix this
		gf = session.query(GitFolder).filter(GitFolder.id==d.gitfolder_id).all()

		for f in gf:
			f.dupe_flag = True
		# set dupe_flag on all folders with this giturl
	session.commit()

def get_dupes(session):
	sql = text('select * from dupeview;')
	dupes = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from nodupes;')
	nodupes = [k._asdict() for k in session.execute(sql).fetchall()]

	logger.info(f'[dupes] found {len(dupes)} dupes nodupes={len(nodupes)}')
	return dupes


def collect_git_folders(gitfolders, session):
	# create GitFolder objects from gitfolders
	for k in gitfolders:
#		if session.query(GitFolder).filter(GitFolder.git_path == str(k.git_path)).first():
		g = session.query(GitFolder).filter(GitFolder.git_path == str(k.git_path)).first()
		try:
			if g:
				# existing gitfolder found, refresh
				g.refresh()
				session.add(g)
				session.commit()
			else:
				# new gitfolder found, add to db
				session.add(k)
				session.commit()
				# logger.debug(f'[!] New: {k} ')
		except OperationalError as e:
			logger.error(f'[E] {e} g={g}')
			continue
		finally:
			session.close()
	logger.debug(f'[collect_git_folders] gitfolders={len(gitfolders)}')

def collect_git_folder(gitfolder, session):
	# create GitFolder objects from gitfolders
	g = session.query(GitFolder).filter(GitFolder.git_path == str(gitfolder.git_path)).first()
	try:
		if g:
			# existing gitfolder found, refresh
			g.refresh()
			session.add(g)
			session.commit()
		else:
			# new gitfolder found, add to db
			session.add(g)
			session.commit()
			# logger.debug(f'[!] New: {k} ')
	except OperationalError as e:
		logger.error(f'[E] {e} g={g}')
	finally:
		session.close()

def collect_repo(gf:GitFolder, session):
	#engine = get_engine(dbtype=dbmode)
	#Session = sessionmaker(bind=engine)
	# session = session

	try:
		# construct repo object from gf (folder)
		gr = GitRepo(gf)
	except MissingConfigException as e:
		#logger.error(f'[cgr] {e} gf={gf}')
		return None
	except TypeError as e:
		logger.error(f'[cgr] TypeError {e} gf={gf}')
		return None

	repo_q = session.query(GitRepo).filter(GitRepo.giturl == str(gr.giturl)).first()
	folder_q = session.query(GitRepo).filter(GitRepo.git_path == str(gr.git_path)).first()
	if repo_q and folder_q:
		# todo: check if repo exists in other folder somewhere...
		pass
		#repo_q.refresh()
		#session.add(repo_q)
		#session.commit()
	else:
		# new repo found, add to db
		try:
			session.add(gr)
		except IntegrityError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except IllegalStateChangeError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		# logger.debug(f'[!] newgitrepo {gr} ')
		try:
			session.commit()
		except IntegrityError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except IllegalStateChangeError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except Exception as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			#session.rollback()
			raise Exception(errmsg)
	return 'done'

def get_parent_entries(session:sessionmaker) -> list:
	gpf = session.query(GitParentPath).all()
	return [k  for k in gpf if os.path.exists(k.folder)]

def add_path(newpath:str, session:sessionmaker):
	# add new path to config  db
	# returns gsp object
	if newpath.endswith('/'):
		newpath = newpath[:-1]
	if not os.path.exists(newpath):
		logger.error(f'[addpath] {newpath} not found')
		return None

	# check db entries for invalid paths and remove
	# gsp_entries = get_parent_entries(session)
	path_check = None
	path_check = session.query(GitParentPath).filter(GitParentPath.folder == newpath).all()

	if len(path_check) == 0:
		gsp = GitParentPath(newpath)
		session.add(gsp)
		session.commit()
		logger.debug(f'[add_path] adding {gsp} to db')
	else:
		logger.warning(f'[add_path] path={newpath} already in config')
	return gsp

def scanpath_thread(gf:GitFolder, argsdbmode:str):
	engine = get_engine(dbtype=argsdbmode)
	Session = sessionmaker(bind=engine)
	session = Session()

	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	#gsp = session.query(GitParentPath).filter(GitParentPath.id == str(scanpath)).first()
	# gitfolders = [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]
	#_ = [session.add(k) for k in gitfolders]
	# try:
	# 	session.commit()
	# except DataError as e:
	# 	logger.error(f'[spt] dataerror {e} scanpath={scanpath} gsp={gsp}')
	# 	session.rollback()
	# 	raise TypeError(f'[spt] {e} {type(e)} scanpath={scanpath} gsp={gsp}')
	# except IntegrityError as e:
	# 	logger.error(f'[spt] IntegrityError {e} scanpath={scanpath} gsp={gsp}')
	# 	session.rollback()
	# 	raise TypeError(f'[spt] {e} {type(e)} scanpath={scanpath} gsp={gsp}')
	#collect_git_folders(gitfolders, session)
	#folder_entries = get_folder_entries(session, gsp.id)
	# logger.info(f'[spt] starting')
	#for gf in folder_entries:
	try:
		cr = collect_repo(gf, session)
	except TypeError as e:
		logger.error(f'[!] TypeError {e} gf={gf}')
		return None
	except AttributeError as e:
		logger.error(f'[!] AttributeError {e} gf={gf}')
		return None
	except IntegrityError as e:
		logger.error(f'[!] IntegrityError {e} {type(e)} gf={gf}')
	except InvalidRequestError as e:
		errmsg = f'[!] InvalidRequestError {e} {type(e)} gf={gf}'
		logger.error(errmsg)
		raise InvalidRequestError(errmsg)
	#repo_entries = get_repo_entries(session)
	#return f'[spt] {gf} cr={cr}'
	#logger.debug(f'[scanpath] repo_entries={len(repo_entries)}')

def scanpath(gsp:GitParentPath, argsdbmode:str):
	engine = get_engine(dbtype=argsdbmode)
	Session = sessionmaker(bind=engine)
	session = Session()

	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	scanpath = gsp.folder
	gitfolders = []
	gfl = get_folder_list(gsp)
	logger.info(f'[scanpath] scanpath={scanpath} gsp={gsp} found {len(gfl["res"])} gitfolders')
	for g in gfl['res']:
		git_folder = GitFolder(g, gsp)
		session.add(git_folder)
		session.commit()
		git_repo = GitRepo(git_folder)
		session.add(git_repo)
		session.commit()
	logger.info(f'[scanpath] scanpath={scanpath} gsp={gsp} found {len(gfl["res"])} gitfolders done')



def show_dbinfo(session):
	# mysql 'select (select count(*) from gitfolder) as fc, (select count(*) from gitrepo) as rc, (select count(*) from gitparentpath) as fpc, (select count(*) from dupeview) as dc';
	# sqlite3 gitrepo1.db 'select (select count(*) from gitparentpath) as gpc, (select count(*) from gitfolder) as gfc, (select count(*) from gitrepo) as grc ' -table
	parent_folders = session.query(GitParentPath).all()
	for gpf in parent_folders:
		git_folders = session.query(GitFolder).filter(GitFolder.parent_id == gpf.id).count()
		git_repos = session.query(GitRepo).filter(GitRepo.parent_id == gpf.id).count()
		logger.info(f'[dbinfo] gpf={gpf} git_folders={git_folders} git_repos={git_repos}')
		# git_repos = session.query(GitRepo).all()
		# dupe_v = session.query()
		# sql = text('select * from dupeview;')
		# dupes = [k._asdict() for k in session.execute(sql).fetchall()]

		# sql = text('select * from nodupes;')
		# nodupes = [k._asdict() for k in session.execute(sql).fetchall()]

		# sql = text('select * from gitrepo where dupe_flag = 1;')
		# dupetest = [k._asdict() for k in session.execute(sql).fetchall()]

		# sql = text('select * from gitrepo where dupe_flag is NULL;')
		# nodupetest = [k._asdict() for k in session.execute(sql).fetchall()]

		# logger.info(f'[dbinfo] parent_folders={len(parent_folders)} git_folders={len(git_folders)} git_repos={len(git_repos)} dupes={len(dupes)} / {len(dupetest)} nodupes={len(nodupes)} / NULL {len(nodupetest)}')

def xshow_dbinfo(session):
	# mysql 'select (select count(*) from gitfolder) as fc, (select count(*) from gitrepo) as rc, (select count(*) from gitparentpath) as fpc, (select count(*) from dupeview) as dc';
	# sqlite3 gitrepo1.db 'select (select count(*) from gitparentpath) as gpc, (select count(*) from gitfolder) as gfc, (select count(*) from gitrepo) as grc ' -table
	parent_folders = session.query(GitParentPath).all()
	git_folders = session.query(GitFolder).all()
	git_repos = session.query(GitRepo).all()
	dupe_v = session.query()
	sql = text('select * from dupeview;')
	dupes = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from nodupes;')
	nodupes = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from gitrepo where dupe_flag = 1;')
	dupetest = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from gitrepo where dupe_flag is NULL;')
	nodupetest = [k._asdict() for k in session.execute(sql).fetchall()]

	logger.info(f'[dbinfo] parent_folders={len(parent_folders)} git_folders={len(git_folders)} git_repos={len(git_repos)} dupes={len(dupes)} / {len(dupetest)} nodupes={len(nodupes)} / NULL {len(nodupetest)}')

def get_folder_list(gitparent:GitParentPath):
	startpath = gitparent.folder
	t0 = datetime.now()
	cmdstr = ['find', startpath+'/', '-type','d', '-name', '.git']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	g_out = out.decode('utf8').split('\n')
	if err != b'':
		logger.warning(f'[get_folder_list] {cmdstr} {err}')
	res = [Path(k).parent for k in g_out if os.path.exists(k + '/config')]
	# logger.debug(f'[get_folder_list] {datetime.now() - t0} gitparent={gitparent} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
	return {'gitparent':gitparent, 'res':res}

def drop_database(engine:Engine):
	logger.warning(f'[drop] all engine={engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)
