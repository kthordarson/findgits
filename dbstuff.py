from __future__ import annotations
import os
from datetime import datetime, timedelta
from loguru import logger
import subprocess
from typing import List

from configparser import ConfigParser, DuplicateSectionError
from sqlalchemy import ForeignKeyConstraint, ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Float, DateTime, text, BIGINT, Numeric, DATE,TIME,DATETIME, Boolean
from sqlalchemy import create_engine
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Mapped, mapped_column, DeclarativeBase

from utils import get_directory_size, get_subdircount, get_subfilecount
#Base = declarative_base()

class MissingConfigException(Exception):
	pass

class Base(DeclarativeBase):
    pass


class GitParentPath(Base):
	__tablename__ = 'gitparentpath'
	id: Mapped[int] = mapped_column(primary_key=True)
	folder = Column('folder', String)

	def __init__(self, folder):
		self.folder = folder

	def __repr__(self):
		return f'GSP id={self.id} {self.folder}'

class GitFolder(Base):
	__tablename__ = 'gitfolder'
	# __table_args__ = (ForeignKeyConstraint(['gitrepo_id']))
	id: Mapped[int] = mapped_column(primary_key=True)
	parent_id = Column('patent_id', Integer)
	parent_path = Column('patent_path', String)
	git_path = Column('git_path', String)
	first_scan = Column('first_scan', DateTime)
	last_scan = Column('last_scan', DateTime)

	folder_size = Column('folder_size', Integer)
	file_count = Column('file_count', Integer)
	subdir_count = Column('subdir_count', Integer)

	gitfolder_ctime = Column('gitfolder_ctime', DateTime)
	commit_ctime = Column('commit_ctime', DateTime)
	config_ctime = Column('config_ctime', DateTime)

	gitfolder_atime = Column('gitfolder_atime', DateTime)
	commit_atime = Column('commit_atime', DateTime)
	config_atime = Column('config_atime', DateTime)

	gitfolder_mtime = Column('gitfolder_mtime', DateTime)
	commit_mtime = Column('commit_mtime', DateTime)
	config_mtime = Column('config_mtime', DateTime)
	commitmsg_file = Column('commitmsg_file', String)
	git_config_file = Column('git_config_file', String)
	dupe_flag = Column('dupe_flag', Boolean)

	def __init__(self, gitfolder, gsp):
		self.parent_path = gsp.folder
		self.parent_id = gsp.id
		self.git_path = f'{gitfolder}'
		self.first_scan = datetime.now()
		self.last_scan = datetime.now()
		self.commitmsg_file = f'{self.git_path}/.git/COMMIT_EDITMSG'
		self.git_config_file = f'{self.git_path}/.git/config'
		self.get_stats()

	def __repr__(self):
		return f'GitFolder id={self.id} {self.git_path}'

	def get_repo(self):
		return GitRepo(self)

	def refresh(self):
		self.last_scan = datetime.now()
		self.get_stats()
		# logger.debug(f'[r] {self}')

	def rescan(self):
		self.last_scan = datetime.now()
		return GitRepo(self)

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
	folderid = Column('folderid', Integer)
	parentid = Column('parentid', Integer)
	git_path = Column('git_path', String)
	giturl = Column('giturl', String)
	remote = Column('remote', String)
	branch = Column('branch', String)
	dupe_flag = Column('dupe_flag', Boolean)
	#git_paths: Mapped[List["GitFolder"]] = relationship()
	# gitfolder = relationship("GitFolder", back_populates="gitrepo")

	def __init__(self,  gitfolder:GitFolder):
		self.folderid = gitfolder.id
		self.parentid = gitfolder.parent_id
		self.git_path = str(gitfolder.git_path)
		self.git_config_file = str(gitfolder.git_path) + '/.git/config'
		self.conf = ConfigParser(strict=False)
		try:
			self.read_git_config()
		except MissingConfigException as e:
			logger.error(f'[!] {e}')
			raise e

	def __repr__(self):
		return f'GitRepo id={self.id} folderid={self.folderid} {self.giturl} {self.remote} {self.branch}'

	def refresh(self):
		pass
		#logger.info(f'[refresh] {self}')

	def read_git_config(self):
		if not os.path.exists(self.git_config_file):
			raise MissingConfigException(f'git_config_file {self.git_config_file} does not exist')
		else:
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
				raise MissingConfigException(f'[!] {self} giturl is empty self.git_config_file={self.git_config_file}')

	def get_git_remote_cmd(self):
		os.chdir(self.git_path)
		status = subprocess.run(['git', 'remote', '-v',], capture_output=True)
		if status.stdout != b'':
			statusstdout = status.stdout.decode('utf-8').split('\n')


def db_init(engine):
	Base.metadata.create_all(bind=engine)


def send_gitfolder_to_db(gf, session):
	try:
		session.add(gf)
	except UnmappedInstanceError as e:
		logger.error(f'[db] {e} k={gf}')
		session.rollback()
	try:
		session.commit()
	except IntegrityError as e:
		logger.error(f'[db] {e} k={gf}')
		session.rollback()
	except ProgrammingError as e:
		logger.error(f'[db] {e} k={gf}')
		session.rollback()

def send_to_db(gitremotes, session):
	#for gr in gitremotes:
	for k in gitremotes:
		try:
			session.add(k)
		except UnmappedInstanceError as e:
			logger.error(f'[db] {e} k={k}')
			session.rollback()
			continue
	try:
		session.commit()
	except IntegrityError as e:
		logger.error(f'[db] {e} k={k}')
		session.rollback()
	except ProgrammingError as e:
		logger.error(f'[db] {e} k={k}')
		session.rollback()

def get_folder_entries(session):
	return session.query(GitFolder).all()

def get_repo_entries(session):
	return session.query(GitRepo).all()

def get_parent_entries(session):
	return session.query(GitParentPath).all()

def remove_dupes(gitremotes, entries, engine):
	Session = sessionmaker(bind=engine)
	session = Session()
	uniques = []
	dupes = []
	for gr in gitremotes:
		try:
			dupes = session.query(GitRepo).filter(GitRepo.giturl.like(gr.giturl)).all()
		except AttributeError as e:
			logger.warning(f'[dupe] AttributeError {e} gr={gr}')
			continue
		except ArgumentError as e:
			logger.warning(f'[dupe] ArgumentError {e} gr={gr}')
			continue
		if len(dupes) > 0:
			pass
			# logger.warning(f'[dupe] {gr.giturl} {dupes}')
		elif len(dupes) == 0:
			uniques.append(gr)
	logger.info(f'[dupe] {len(uniques)} uniques')
	return uniques
	# [k for k in session.query(GitRepo).filter(GitRepo.giturl.like(gitremotes[1].giturl))]


def get_engine(dbtype):
	dbuser = None
	dbpass = None
	dbhost = None
	dbname = None
	if dbtype == 'mysql':
		dburl = (f"mysql+pymysql://{dbuser}:{dbpass}@{dbhost}/{dbname}?charset=utf8mb4")
		return create_engine(dburl)
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	if dbtype == 'postgresql':
		# dburl = f"postgresql://postgres:foobar9999@{args.dbhost}/{args.dbname}"
		dburl = (f"postgresql://{dbuser}:{dbpass}@{dbhost}/{dbname}")
		return create_engine(dburl)
	if dbtype == 'sqlite':
		return create_engine('sqlite:///gitrepo.db', echo=False, connect_args={'check_same_thread': False})
	else:
		return None

def get_dupes(session):
	# return list of repos with multiple entries
	sql = text('select id,folderid,giturl,count(*) as count from gitrepo group by giturl having count>1;')
	dupes_ = [k._asdict() for k in session.execute(sql).fetchall()]
	dupes = []
	for d in dupes_:
		# find all repos with this giturl
		sql_d = text(f"""select id,folderid,giturl from gitrepo where giturl="{d.get('giturl')}" """)
		repo_dupes = [r._asdict() for r in session.execute(sql_d).fetchall()]

		# set dupe_flag on all repos with this giturl
		for rpd in repo_dupes:
			r = session.query(GitRepo).filter(GitRepo.id==rpd.get('id')).first()
			r.dupe_flag = True
			session.commit()

		# find all folders with this repo
		dupepaths = [session.query(GitFolder).filter(GitFolder.id==k.get('folderid')).first() for k in repo_dupes]
		# logger.debug(f'[d] {d.get("giturl")} {d.get("count")}')
		dupeitem = {
			'gitid': d.get('id'),
			'count': d.get('count'),
			'giturl': d.get('giturl'),
			'folders': [],
		}
		for dp in dupepaths:
			# set dupe_flag on all folders with this repo
			r = session.query(GitFolder).filter(GitFolder.id==dp.id).first()
			r.dupe_flag = True
			session.commit()
			dpitem = {
				'folderid': dp.id,
				'git_path': dp.git_path,
			}
			dupeitem['folders'].append(dpitem)
		# logger.info(f'[d] {dupeitem}')
		dupes.append(dupeitem)
		#[logger.debug(f'[d] {k[0].git_path}') for k in dupepaths]
	logger.info(f'[dupes] found {len(dupes)} dupes')
	return dupes
	# foo = session.query(GitRepo).from_statement(text('select id,giturl,count(*) as count from gitrepo group by giturl having count>1 ')).all()
