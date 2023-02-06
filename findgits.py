#!/usr/bin/python3

import os, sys, signal
import time
import argparse
from pathlib import Path
import glob
from loguru import logger
from datetime import datetime, timedelta
from timeit import default_timer as timer
import subprocess
from configparser import ConfigParser, DuplicateSectionError
from dataclasses import dataclass, field
import random
import datetime
from sqlalchemy import ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Float, DateTime, text, BIGINT, Numeric, DATE,TIME,DATETIME
from sqlalchemy import create_engine
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import asyncio
from functools import partial
from concurrent.futures import (ProcessPoolExecutor, as_completed)


Base = declarative_base()

def generate_id():
	return ''.join(random.choices('0123456789abcdef', k=16))


def get_directory_size(directory):
	#directory = Path(directory)
	total = 0
	try:
		for entry in os.scandir(directory):
			if entry.is_symlink():
				break
			if entry.is_file():
				total += entry.stat().st_size
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
	return total

def get_subfilecount(directory):
	directory = Path(directory)
	try:
		filecount = len([k for k in directory.glob('**/*') if k.is_file()])
	except PermissionError as e:
		logger.warning(f'[err] {e} d:{directory}')
		return 0
	return filecount

def get_subdircount(directory):
	directory = Path(directory)
	dc = 0
	try:
		dc = len([k for k in directory.glob('**/*') if k.is_dir()])
	except (PermissionError,FileNotFoundError) as e:
		logger.warning(f'[err] {e} d:{directory}')
	return dc

class GitFolder(Base):
	__tablename__ = 'gitfolder'
	id = Column(Integer, primary_key =  True)
	gitfolder = Column('gitfolder', String)

	def __init__(self, gitfolder):
		self.gitfolder = gitfolder

class GitClassDB(Base):
	# todo: add foldersize, filecount, lastcommit, lastupdate
	__tablename__ = 'gitrepo'
	# id = Column('id', String, primary_key=True)
	id = Column(Integer, primary_key =  True)
	gitfolder = Column('gitfolder', String)
	giturl = Column('giturl', String)
	remote = Column('remote', String)
	branch = Column('branch', String)
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

	commitmsg = Column('commitmsg', String)

	def __init__(self,  gitfolder:str):
		self.gitfolder = str(gitfolder)
		self.git_config_file = self.gitfolder + '/.git/config'
		self.commitmsg_file = self.gitfolder + '/.git/COMMIT_EDITMSG'
		self.conf = ConfigParser()
		self.get_stats()

	def __repr__(self):
		return f'GitClassDB id={self.id} gitfolder={self.gitfolder} giturl={self.giturl} size={self.folder_size} {self.file_count} {self.subdir_count}'

	def get_stats(self):
		stat = os.stat(self.gitfolder)
		self.gitfolder_ctime = datetime.datetime.fromtimestamp(stat.st_ctime)
		self.gitfolder_atime = datetime.datetime.fromtimestamp(stat.st_atime)
		self.gitfolder_mtime = datetime.datetime.fromtimestamp(stat.st_mtime)

		self.folder_size = get_directory_size(self.gitfolder)
		self.file_count = get_subfilecount(self.gitfolder)
		self.subdir_count = get_subdircount(self.gitfolder)
		self.read_git_config()
		self.read_git_commitmsg()

	def read_git_commitmsg(self):
		if os.path.exists(self.commitmsg_file):
			with open(self.commitmsg_file, 'r') as f:
				self.commitmsg = f.read()
			stat = os.stat(self.commitmsg_file)
			self.commit_ctime = datetime.datetime.fromtimestamp(stat.st_ctime)
			self.commit_atime = datetime.datetime.fromtimestamp(stat.st_atime)
			self.commit_mtime = datetime.datetime.fromtimestamp(stat.st_mtime)

	def read_git_config(self):
		if os.path.exists(self.git_config_file):
			try:
				c = self.conf.read(self.git_config_file)
			except DuplicateSectionError as e:
				logger.warning(f'[rgc] DuplicateSectionError {e} in {self.git_config_file}')
			st = os.stat(self.git_config_file)
			self.config_ctime = datetime.datetime.fromtimestamp(st.st_ctime)
			self.config_atime = datetime.datetime.fromtimestamp(st.st_atime)
			self.config_mtime = datetime.datetime.fromtimestamp(st.st_mtime)
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

def get_git_remote_config(gitfolder):
	gc = None
	giturl = ''
	conf = ConfigParser()
	git_folder = str(gitfolder.parent)
	git_config_file = f'{git_folder}/.git/config'
	try:
		stat = os.stat(git_config_file)
	except FileNotFoundError as e:
		logger.warning(f'[gconfig] file not found {e} gitfolder={gitfolder} git_config_file={git_config_file}')
		return None
	try:
		c = conf.read(git_config_file)
	except DuplicateSectionError as e:
		logger.error(f'[conf] DuplicateSectionError {e} git_config_file={git_config_file}')
		return None
	if conf.has_section('remote "origin"'):
		try:
			remote_section = [k for k in conf.sections() if 'remote' in k][0]
		except IndexError as e:
			logger.error(f'[err] {e} gitfolder={gitfolder} git_config_file={git_config_file} conf={conf.sections()}')
		remote_section_name = remote_section.split(' ')[1].replace('"','')
		branch_section = [k for k in conf.sections() if 'branch' in k][0]
		branch_section_name = branch_section.split(' ')[1].replace('"','')
		try:
			# giturl = [k for k in conf['remote "origin"'].items()][0][1]
			giturl = conf[remote_section]['url']
		except TypeError as e:
			logger.warning(f'[gconfig] typeerror {e} gitfolder={gitfolder} git_config_file={git_config_file} c={c}')
		except KeyError as e:
			logger.warning(f'[gconfig] KeyError {e} gitfolder={gitfolder} git_config_file={git_config_file} c={c}')
		gc = GitClassDB(gitfolder=git_folder, giturl=giturl, remote=remote_section_name, branch=branch_section_name)
		return gc
	else:
		logger.warning(f'[gconfig] no remote origin gitfolder={gitfolder} conf={conf.sections()}')
		return None

def get_git_remote_cmd(gitfolder):
	os.chdir(gitfolder.parent)
	status = subprocess.run(['git', 'remote', '-v',], capture_output=True)
	if status.stdout != b'':
		remotes = status.stdout.decode('utf-8').split('\n')
		parts = None
		try:
			parts = remotes[0].split('\t')
		except IndexError as e:
			logger.warning(f'[ggr] indexerror {e} {gitfolder}')
			return None
		try:
			if parts:
				origin = parts[0]
				giturl = parts[1].split(' ')[0]
				result = {'gitfolder':gitfolder,'remotes':remotes, 'origin':origin, 'giturl':giturl }
				return result
		except IndexError as e:
			logger.warning(f'[ggr] indexerror {e} git={gitfolder} parts={parts}')
			return {}
	else:
		logger.warning(f'[ggr] no gitremote in {gitfolder} stdout={status.stdout} stderr={status.stderr}')
		return {}

def get_sqlite_engine():
	eng = create_engine('sqlite:///gitrepo.db', echo=False, connect_args={'check_same_thread': False})
	return eng

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



def send_to_db(gitremotes, session):
	for gr in gitremotes:
		for k in gr:
			# logger.debug(f'[db] {k}')
	#		for k in gr:
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
				continue
			except ProgrammingError as e:
				logger.error(f'[db] {e} k={k}')
				session.rollback()
				continue

def load_db_entries(engine):
	Base.metadata.create_all(bind=engine)
	Session = sessionmaker(bind=engine)
	session = Session()
	return session.query(GitClassDB).all()

def remove_dupes(gitremotes, entries, engine):
	Session = sessionmaker(bind=engine)
	session = Session()
	uniques = []
	dupes = []
	for gr in gitremotes:
		try:
			dupes = session.query(GitClassDB).filter(GitClassDB.giturl.like(gr.giturl)).all()
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
	# [k for k in session.query(GitClassDB).filter(GitClassDB.giturl.like(gitremotes[1].giturl))]

def get_folder_list(startpath):
	for k in glob.glob(str(Path(startpath))+'/**',recursive=True, include_hidden=True):
		 if Path(k).is_dir() and Path(k).name == '.git':
			 yield Path(k).parent

def get_folders_task(startpath):
	gitfolders = [k for k in get_folder_list(startpath)]
	return gitfolders

def create_remotes_task(gf):
	gr = [GitClassDB(g) for g in gf]
	return gr

async def main(args):
	engine = get_engine(dbtype='sqlite')
	Base.metadata.create_all(bind=engine)
	Session = sessionmaker(bind=engine)
	session = Session()
	entries = load_db_entries(engine)
	read_tasks = []
	gitfolders = []
	with ProcessPoolExecutor() as executor:
		for argspath in args.path:
			read_tasks.append(executor.submit(partial(get_folders_task, argspath)))
			logger.debug(f'[ppe] {argspath} read_tasks={len(read_tasks)}')
	for read_task in as_completed(read_tasks):
		res = read_task.result()
		gitfolders.append([k for k in res])
		logger.debug(f'[ppe] read_task res={len(res)} gitfolders={len(gitfolders)}')
	#gf = as_completed(fs)
	#gitfolders.append(fs.result())
	#logger.debug(f'[ppe] {argspath} fs={fs} gitfolders={len(gitfolders)} ')
	gitremotes = []
	remote_tasks = []
	newlist=[k for k in gitfolders for k in gitfolders]
	with ProcessPoolExecutor() as executor:
		for idx, gf in enumerate(newlist, start=1):
			remote_tasks.append(executor.submit(partial(create_remotes_task, gf)))
	logger.info(f'[g] remote_taskss={len(remote_tasks)} nl={len(newlist)}' )
	db_chunks = []
	for remote_task in as_completed(remote_tasks):
		db_chunks.append(remote_task.result())
	db_data = [k for k in db_chunks for k in db_chunks]
	logger.info(f'[g] db_chunks={len(db_chunks)} db_data={len(db_data)}')
	try:
		send_to_db(db_data,session)
	except Exception as e:
		logger.error(f'[send2db] {e} {type(e)}')
	logger.debug(f'[g] Done')

if __name__ == '__main__':
	myparse = argparse.ArgumentParser(description="findgits", exit_on_error=False)
	myparse.add_argument('path', nargs='+',  metavar='searchpaths')
	args = myparse.parse_args()
	asyncio.run(main(args))
