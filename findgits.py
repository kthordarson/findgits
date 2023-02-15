#!/usr/bin/python3
from run import MainApp
import os, sys
import time
import argparse
from pathlib import Path
from loguru import logger
from datetime import datetime, timedelta
from threading import Thread
from queue import SimpleQueue as Queue
from sqlalchemy import create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from concurrent.futures import (ProcessPoolExecutor, as_completed)

from dbstuff import GitRepo, GitFolder, send_to_db, get_engine, db_init, send_gitfolder_to_db, get_dupes
from dbstuff import  get_folder_entries, get_repo_entries, get_parent_entries
from dbstuff import MissingConfigException, GitParentPath

from utils import get_folder_list



def create_folders_task(gitpath):
	gf = GitFolder(gitpath)
	return gf

def create_remotes_task(gf):
	gr = GitRepo(gf)
	return gr

def worker_task(gitpath):
	gf = GitFolder(gitpath)
	gr = GitRepo(gf)
	return gr,gf

class FolderScanner(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.kill = False
		self.queue = Queue()
	def run(self):
		while not self.kill:
			if self.kill:
				break
			while not self.queue.empty():
				item = self.queue.get()

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
	logger.debug(f'[collect_git_folders] gitfolders={len(gitfolders)}')

def collect_repo(gf, session):
	try:
		# construct repo object from gf (folder)
		gr = GitRepo(gf)
	except MissingConfigException as e:
		logger.error(f'[cgr] {e} gf={gf}')
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
		session.add(gr)
		session.commit()
		# logger.debug(f'[!] newgitrepo {gr} ')


def listpaths(session):
	gsp_entries = get_parent_entries(session)
	for gsp in gsp_entries:
		sql = text(f"SELECT COUNT(*) as count FROM gitfolder WHERE gitfolder.parent_id = {gsp.id}")
		res = session.execute(sql).fetchone()._asdict()
		logger.info(f'[gsp] {gsp} folders={res.get("count")}')


def add_path(path, session):
	# add new path to config  db
	if not os.path.exists(path):
		logger.error(f'[addpath] {path} not found')
		return

	# check db entries for invalid paths and remove
	gsp_entries = get_parent_entries(session)
	_ = [session.delete(k) for k in gsp_entries if not os.path.exists(k.folder)]
	session.commit()

	if path.endswith('/'):
		path = path[:-1]
	if path not in gsp_entries:
		gsp = GitParentPath(path)
		logger.debug(f'[add_path] path={path} gsp={gsp} to {gsp_entries}')
		session.add(gsp)
		session.commit()
		listpaths(session)
	else:
		logger.warning(f'[add_path] path={path} already in config')

def scanpath(scanpath, session):
	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	gsp = session.query(GitParentPath).filter(GitParentPath.id == str(scanpath)).first()
	logger.debug(f'[scanpath] scanpath={scanpath} path_q={gsp}')
	gitfolders = [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]
	logger.info(f'[scanpath] gitsearchpath={gsp} found {len(gitfolders)} gitfolders')
	_ = [session.add(k) for k in gitfolders]
	try:
		session.commit()
	except DataError as e:
		logger.error(f'[scanpath] dataerror {e} gsp={gsp}')
		raise DataError(f'[scanpath] dataerror {e} gsp={gsp}')
	#collect_git_folders(gitfolders, session)
	folder_entries = get_folder_entries(session)
	logger.debug(f'[scanpath] folder_entries={len(folder_entries)}')
	for gf in folder_entries:
		collect_repo(gf, session)
	repo_entries = get_repo_entries(session)
	logger.debug(f'[scanpath] folder_entries={len(folder_entries)} repo_entries={len(repo_entries)}')

class Collector(Thread):
	def __init__(self, gsp):
		Thread.__init__(self)
		self.gsp = gsp
		self.folders = []
		self.gfl = []
		self.kill = False

	def __repr__(self):
		return f'Collector({self.gsp}) folders:{len(self.folders)} gfl:{len(self.gfl)} kill:{self.kill}'

	def run(self):
		#self.folders = [) for
		try:
			self.gfl = [k for k in get_folder_list(self.gsp.folder)]
		except KeyboardInterrupt as e:
			self.kill = True
			self.join(timeout=1)
		logger.debug(f'{self} run')
		for g in self.gfl:
			if self.kill:
				break
			self.folders.append(GitFolder(g, self.gsp))

	#return [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]

def runscan(session):
	# scan all paths in config/db
	gsp_entries = get_parent_entries(session)
	#gsp_entries = [config.get(k, 'path') for k in config.sections()]
	collectors = []
	gfl = []
	for gsp in gsp_entries:
		t = Collector(gsp)
		collectors.append(t)
		t.start()
		logger.info(f'[t] {t} started')
	logger.debug(f'[runscan] collectors={len(collectors)}')
	for t in collectors:
		try:
			t.join()
		except KeyboardInterrupt as e:
			for t in collectors:
				t.kill = True
				t.join(timeout=0)
				logger.debug(f'{t} killed')
				_ = [collect_repo(t, session) for t in t.folders]
			logger.warning(f'[runscan] {e} collectors={len(collectors)} gfl={len(gfl)}')
			break
		_ = [collect_repo(t, session) for t in t.folders]
		#gfl.append(t.folders)
		logger.debug(f'[runscan] {t} done gfl={len(gfl)}')
	# gfl = [k.folders for k in collectors]
	#for gf in gfl:
#		collect_repo(gf, session)
	repo_entries = get_repo_entries(session)
	logger.debug(f'[runscan] collectors={len(collectors)} gfl={len(gfl)} repo_entries={len(repo_entries)}')

if __name__ == '__main__':
	engine = get_engine(dbtype='mysql')
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	myparse = argparse.ArgumentParser(description="findgits", exit_on_error=False)
	myparse.add_argument('--addpath', nargs='?', dest='addpath')
	myparse.add_argument('--listpaths', action='store_true', default=False, dest='listpaths')
	myparse.add_argument('--runscan', action='store_true', default=False, dest='runscan')
	myparse.add_argument('--scanpath', nargs='?', help='run scan on path, specify pathid', action='store', dest='scanpath')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	if args.getdupes:
		dupe_repos = get_dupes(session)
	if args.scanpath:
		scanpath(args.scanpath, session)
	if args.runscan:
		foobar = runscan(session)
	if args.listpaths:
		listpaths(session)
	if args.addpath:
		add_path(args.addpath, session)


