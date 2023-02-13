#!/usr/bin/python3
from PyQt5.QtWidgets import QApplication
from run import MainApp
import os, sys
import time
import argparse
from configparser import ConfigParser
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

from dbstuff import GitRepo, GitFolder, send_to_db, get_engine, db_init, send_gitfolder_to_db
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

def runscan(config, session):
	# scan all paths in config/db
	# todo: select scan folders from config or db?
	gsp_entries = get_parent_entries(session)
	config_entries = [config.get(k, 'path') for k in config.sections()]
	for gitsearchpath in config_entries:
		gsp = GitParentPath(gitsearchpath)
		folder_q = session.query(GitParentPath).filter(GitParentPath.folder == str(gsp.folder)).first()
		if folder_q:
			gsp = folder_q
			logger.info(f'[runscan] gsp={gsp}')
		else:
			# add new parent path
			logger.info(f'[runscan] adding new parent path {gsp}')
			session.add(gsp)
			session.commit()
		logger.info(f'[runscan] gitsearchpath={gsp}')
		gitfolders = [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]
		logger.info(f'[runscan] gitsearchpath={gsp} found {len(gitfolders)} gitfolders')
		collect_git_folders(gitfolders, session)
		folder_entries = get_folder_entries(session)
		logger.debug(f'[runscan] folder_entries={len(folder_entries)}')
		for gf in folder_entries:
			collect_repo(gf, session)
		repo_entries = get_repo_entries(session)
		logger.debug(f'[runscan] folder_entries={len(folder_entries)} repo_entries={len(repo_entries)}')

def listpaths(config, session):
	gsp_entries = get_parent_entries(session)
	try:
		for k in [config.get(k, 'path') for k in config.sections()]:
			logger.info(f'[confpath] {k}')
	except KeyError as e:
		logger.error(f'[listpaths] {e}')
	for gsp in gsp_entries:
		logger.info(f'[gsp] {gsp}')

def read_config():
	config = ConfigParser()
	config.read('findgits.ini')
	return config

def add_path(path, config, session):
	# add new path to config file and db
	gsp_entries = get_parent_entries(session)
	config_entries = [config.get(k, 'path') for k in config.sections()]
	if path.endswith('/'):
		path = path[:-1]
	if path not in config_entries:
		logger.debug(f'[add_path] path={path} to {config}')
		config.add_section(path)
		config.set(path, 'path', path)
		with open('findgits.ini', 'w') as f:
			config.write(f)
		gsp = GitParentPath(path)
		session.add(gsp)
		session.commit()
		listpaths(config, session)
	else:
		logger.warning(f'[add_path] path={path} already in config')

def scanpath(scanpath, config, session):
	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	gsp = session.query(GitParentPath).filter(GitParentPath.id == str(scanpath)).first()
	logger.debug(f'[scanpath] scanpath={scanpath} path_q={gsp}')
	gitfolders = [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]
	logger.info(f'[scanpath] gitsearchpath={gsp} found {len(gitfolders)} gitfolders')
	collect_git_folders(gitfolders, session)
	folder_entries = get_folder_entries(session)
	logger.debug(f'[scanpath] folder_entries={len(folder_entries)}')
	for gf in folder_entries:
		collect_repo(gf, session)
	repo_entries = get_repo_entries(session)
	logger.debug(f'[scanpath] folder_entries={len(folder_entries)} repo_entries={len(repo_entries)}')

def get_dupes(session):
	# return list of repos with multiple entries
	# select giturl,count(*) as count from gitrepo group by giturl having count>1;
	# select giturl,count(*) as count from gitrepo group by giturl having count>1 order by count;
	sql = text('select id,folderid,giturl,count(*) as count from gitrepo group by giturl having count>1;')
	dupes = session.execute(sql).fetchall()
	for d in dupes:
		dup = d._asdict()
		repo_id = dup.get('id')
		folder_id = dup.get('folderid')
		giturl = dup.get('giturl')
		sql = text(f'select id,folderid,giturl from gitrepo where giturl="{giturl}"')
		#repo_dupes_ = session.execute(sql).fetchall()
		repo_dupes = [r._asdict() for r in session.execute(sql).fetchall()]
		dupepaths = [session.query(GitFolder).filter(GitFolder.id==k.get('folderid')).all() for k in repo_dupes]
		logger.debug(f'[d] {repo_id} {folder_id} {giturl} {dup.get("count")}')
		for dp in dupepaths:
			logger.info(f'\t{dp[0].git_path}')
		#[logger.debug(f'[d] {k[0].git_path}') for k in dupepaths]
	logger.info(f'[dupes] found {len(dupes)} dupes')
	return dupes
	# foo = session.query(GitRepo).from_statement(text('select id,giturl,count(*) as count from gitrepo group by giturl having count>1 ')).all()

if __name__ == '__main__':
	engine = get_engine(dbtype='sqlite')
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	config = read_config()
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
		scanpath(args.scanpath, config, session)
	if args.runscan:
		runscan(config, session)
	if args.listpaths:
		listpaths(config, session)
	if args.addpath:
		add_path(args.addpath, config, session)


