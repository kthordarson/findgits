#!/usr/bin/python3
import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from queue import SimpleQueue as Queue
from threading import Thread

from loguru import logger
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError,
                            IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dbstuff import (GitFolder, GitParentPath, GitRepo, MissingConfigException,
                     add_path, collect_repo, db_init, dupe_view_init, get_dupes,
                     get_engine, get_folder_entries, get_parent_entries,
                     get_repo_entries, listpaths, scanpath)
from utils import get_folder_list


class FolderCollector(Thread):
	def __init__(self, gsp:GitParentPath):
		Thread.__init__(self)
		self.gsp = gsp
		self.folders = []
		self.kill = False

	def __repr__(self):
		return f'FolderCollector({self.gsp}) f:{len(self.folders)} k:{self.kill}'

	def run(self):
		#self.folders = [) for
		try:
			self.folders = [GitFolder(k, self.gsp) for k in get_folder_list(self.gsp.folder)]
		except KeyboardInterrupt as e:
			self.kill = True
			self.join(timeout=1)

class RepoCollector(Thread):
	def __init__(self, git_path, session:sessionmaker):
		Thread.__init__(self)
		self.git_path = git_path
		self.kill = False
		self.session = session

	def run(self):
		try:
			collect_repo(self.git_path, self.session)
		except AttributeError as e:
			logger.error(f'{self} {e} self.git_path {self.git_path} {type(self.git_path)}')
			#self.join(timeout=1)
		except Exception as e:
			logger.error(f'{self} {e} self.git_path {self.git_path} {type(self.git_path)}')
			#self.join(timeout=1)
			# raise AttributeError(f'{self} {e} self.git_path {self.git_path} {type(self.git_path)}')

def runscan(session:sessionmaker):
	parents = session.query(GitParentPath).all()
	p_threads = []
	g_threads = []
	folderlist = []
	for p in parents:
		pfc = FolderCollector(p)
		p_threads.append(pfc)
		pfc.start()
	for p in p_threads:
		try:
			p.join()
		except Exception as e:
			logger.error(f'[!] {e} {type(e)} in {p}')
		rcs = [RepoCollector(k, session) for k in p.folders]
		_ = [r.start() for r in rcs]
		logger.debug(f'[runscan] {p} done {len(rcs)}')

def import_paths(pathfile, session:sessionmaker):
	# import paths from file
	if not os.path.exists(pathfile):
		logger.error(f'[importpaths] {pathfile} not found')
		return
	with open(pathfile, 'r') as f:
		for line in f:
			if os.path.exists(line):
				add_path(line.strip(), session)
			else:
				logger.warning(f'[importpaths] {line} not found')

def dbdump(backupfile, engine):
	# dump db to file
	pass
	# logger.info(f'[dbdump] {backupfile}')
	# meta = MetaData()
	# meta.reflect(bind=engine)
	# result = {}
	# for table in meta.sorted_tables:
	# 	result[table.name] = [dict(row) for row in engine.execute(table.select())]
	# return json.dumps(result)

if __name__ == '__main__':
	engine = get_engine(dbtype='mysql')
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)

	myparse = argparse.ArgumentParser(description="findgits", exit_on_error=False)
	myparse.add_argument('--addpath', nargs='?', dest='addpath')
	myparse.add_argument('--importpaths', nargs='?', dest='importpaths')
	myparse.add_argument('--dbdump', nargs='?', dest='dbdump')
	myparse.add_argument('--listpaths', action='store_true', default=False, dest='listpaths')
	myparse.add_argument('--dumppaths', action='store_true', default=False, dest='dumppaths')
	myparse.add_argument('--runscan', action='store_true', default=False, dest='runscan')
	myparse.add_argument('--scanpath', nargs='?', help='run scan on path, specify pathid', action='store', dest='scanpath')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	if args.getdupes:
		dupe_view_init(session)
		dupe_repos = get_dupes(session)
	if args.scanpath:
		scanpath(args.scanpath, session)
	if args.runscan:
		foobar = runscan(session)
	if args.listpaths:
		listpaths(session, args.dumppaths)
	if args.addpath:
		add_path(args.addpath, session)
	if args.importpaths:
		# read paths from text file and import
		import_paths(args.importpaths, session)
	if args.dbdump:
		# dump database to file
		dbdump(args.dbdump, engine)
