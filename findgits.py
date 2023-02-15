#!/usr/bin/python3
from run import MainApp
import os, sys
import time
import argparse
import json
from pathlib import Path
from loguru import logger
from datetime import datetime, timedelta
from threading import Thread
from queue import SimpleQueue as Queue
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from concurrent.futures import (ProcessPoolExecutor, as_completed)

from dbstuff import GitRepo, GitFolder, send_to_db, get_engine, db_init, send_gitfolder_to_db, get_dupes
from dbstuff import  get_folder_entries, get_repo_entries, get_parent_entries
from dbstuff import MissingConfigException, GitParentPath
from dbstuff import collect_repo, add_path, scanpath, listpaths, get_folder_list
from utils import get_folder_list

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

def import_paths(pathfile, session):
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
