#!/usr/bin/python3
import argparse
import json
import os
import sys
import time
import pymysql
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from datetime import datetime, timedelta
from pathlib import Path
from queue import SimpleQueue as Queue
from threading import Thread
from multiprocessing import cpu_count

from loguru import logger
from sqlalchemy import Engine
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError,
                            IntegrityError, OperationalError, ProgrammingError, InvalidRequestError)
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dbstuff import (GitFolder, GitParentPath, GitRepo, MissingConfigException,
                     add_path, collect_repo, db_init, dupe_view_init, get_dupes,
                     get_engine, get_folder_entries, get_parent_entries,
                     get_repo_entries, listpaths, scanpath, scanpath_thread, show_dbinfo)
from dbstuff import get_folder_list, drop_database


CPU_COUNT = cpu_count()


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
	def __init__(self, git_path, engine:Engine):
		Thread.__init__(self)
		self.git_path = git_path
		self.kill = False
		#self.dbmode = dbmode
		self.engine = engine # get_engine(dbtype=dbmode)
		Session = sessionmaker(bind=self.engine)
		self.session = Session()
		self.collect_status = 'unknown'

		#self.session = session

	def run(self):
		try:
			self.collect_status = collect_repo(self.git_path, self.session)
		except AttributeError as e:
			errmsg = f'AttributeError {self} {e} self.git_path {self.git_path} engine={self.engine} session={self.session}'
			logger.error(errmsg)
			self.collect_status = errmsg
			raise AttributeError(errmsg)
			#self.join(timeout=1)
		except InvalidRequestError as e:
			errmsg = f'InvalidRequestError {self} {e} self.git_path {self.git_path} '
			logger.error(errmsg)
			self.collect_status = errmsg
			session.rollback()
		except pymysql.err.InternalError as e:
			errmsg = f'pymysql.err.InternalError {self} {e} self.git_path {self.git_path} '
			logger.error(errmsg)
			self.collect_status = errmsg
			session.rollback()
		except Exception as e:
			errmsg = f'Unhandled Exception {self} {e} {type(e)} self.git_path {self.git_path} '
			self.collect_status = errmsg
			logger.error(errmsg)
			# session.rollback()
			#self.join(timeout=1)
			# raise AttributeError(f'{self} {e} self.git_path {self.git_path} ')

def repocollectionthread(git_folder:str, gsp:GitParentPath, session):
	git_folder = GitFolder(git_folder, gsp)
	session.add(git_folder)
	session.commit()
	git_repo = GitRepo(git_folder)
	session.add(git_repo)
	session.commit

def runscan(dbmode):
	t0 = datetime.now()
	engine = get_engine(dbtype=dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	gsp = session.query(GitParentPath).all()
	gitfolders = []
	tasks = []
	gfl = []
	logger.info(f'[runscan] {datetime.now()-t0} CPU_COUNT={CPU_COUNT} gsp={len(gsp)}')
	folder_entries = []
	tasks = []
	results = []
	collector_threads = []
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		for git_parentpath in gsp:
			logger.debug(f'[runscan] {datetime.now()-t0} scanning {git_parentpath} threads {len(tasks)} gsp={len(gsp)}')
			tasks.append(executor.submit(get_folder_list, git_parentpath))
		for res in as_completed(tasks):
			r = res.result()
			#gfl.append(r['res'])
			for gf in r['res']:
				git_folder = GitFolder(gf, r["gitparent"])
				session.add(git_folder)
				session.commit()
				git_repo = GitRepo(git_folder)
				session.add(git_repo)
				session.commit
			logger.debug(f'[runscan] {len(r["res"])} gitfolders from {r["gitparent"]} ')
	return results

def xxxrunscan(dbmode):
	t0 = datetime.now()
	engine = get_engine(dbtype=dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	gsp = session.query(GitParentPath).all()
	gitfolders = []
	tasks = []
	gfl = []
	logger.info(f'[runscan] {datetime.now()-t0} CPU_COUNT={CPU_COUNT} gsp={len(gsp)}')
	folder_entries = []
	tasks = []
	results = []
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		for git_f in gsp:
			tasks.append(executor.submit(get_folder_list, git_f))
		logger.debug(f'[runscan] {datetime.now()-t0} gfl threads {len(tasks)} gsp={len(gsp)}')
		for res in as_completed(tasks):
			r = res.result()
			#gfl.append(r['res'])
			for gf in r['res']:
				session.add(GitFolder(gf, git_f))
			session.commit()
			logger.debug(f'[runscan] {datetime.now()-t0} gfl {len(r["res"])}')

		for git_pp in session.query(GitParentPath).all():
			gfe = [k for k in session.query(GitFolder).filter(GitFolder.parent_id == git_pp.id).all()]
			folder_entries.append(gfe)
		logger.debug(f'[runscan] {datetime.now()-t0} gfe {len(folder_entries)} ')
	threadend = datetime.now()

	tasks = []
	results = []

	tasks = []
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		gfe = [k for k in session.query(GitFolder).all()]
		for g in gfe:
			tasks.append(executor.submit(scanpath_thread, g, dbmode))
		logger.debug(f'[runscan] {datetime.now()-t0} spt {len(tasks)}')
		for res in as_completed(tasks):
			r = res.result()
			results.append(r)
		logger.debug(f'[runscan] {datetime.now()-t0} results={len(results)}')
	logger.info(f'[runscan] {datetime.now()-t0} ')

	return results

	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	#gsp = session.query(GitParentPath).filter(GitParentPath.id == str(scanpath)).first()
	#gitfolders = [GitFolder(k, gsp) for k in get_folder_list(gsp.folder)]
	# for g in gsp:
	# 	t = Thread(target=scanpath_thread, args=(g, dbmode))
	# 	logger.debug(f'[runscan] starting {t}')
	# 	t.run()

def old_runscan(engine:Engine):
	#engine = get_engine(dbtype=dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	parents = session.query(GitParentPath).all()
	fc_threads = []
	rcstatus = []
	rc_threads = []
	#g_threads = []
	#folderlist = []
	for p in parents:
		pfc = FolderCollector(p)
		fc_threads.append(pfc)
	logger.info(f'[runscan] FolderCollector {len(fc_threads)} threads...')
	_ = [p.start() for p in fc_threads]
	for p in fc_threads:
		try:
			p.join()
		except Exception as e:
			logger.error(f'[!] {e} {type(e)} in {p}')
		for folder in p.folders:
			rc_threads.append(RepoCollector(folder, engine))
	logger.info(f'[runscan] FolderCollector found {len(p.folders)} folders. Starting {len(rc_threads)} RepoCollector threads ')
	for k in rc_threads:
		k.start()
		#_ = [p.start() for p in k]
	for rc in rc_threads:
		rc.join()
		rcstatus.append(rc.collect_status)
	#	_ = [r.start() for r in rcs]
	logger.debug(f'[runscan] done rcstatus={len(rcstatus)}')
	return rcstatus

def import_paths(pathfile, session:sessionmaker):
	# import paths from file
	if not os.path.exists(pathfile):
		logger.error(f'[importpaths] {pathfile} not found')
		return
	with open(pathfile, 'r') as f:
		fdata_ = f.readlines()
	fdata = [k.strip() for k in fdata_]
	# todo finish

def dbdump(backupfile, engine):
	# dump db to file
	pass

if __name__ == '__main__':

	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('--addpath', nargs='?', dest='addpath')
	myparse.add_argument('--importpaths', nargs='?', dest='importpaths')
	myparse.add_argument('--dbdump', nargs='?', dest='dbdump')
	myparse.add_argument('--listpaths', action='store_true', default=False, dest='listpaths')
	myparse.add_argument('--dumppaths', action='store_true', default=False, dest='dumppaths')
	myparse.add_argument('--runscan', action='store_true', default=False, dest='runscan')
	myparse.add_argument('--scanpath', nargs='?', help='run scan on path, specify pathid', action='store', dest='scanpath')
	myparse.add_argument('--scanpath_threads', nargs='?', help='run scan on path, specify pathid. threadmode.', action='store', dest='scanpath_threads')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', default='sqlite', dest='dbmode')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()

	engine = get_engine(dbtype=args.dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	logger.info(f'[main] dbmode={args.dbmode} bind={session.bind} {session.bind.driver}')
	if args.dropdatabase:
		drop_database(engine)
	if args.dbinfo:
		# show db info
		show_dbinfo(session)
	if args.getdupes:
		dupe_view_init(session)
		dupe_repos = get_dupes(session)
	if args.scanpath:
		gsp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		# entries = get_folder_list(gsp)
		entries = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)}')
		scanpath(gsp, args.dbmode)
		entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)} after scan={len(entries_afterscan)}')
	if args.scanpath_threads:
		gsp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath_threads).first()
		entries = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath_threads] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)}')
		gfl = get_folder_list(gsp)
		tasks = []
		with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
			for g_ in gfl:
				g = gfl[g]['gfl']
				gitfolder = GitFolder(g, gsp)
				session.add(gitfolder)
				session.commit()
				tasks.append(executor.submit(scanpath_thread, gitfolder, args.dbmode))

			#scanpath_thread(gitfolder, args.dbmode)
		# gsp = session.query(GitParentPath).all()
		#print(gfl)
		#for gf in gfl['gf']:
			#print(gf)
		#scanpath_thread(GitFolder(gfl['gf'], gsp), args.dbmode)
	if args.runscan:
		scan_results = runscan(args.dbmode)
		logger.info(f'[*] runscan done {len(scan_results)} ')
	if args.listpaths:
		p = listpaths(session, args.dumppaths)
		print(p)
	if args.addpath:
		new_gsp = add_path(args.addpath, session)
		logger.debug(f'[*] new path: {new_gsp}')
		# scanpath(new_gsp.id, session)
	if args.importpaths:
		# read paths from text file and import
		import_paths(args.importpaths, session)
	if args.dbdump:
		# dump database to file
		dbdump(args.dbdump, engine)
