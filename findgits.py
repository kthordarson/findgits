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
                     get_repo_entries, scanpath, scanpath_thread, show_dbinfo)
from dbstuff import get_folder_list, drop_database


CPU_COUNT = cpu_count()


def runscan(dbmode):
	t0 = datetime.now()
	engine = get_engine(dbtype=dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	tasks = []
	gsp = session.query(GitParentPath).all()
	logger.info(f'[runscan] {datetime.now()-t0} CPU_COUNT={CPU_COUNT} gsp={len(gsp)}')
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		# start thread for each gitparentpath
		for git_parentpath in gsp:
			tasks.append(executor.submit(get_folder_list, git_parentpath))
			git_parentpath.last_scan = datetime.now()
			logger.debug(f'[runscan] {datetime.now()-t0} {git_parentpath} {git_parentpath.first_scan} {git_parentpath.last_scan} get_folder_list threads {len(tasks)} ')
		for res in as_completed(tasks):
			r = res.result()
			gitparent_ = r["gitparent"]
			gitparent = session.query(GitParentPath).filter(GitParentPath.folder == gitparent_.folder).first()
			gitparent.last_scan = datetime.now()
			git_folders = r["res"]
			scan_time = r["scan_time"]
			gitparent.scan_time = scan_time
			#session.add(gitparent)
			session.add(gitparent)
			session.commit()
			#gfl.append(r['res'])
			logger.info(f'[runscan] {datetime.now()-t0} {len(git_folders)} gitfolders from {gitparent} scan_time={scan_time} {gitparent.scan_time} ' )
			cnt = 0
			ups = 0
			for gf in git_folders:
				folder_check = None
				folder_check = session.query(GitFolder).filter(GitFolder.git_path == str(gf)).first()
				if not folder_check:
					# add new entries
					_t0_ = datetime.now()
					git_folder = GitFolder(gf, gitparent)
					git_folder.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_folder)
					session.commit()
					_t0_ = datetime.now()
					git_repo = GitRepo(git_folder)
					git_repo.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_repo)
					session.commit
					cnt += 1
				else:
					# update stats for existing entries
					#logger.debug(f'[runscan] gitparent={r["gitparent"]} r={len(r["res"])} fc={len(folder_check)} gc0={folder_check[0]}')
					# for updatefolder_ in folder_check:
					# gf_update = session.query(GitFolder).filter(GitFolder.git_path == str(updatefolder_.git_path)).first()
					folder_check.last_scan = datetime.now()
					#scan_time = (datetime.now() - _t0_).total_seconds()
					# _t0_ = datetime.now()
					gr_update = session.query(GitRepo).filter(GitRepo.git_path == str(folder_check.git_path)).first()
					gr_update.last_scan = datetime.now()
					#gru.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(folder_check)
					session.add(gr_update)
					session.commit()
					ups += 1
					#logger.debug(f'[runscan] gitparent={gitparent} fc={len(folder_check)} gc0={folder_check[0]} updatefolder_={updatefolder_} gf_update={gf_update} gr_update={gr_update}')
			logger.debug(f'[runscan] {datetime.now()-t0} {len(r["res"])} gitfolders from {r["gitparent"]} entries {cnt}/{ups} ')

	gsp = session.query(GitParentPath).all()
	repos = session.query(GitRepo).all()
	folders = session.query(GitFolder).all()
	results = {'gsp':len(gsp), 'repos':len(repos), 'folders':len(folders)}
	return results


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
	myparse.add_argument('--addpath', dest='addpath')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('--listpaths', action='store', default='all', help='list paths in db, specify "all" or id', dest='listpaths', metavar='krem')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--runscan', action='store_true', default=False, dest='runscan')
	myparse.add_argument('--scanpath', help='run scan on path, specify pathid', action='store', dest='scanpath')
	myparse.add_argument('--scanpath_threads', help='run scan on path, specify pathid. threadmode.', action='store', dest='scanpath_threads')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	engine = get_engine(dbtype=args.dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	logger.info(f'[main] dbmode={args.dbmode} bind={session.bind} {session.bind.driver}')
	if args.dropdatabase:
		drop_database(engine)
	if args.getdupes:
		sql = text('select * from newdupeview order by count desc limit 10;')
		dupes = session.execute(sql).all()
		for d in dupes:
			print(d)
	if args.scanpath:
		gsp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		# entries = get_folder_list(gsp)
		entries = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)}')
		scanpath(gsp, args.dbmode)
		entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		dupe_view_init(session)
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
		dupe_view_init(session)
		logger.info(f'[*] runscan done res={scan_results} ')

	if args.dbinfo:
		# show db info
		show_dbinfo(session)
		if args.listpaths:
			if args.listpaths == 'all':
				git_parent_entries = get_parent_entries(session)
			else:
				git_parent_entries = session.query(GitParentPath).filter(GitParentPath.id == str(args.listpaths)).all()
			for gpe in git_parent_entries:
				fc = session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).count()
				rc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
				print(f'[*] {gpe.id} {gpe.folder} {fc} {rc}')

	if args.addpath:
		new_gsp = add_path(args.addpath, session)
		logger.debug(f'[*] new path: {new_gsp}')
		# scanpath(new_gsp.id, session)
	if args.importpaths:
		# read paths from text file and import
		import_paths(args.importpaths, session)
