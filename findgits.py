#!/usr/bin/python3
import argparse
import os
import glob
from pathlib import Path
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from datetime import datetime, timedelta, timezone
from multiprocessing import cpu_count
from threading import Thread
from loguru import logger
from sqlalchemy import text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from dbstuff import (GitFolder, GitParentPath, GitRepo)
from dbstuff import drop_database, get_engine, db_init, get_dupes, db_dupe_info, get_db_info,gitfolder_to_gitparent
from dbstuff import MissingGitFolderException, MissingConfigException
from git_tasks import collect_folders, scan_subfolders,create_git_folders,create_git_repos
from git_tasks import (add_parent_path, scanpath)
from git_tasks import (get_git_log, get_git_show, get_git_status)
from utils import format_bytes
CPU_COUNT = cpu_count()


def show_dupe_info(dupes):
	"""
	show info about dupes
	Parameters: dupes: list - list of dupes
	"""
	dupe_counter = 0
	for d in dupes:
		repdupe = session.query(GitRepo).filter(GitRepo.git_url == d.git_url).all()
		dupe_counter += len(repdupe)
		print(f'[d] gitrepo url:{d.git_url} has {len(repdupe)} dupes found in:')
		for r in repdupe:
			grepo = session.query(GitRepo).filter(GitRepo.git_path == r.git_path).first()
			g_show = get_git_show(grepo)
			lastcommitdate = g_show["last_commit"]
			timediff = grepo.config_ctime - lastcommitdate
			timediff2 = datetime.now() - lastcommitdate
			print(f'\tid:{grepo.id} path={r.git_path} age {timediff.days} days td2={timediff2.days}')
	print(f'[getdupes] {dupe_counter} dupes found')

def main_scanpath(gpp:GitParentPath, session:sessionmaker) -> None:
	"""
	main scanpath function
	Parameters: gpp: GitParentPath scan all subfolders if this gpp, session: sessionmaker object
	"""
	scantime_start = datetime.now()
	try:
		scanpath(gpp, session)
	except OperationalError as e:
		logger.error(f'[msp] OperationalError: {e}')
		return None
	scantime_end = (datetime.now() - scantime_start).total_seconds()
	logger.debug(f'[msp] scan_time:{scantime_end}')
	return 0

def dbcheck(session) -> int:
	"""
	run db checks:
		* todo check for missing folders
		* todo check for missing repos
	"""
	gpp = session.query(GitParentPath).all()
	ggp_count = len(gpp)
	return ggp_count

if __name__ == '__main__':
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('--addpath', dest='addpath', help='add new parent path to db and run scan on it')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('--listpaths', action='store_true', help='list gitparentpaths in db', dest='listpaths')
	myparse.add_argument('--fullscan', action='store_true', default=False, dest='fullscan',help='run full scan on all parent paths in db')
	myparse.add_argument('--scanpath', help='Scan single GitParent, specified by ID. Use --listpaths to get IDs', action='store', dest='scanpath')
	myparse.add_argument('--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--dbcheck', help='run checks', action='store_true', default=False, dest='dbcheck')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	engine = get_engine(dbtype=args.dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	if args.dbcheck:
		res = dbcheck(session)
		print(f'dbcheck res: {res}')
	if args.dropdatabase:
		drop_database(engine)
	if args.listpaths:
		gpp = session.query(GitParentPath).all()
		for gp in gpp:
			print(f'[listpaths] id={gp.id} path={gp.folder} last_scan={gp.last_scan} scan_time={gp.scan_time}')
	if args.getdupes:
		if args.dbmode == 'postgresql':
			logger.warning(f'[dbinfo] postgresql dbinfo not implemented')
		else:
			dupes = get_dupes(session)
			# session.query(GitRepo.id, GitRepo.git_url, func.count(GitRepo.git_url).label("count")).group_by(GitRepo.git_url).order_by(func.count(GitRepo.git_url).desc()).limit(10).all()
	if args.scanpath:
		mainres = None
		gpp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		if gpp:
			existing_entries = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
			try:
				mainres = main_scanpath(gpp, session)
			except OperationalError as e:
				logger.error(f'[scanpath] error: {e}')
			if mainres:
				entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
				logger.info(f'[scanpath] scanning {gpp.folder} id={gpp.id} existing_entries={existing_entries} after scan={entries_afterscan}')
		else:
			logger.warning(f'Path with id {args.scanpath} not found')
	if args.fullscan:
		t0 = datetime.now()
		scan_result = collect_folders(args.dbmode)
		for gp in session.query(GitParentPath).all():
			logger.info(f'[fullscan] id={gp.id} path={gp.folder} last_scan={gp.last_scan} scan_time={gp.scan_time} res={len(scan_result[gp.id])}')
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] collect done t:{t1} scan_result:{len(scan_result)} starting create_git_folders')
		git_folders_result = create_git_folders(args.dbmode, scan_result)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] create_git_folders done t:{t1} git_folders_result:{git_folders_result} starting create_git_repos')
		git_repo_result = create_git_repos(args.dbmode)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] create_git_repos done t:{t1} git_folders_result:{git_folders_result} starting create_git_repos')
	if args.dbinfo:
		if args.dbmode == 'postgresql':
			logger.warning(f'[dbinfo] postgresql dbinfo not implemented')
		else:
			db_dupe_info(session)
			get_db_info(session)
	if args.addpath:
		t0 = datetime.now()
		new_gpp = add_parent_path(args.addpath, session)
		if new_gpp:
			session.add(new_gpp)
			session.commit()
			logger.debug(f'[addpath] {new_gpp}')
		else:
			logger.warning(f'[addpath] {args.addpath} already in db')


		# 	parent_subfolders = scan_subfolders(new_gpp)
		# 	if len(parent_subfolders) > 0:
		# 		for new_subgpp in parent_subfolders:
		# 			sub_gpp = GitParentPath(new_subgpp)
		# 			session.add(sub_gpp)
		# 			session.commit()
		# 			logger.info(f'[*] {new_subgpp} from {new_gpp}')
		# 			scanpath(sub_gpp, session)
		# 	scanpath(new_gpp, session)
		# gppcount = session.query(GitParentPath).count()
		# t1 = (datetime.now() - t0).total_seconds()
		# logger.debug(f'[*] gppcount:{gppcount} t:{t1}')
