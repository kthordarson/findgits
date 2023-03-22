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
from git_tasks import run_full_scan
from git_tasks import (add_path, scanpath)
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
	gspscantime0 = gpp.scan_time
	scanpath(gpp, session)
	scantime_end = (datetime.now() - scantime_start).total_seconds()
	gpp.scan_time = scantime_end
	gpp.last_scan = datetime.now()
	session.commit()
	logger.debug(f'[mainscanpath] timecheck scantime_start: {scantime_start} gspt0:{gspscantime0} gspt1:{gpp.scan_time} scantime:{scantime_end}')

def dbcheck(session) -> str:
	"""
	run db checks:
		* check if any subfolders of each gitparentpath contain more than one gitfolder, if so, turn into gitparentpath
		* todo check for missing folders
		* todo check for missing repos
	"""
	gpp = session.query(GitParentPath).all()
	ggp_count = len(gpp)
	gflist_to_convert = []
	for gp in gpp:
		sub_count = 0
		logger.info(f'[chk] scanning {gp} for subgitfolders')
		try:
			subfolders = session.query(GitFolder).filter(GitFolder.parent_id == gp.id).all()
		except OperationalError as e:
			logger.error(f'[chk] {e} gp: {gp}')
			continue
		for sf in subfolders:
			sub_gits = [k for k in glob.glob(str(Path(sf.git_path))+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir()]
			if len(sub_gits) > 1:
				sub_count += len(sub_gits)
				sf.is_parent = True
				session.commit()
				# newgpp = gitfolder_to_gitparent(sf, session)
				# logger.debug(f'[chk] {sf} contains {len(sub_gits)} subgitfolders converted to {gpp}')
				#session.add(newgpp)
				#session.commit()
				#new_gfl = get_folder_list(newgpp, session)
				#scanpath(newgpp, session)
		gppcnt = session.query(GitParentPath).count()
		subfcnt = session.query(GitFolder).filter(GitFolder.is_parent == True).count()
		logger.debug(f'[chk] gppcntt:{gppcnt}  subfcnt:{subfcnt}')
	else:
		logger.info(f'[cgk] no folders to convbert to gitparentpath')
	return len(gflist_to_convert)

if __name__ == '__main__':
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('--addpath', dest='addpath')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('--listpaths', action='store_true', help='list gitparentpaths in db', dest='listpaths')
	myparse.add_argument('--fullscan', action='store_true', default=False, dest='fullscan')
	myparse.add_argument('--scanpath', help='Scan single GitParent, specified by ID. Use listpaths to get IDs', action='store', dest='scanpath')
	myparse.add_argument('--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database')
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
	if args.getdupes and (args.dbmode == 'mysql' or args.dbmode == 'sqlite'):
		# session.query(GitRepo.id, GitRepo.git_url, func.count(GitRepo.git_url).label("count")).group_by(GitRepo.git_url).order_by(func.count(GitRepo.git_url).desc()).limit(10).all()
		dupes = get_dupes(session)
	if args.scanpath:
		gpp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		if gpp:
			existing_entries = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
			main_scanpath(gpp, session)
			entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
			logger.info(f'[scanpath] scanning {gpp.folder} id={gpp.id} existing_entries={existing_entries} after scan={entries_afterscan}')
		else:
			logger.warning(f'Path with id {args.scanpath} not found')
	if args.fullscan:
		scan_results = run_full_scan(args.dbmode)
		logger.info(f'[*] runscan done res={scan_results} ')
	if args.dbinfo and args.dbmode == 'postgresql':
		logger.warning(f'[dbinfo] postgresql dbinfo not implemented')
	if args.dbinfo and (args.dbmode == 'mysql' or args.dbmode == 'sqlite'):
		db_dupe_info(session)
		get_db_info(session)
	if args.addpath:
		try:
			new_gsp = add_path(args.addpath, session)
		except (MissingGitFolderException, OperationalError) as e:
			logger.error(e)
		gspcount = session.query(GitParentPath).count()
		logger.debug(f'[*] new gsp: {new_gsp} gspcount:{gspcount}')
