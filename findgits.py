#!/usr/bin/python3
import argparse
import os
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from threading import Thread
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from dbstuff import (GitFolder, GitParentPath, GitRepo)
from dbstuff import drop_database, dupe_view_init,get_engine, db_init
from dbstuff import MissingGitFolderException, MissingConfigException
from git_tasks import run_scanpath_threads, run_full_scan
from git_tasks import (add_path, get_parent_entries, scanpath)

CPU_COUNT = cpu_count()


def import_paths(pathfile, session: sessionmaker):
	# import paths from file
	if not os.path.exists(pathfile):
		logger.error(f'[importpaths] {pathfile} not found')
		return
	with open(pathfile, 'r') as f:
		fdata_ = f.readlines()
	fdata = [k.strip() for k in fdata_]

def dbdump(backupfile, engine):
	# todo finish
	# dump db to file
	pass


if __name__ == '__main__':
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('--addpath', dest='addpath')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('--listpaths', action='store', default='all', help='list paths in db, specify "all" or id', dest='listpaths', metavar='krem')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--fullscan', action='store_true', default=False, dest='fullscan')
	myparse.add_argument('--scanpath', help='run scan on path, specify pathid', action='store', dest='scanpath')
	myparse.add_argument('--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	engine = get_engine(dbtype=args.dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	if args.dropdatabase:
		drop_database(engine)
	if args.getdupes:
		#dupe_view_init(session)
		#sql = text('select * from dupeview order by count desc limit 10;')
		# psql:
		# select * from gitrepo ou where (select count(*) from gitrepo inr where inr.giturl = ou.giturl)>1;
		# select giturl, count(*) from gitrepo group by giturl having count(*)>3;
		# sql = text('select id,gitfolder_id,giturl,git_path, count(*) as count from gitrepo group by giturl having count>1;')
		sql = text('select giturl, count(*) from gitrepo group by giturl having count(*)>1;')
		dupes = None
		dupe_counter = 0
		try:
			dupes = session.execute(sql).all()
		except ProgrammingError as e:
			logger.error(f'[getdupes] {e}')
		if dupes:
			for d in dupes:
				repdupe = session.query(GitRepo).filter(GitRepo.giturl == d.giturl).all()
				dupe_counter += len(repdupe)
				print(f'[d] gitrepo {d.giturl} has {len(repdupe)} dupes found in:')
				for r in repdupe:
					print(f'\t{r.git_path} ')
		print(f'[getdupes] {dupe_counter} dupes found')
	if args.scanpath:
		gsp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		# entries = get_folder_list(gsp)
		entries = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)}')
		scanpath(gsp, args.dbmode)
		entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).all()
		logger.info(f'[scanpath] scanning {gsp.folder} id={gsp.id} existing_entries={len(entries)} after scan={len(entries_afterscan)}')
	if args.scanpath_threads:
		run_scanpath_threads(args.scanpath_threads, session)
	if args.fullscan:
		scan_results = run_full_scan(args.dbmode)
		logger.info(f'[*] runscan done res={scan_results} ')

	if args.dbinfo:
		# show db info
		# show_dbinfo(session)
		if args.listpaths:
			if args.listpaths == 'all':
				git_parent_entries = get_parent_entries(session)
			else:
				git_parent_entries = session.query(GitParentPath).filter(GitParentPath.id == str(args.listpaths)).all()
			for gpe in git_parent_entries:
				fc = session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).count()
				f_size = sum([k.folder_size for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
				f_scantime = sum([k.scan_time for k in session.query(GitFolder).filter(GitFolder.parent_id == gpe.id).all()])
				rc = session.query(GitRepo).filter(GitRepo.parent_id == gpe.id).count()
				print(f'[*] id={gpe.id} path={gpe.folder}\n\tfolders={fc}\n\trepos={rc}\n\tsize={f_size}\n\tscantime={f_scantime}')

	if args.addpath:
		try:
			new_gsp = add_path(args.addpath, session)
		except MissingGitFolderException as e:
			logger.error(e)
		# logger.debug(f'[*] new path: {new_gsp}')
		entries = session.query(GitFolder).filter(GitFolder.parent_id == new_gsp.id).count()
		logger.info(f'[addpath] scanning newgsp {new_gsp.folder} id={new_gsp.id} existing_entries={entries}')
		scanpath(new_gsp, args.dbmode)
		entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == new_gsp.id).count()
		logger.info(f'[addpath] scanning {new_gsp.folder} id={new_gsp.id} existing_entries={entries} after scan={entries_afterscan}')
	# scanpath(new_gsp.id, session)
	if args.importpaths:
		# read paths from text file and import
		import_paths(args.importpaths, session)
