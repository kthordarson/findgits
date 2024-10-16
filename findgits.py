#!/usr/bin/python3
import argparse
from datetime import datetime
from multiprocessing import cpu_count

from loguru import logger
# from sqlalchemy.exc import (OperationalError)
from sqlalchemy.orm import sessionmaker

from dbstuff import (GitRepo, SearchPath)  #
from dbstuff import drop_database, get_engine, db_init, db_dupe_info, get_db_info, check_dupe_status
# from utils import (get_directory_size, get_subdircount, get_subfilecount, format_bytes, check_dupe_status)
from git_tasks import (add_path)  # , scanpath
from git_tasks import collect_folders, create_git_folders

CPU_COUNT = cpu_count()


def dbcheck(session) -> dict:
	"""
	run db checks:
		* todo check for missing folders
		* todo check for missing repos
	"""
	gpp = session.query(GitRepo).all()
	result = {'ggp_count': len(gpp), }
	return result

def get_args():
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('-ap', '--addpath', dest='add_path', action='store', help='add new search path to db')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('-l', '--listpaths', action='store_true', help='list paths in db', dest='listpaths')
	myparse.add_argument('-fs', '--fullscan', action='store_true', default=False, dest='fullscan', help='run full scan on all search paths in db')
	myparse.add_argument('-sp','--scanpath', help='Scan single path, specified by ID. Use --listpaths to get IDs', action='store', dest='scanpath')
	myparse.add_argument('-spt', '--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('-gd', '--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
	myparse.add_argument('--dbsqlitefile', help='sqlitedb filename', default='gitrepo.db', dest='dbsqlitefile', action='store', metavar='dbsqlitefile')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--dbcheck', help='run checks', action='store_true', default=False, dest='dbcheck')
	myparse.add_argument('--debug', help='debug', action='store_true', default=True, dest='debug')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	return args

def main():
	args = get_args()
	engine = get_engine(args)
	s = sessionmaker(bind=engine)
	session = s()
	db_init(engine)
	if args.dbcheck:
		res = dbcheck(session)
		print(f'dbcheck res: {res}')
	elif args.dropdatabase:
		drop_database(engine)
	elif args.listpaths:
		sp = session.query(SearchPath).all()
		for s in sp:
			print(f'{s.id} {s.folder}')
	elif args.getdupes:
		if args.dbmode == 'postgresql':
			logger.warning('[dbinfo] postgresql dbinfo not implemented')
		else:
			db_dupe_info(session)
	elif args.scanpath:
		pass
		# todo
	elif args.fullscan:
		t0 = datetime.now()

		# collect all folders from all paths
		scanresults = collect_folders(args)
		if len(scanresults) > 0:
			for scan_result in scanresults:
				# create gitfolders in db
				t1 = (datetime.now() - t0).total_seconds()
				logger.info(f'[*] collect done t:{t1} scan_result:{len(scan_result)} starting create_git_folders')
				# create gitfolders in db
				git_folders_result = create_git_folders(args, scan_result)
				t1 = (datetime.now() - t0).total_seconds()
				logger.info(f'[*] create_git_folders done t:{t1} git_folders_result:{git_folders_result}')

				# create gitrepos in db
				# git_repo_result = create_git_repos(args)
				# update gitfolder stats
				# folder_results = update_gitfolder_stats(args)
				t1 = (datetime.now() - t0).total_seconds()
				# logger.info(f'[*]  update_gitfolder_stats done t:{t1} folder_results:{len(folder_results)}')

				# check_dupe_status(session)
				t1 = (datetime.now() - t0).total_seconds()
				logger.info(f'[*] check_dupe_status done t:{t1}')
	elif args.dbinfo:
		if args.dbmode == 'postgresql':
			logger.warning('[dbinfo] postgresql dbinfo not implemented')
		else:
			get_db_info(session)
	elif args.add_path:
		t0 = datetime.now()
		gpid = add_path(args)
		if gpid:
			gp = session.query(SearchPath).filter(SearchPath.id == gpid).first()
			logger.info(f'[*] add_path done gpid:{gpid} gp: {gp}')
			gitfolders = gp.get_git_folders()
			logger.info(f'[*] get_git_folders done {len(gitfolders)}')
			git_folders_result = create_git_folders(args, gitfolders)
			logger.info(f'[*] create_git_folders done git_folders_result:{git_folders_result}')
	else:
		logger.warning(f'missing args? {args}')


if __name__ == '__main__':
	main()
