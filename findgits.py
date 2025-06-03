#!/usr/bin/python3
import glob
from pathlib import Path
import argparse
from multiprocessing import cpu_count

from loguru import logger
# from sqlalchemy.exc import (OperationalError)
from sqlalchemy.orm import sessionmaker
from dbstuff import (GitRepo, GitFolder)
from dbstuff import get_engine, db_init, drop_database, insert_update_git_folder
from utils import check_update_dupes

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
	myparse.add_argument('-sp','--scanpath', help='Scan path for git repos', action='store', dest='scanpath', nargs=1)
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
	elif args.dbinfo:
		git_folders = session.query(GitFolder).count()
		git_repos = session.query(GitRepo).count()
		dupes = check_update_dupes(session)
		print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')
		print(f'Git Folders: {git_folders} Git Repos: {git_repos} Dupes: {dupes['dupe_repos']} ')
	elif args.dropdatabase:
		drop_database(engine)
	elif args.scanpath:
		scanpath = Path(args.scanpath[0])
		if scanpath.is_dir():
			# Find git folders
			git_folders = [k for k in scanpath.glob('**/.git') if Path(k).is_dir()]
			logger.info(f'Scan path: {scanpath} found {len(git_folders)} git folders')
			# Process each git folder
			for git_folder in git_folders:
				# The git folder is the parent directory of .git
				git_path = git_folder.parent
				insert_update_git_folder(git_path, session)

			session.commit()

			print(f'Processed {len(git_folders)} git folders')
		else:
			logger.error(f'Scan path: {scanpath} is not a valid directory')
			return

if __name__ == '__main__':
	main()
