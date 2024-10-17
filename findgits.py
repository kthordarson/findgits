#!/usr/bin/python3
import argparse
from datetime import datetime
from multiprocessing import cpu_count

from loguru import logger
# from sqlalchemy.exc import (OperationalError)
from sqlalchemy.orm import sessionmaker

from dbstuff import GitRepo, GitFolder
from dbstuff import drop_database, get_engine, db_init, db_dupe_info
from git_tasks import create_git_folder,get_folder_list

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
	myparse.add_argument('-sp','--scanpath', help='Scan single path', action='store', dest='scanpath', type=str, default=None)
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
		pass
	elif args.getdupes:
		if args.dbmode == 'postgresql':
			logger.warning('[dbinfo] postgresql dbinfo not implemented')
		else:
			db_dupe_info(session)
	elif args.scanpath:
		db_gitfolders = session.query(GitFolder).all()
		gitfolders = get_folder_list(args.scanpath)
		new_folders = [k for k in gitfolders['res'] if k not in [x.git_path for x in db_gitfolders]]
		logger.info(f'new_folders: {len(new_folders)} db_gitfolders: {len(db_gitfolders)} gitfolders: {len(gitfolders["res"])}')
		for gf in new_folders:
			logger.info(f'new gf: {gf}')
			try:
				create_git_folder(gf, args, session)
			except Exception as e:
				logger.error(f'create_git_folder: {e} {type(e)} gf: {gf} args = {args}')
				break
	elif args.fullscan:
		pass
	elif args.add_path:
		pass
	else:
		logger.warning(f'missing args? {args}')


if __name__ == '__main__':
	main()
