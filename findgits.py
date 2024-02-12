#!/usr/bin/python3
import argparse
from datetime import datetime
from multiprocessing import cpu_count

from loguru import logger
from sqlalchemy.exc import (OperationalError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

from dbstuff import (GitFolder, GitParentPath, GitRepo)
from dbstuff import drop_database, get_engine, db_init, db_dupe_info, get_db_info, check_dupe_status
#from utils import (get_directory_size, get_subdircount, get_subfilecount, format_bytes, check_dupe_status)
from git_tasks import (add_parent_path, scanpath)
from git_tasks import collect_folders, create_git_folders, create_git_repos, update_gitfolder_stats
from git_tasks import (get_git_show)

CPU_COUNT = cpu_count()


def show_dupe_info(dupes, session: Session):
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


def main_scanpath(gpp: GitParentPath, session: sessionmaker) -> None:
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


def dbcheck(session) -> dict:
	"""
	run db checks:
		* todo check for missing folders
		* todo check for missing repos
	"""
	gpp = session.query(GitParentPath).all()
	result = {'ggp_count': len(gpp), }
	return result


def main():
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('-app', '--addppath', dest='add_parent_path', help='add new parent path to db and run scan on it')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('-l', '--listpaths', action='store_true', help='list gitparentpaths in db', dest='listpaths')
	myparse.add_argument('-fs', '--fullscan', action='store_true', default=False, dest='fullscan', help='run full scan on all parent paths in db')
	myparse.add_argument('-sp','--scanpath', help='Scan single GitParent, specified by ID. Use --listpaths to get IDs', action='store', dest='scanpath')
	myparse.add_argument('-spt', '--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('-gd', '--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', required=True, action='store', metavar='dbmode')
	myparse.add_argument('--dbsqlitefile', help='sqlitedb filename', default='gitrepo.db', dest='dbsqlitefile', action='store', metavar='dbsqlitefile')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--dbcheck', help='run checks', action='store_true', default=False, dest='dbcheck')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	if args.dbcheck:
		res = dbcheck(session)
		print(f'dbcheck res: {res}')
	elif args.dropdatabase:
		drop_database(engine)
	elif args.listpaths:
		gpp = session.query(GitParentPath).all()
		for gp in gpp:
			print(f'[listpaths] id={gp.id} path={gp.folder} last_scan={gp.last_scan} scan_time={gp.scan_time}')
	elif args.getdupes:
		if args.dbmode == 'postgresql':
			logger.warning(f'[dbinfo] postgresql dbinfo not implemented')
		else:
			db_dupe_info(session)
		# dupes = get_dupes(session)
		# session.query(GitRepo.id, GitRepo.git_url, func.count(GitRepo.git_url).label("count")).group_by(GitRepo.git_url).order_by(func.count(GitRepo.git_url).desc()).limit(10).all()
	elif args.scanpath:
		mainres = None
		gpp = session.query(GitParentPath).filter(GitParentPath.id == args.scanpath).first()
		if gpp:
			existing_entries = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
			logger.info(f'existing_entries:{existing_entries}')
			try:
				# mainres = main_scanpath(gpp, session)
				scanpath(gpp, session)
			except OperationalError as e:
				logger.error(f'[scanpath] error: {e}')
		else:
			logger.warning(f'Path with id {args.scanpath} not found')
	elif args.fullscan:
		t0 = datetime.now()

		# collect all folders from all gitparentpaths
		scan_result = collect_folders(args)
		for gp in session.query(GitParentPath).all():
			logger.info(f'[fullscan] id={gp.id} path={gp.folder} last_scan={gp.last_scan} scan_time={gp.scan_time} res={len(scan_result[gp.id])}')
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] collect done t:{t1} scan_result:{len(scan_result)} starting create_git_folders')

		# create gitfolders in db
		git_folders_result = create_git_folders(args, scan_result)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] create_git_folders done t:{t1} git_folders_result:{git_folders_result} starting create_git_repos')

		# create gitrepos in db
		git_repo_result = create_git_repos(args)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] create_git_repos done t:{t1} git_repo_result:{git_repo_result} starting update_gitfolder_stats')

		# update gitfolder stats
		folder_results = update_gitfolder_stats(args)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*]  update_gitfolder_stats done t:{t1} folder_results:{len(folder_results)}')

		check_dupe_status(session)
		t1 = (datetime.now() - t0).total_seconds()
		logger.info(f'[*] check_dupe_status done t:{t1}')
	elif args.dbinfo:
		if args.dbmode == 'postgresql':
			logger.warning(f'[dbinfo] postgresql dbinfo not implemented')
		else:
			get_db_info(session)
	elif args.add_parent_path:
		t0 = datetime.now()
		new_gpp = add_parent_path(args.add_parent_path, session)
		if new_gpp:
			session.add(new_gpp)
			session.commit()
			logger.debug(f'new_gpp={new_gpp}')
			scanpath(new_gpp, session)
			check_dupe_status(session)
		else:
			logger.warning(f'{args.add_parent_path} already in db')
	else:
		logger.warning(f'missing args? {args}')


if __name__ == '__main__':
	main()

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
