#!/usr/bin/python3
import aiohttp
import asyncio
import aiofiles
import sys
from pathlib import Path
import argparse
from multiprocessing import cpu_count
from loguru import logger
from sqlalchemy.orm import sessionmaker
from dbstuff import GitRepo, GitFolder
from dbstuff import get_engine, db_init, drop_database
from dbstuff import check_update_dupes, insert_update_git_folder, insert_update_starred_repo, populate_starred_repos, fetch_missing_repo_data, populate_repo_data
from gitstars import get_git_list_stars, get_git_stars
from utils import flatten
from datetime import datetime
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

async def process_git_folder(git_path, session):
	"""Process a single git folder asynchronously"""
	logger.info(f'Processing {git_path}')
	try:
		await insert_update_git_folder(git_path, session)
	except Exception as e:
		logger.error(f'Error processing {git_path}: {e} {type(e)}')
		session.rollback()

async def process_starred_repo(repo, session):
	"""Process a single starred repo asynchronously"""
	try:
		await insert_update_starred_repo(repo, session)
	except Exception as e:
		logger.error(f'Error processing {repo}: {e} {type(e)}')

def get_args():
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('-ap', '--addpath', dest='add_path', action='store', help='add new search path to db')
	myparse.add_argument('--importpaths', dest='importpaths')
	myparse.add_argument('-l', '--listpaths', action='store_true', help='list paths in db', dest='listpaths')
	myparse.add_argument('-fs', '--fullscan', action='store_true', default=False, dest='fullscan', help='run full scan on all search paths in db')
	myparse.add_argument('-sp','--scanpath', help='Scan path for git repos', action='store', dest='scanpath', nargs=1)
	myparse.add_argument('--scanstars', help='include starred from github', action='store_true', dest='scanstars', default=False)
	myparse.add_argument('-spt', '--scanpath_threads', help='run scan on path, specify pathid', action='store', dest='scanpath_threads')
	myparse.add_argument('-gd', '--getdupes', help='show dupe repos', action='store_true', default=False, dest='getdupes')
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
	myparse.add_argument('--dbsqlitefile', help='sqlitedb filename', default='gitrepo.db', dest='dbsqlitefile', action='store', metavar='dbsqlitefile')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	myparse.add_argument('--dbcheck', help='run checks', action='store_true', default=False, dest='dbcheck')
	myparse.add_argument('--gitstars', help='gitstars info', action='store_true', default=False, dest='gitstars')
	myparse.add_argument('--create_stars', help='add repos from git stars', action='store_true', default=False, dest='create_stars')
	myparse.add_argument('--populate', help='gitstars populate', action='store_true', default=False, dest='populate')
	myparse.add_argument('--max_pages', help='gitstars max_pages', action='store', default=10, dest='max_pages')
	myparse.add_argument('--use_cache', help='use_cache', action='store_true', default=True, dest='use_cache')
	myparse.add_argument('--no_cache', help='no_cache', action='store_true', default=False, dest='no_cache')
	myparse.add_argument('--debug', help='debug', action='store_true', default=True, dest='debug')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	return args

async def main():
	args = get_args()
	engine = get_engine(args)
	s = sessionmaker(bind=engine)
	session = s()
	db_init(engine)
	if args.no_cache:
		logger.info('No cache will be used')
		args.use_cache = False
	if args.dbcheck:
		res = dbcheck(session)
		print(f'dbcheck res: {res}')
	if args.dbinfo:
		git_folders = session.query(GitFolder).count()
		git_repos = session.query(GitRepo).count()
		dupes = check_update_dupes(session)
		print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')
		print(f'Git Folders: {git_folders} Git Repos: {git_repos} Dupes: {dupes['dupe_repos']} ')

	if args.gitstars:
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(use_cache=args.use_cache)
		starred_repos = await get_git_stars()
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'Git Lists: {len(git_lists)} git_list_count: {len(git_lists)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

	if args.dropdatabase:
		drop_database(engine)

	if args.scanpath:
		scanpath = Path(args.scanpath[0])
		if scanpath.is_dir():
			# Find git folders
			git_folders = [k for k in scanpath.glob('**/.git') if Path(k).is_dir()]
			logger.info(f'Scan path: {scanpath} found {len(git_folders)} git folders')

			# Process git folders in batches
			batch_size = 10
			for i in range(0, len(git_folders), batch_size):
				batch = git_folders[i:i+batch_size]
				tasks = []

				for git_folder in batch:
					git_path = git_folder.parent
					tasks.append(process_git_folder(git_path, session))

				await asyncio.gather(*tasks)
				session.commit()

			print(f'Processed {len(git_folders)} git folders')
		else:
			logger.error(f'Scan path: {scanpath} is not a valid directory')

	if args.populate:
		stats = await populate_repo_data(session, args)
		print(f"GitHub Stars Processing Stats:{stats}")
		git_folders = session.query(GitFolder).all()
		print(f'Git Folders: {len(git_folders)}')

		# For larger datasets, consider processing in batches
		batch_size = 50
		for i, folder in enumerate(git_folders):
			folder.get_folder_time()
			folder.get_folder_stats()
			if args.debug:
				# Print folder stats
				logger.debug(f'Git Folder: {folder.git_path} ID: {folder.id} Scan Count: {folder.scan_count} ')

			# Commit changes in batches to avoid large transactions
			if (i + 1) % batch_size == 0 or i == len(git_folders) - 1:
				session.commit()
				logger.info(f"Committed batch of folder updates ({i+1}/{len(git_folders)})")

		# Make sure all changes are committed
		session.commit()
		logger.info(f"Updated {len(git_folders)} folders in database")

	if args.scanstars:
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(use_cache=args.use_cache)
		git_list_count = sum([len(git_lists[k]['hrefs']) for k in git_lists])
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

	if args.create_stars:
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(use_cache=args.use_cache)
		git_list_count = sum([len(git_lists[k]['hrefs']) for k in git_lists])
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

		# Process repos in parallel
		batch_size = 20
		for i in range(0, len(notfoundrepos), batch_size):
			batch = notfoundrepos[i:i+batch_size]
			tasks = []

			for repo in batch:
				tasks.append(process_starred_repo(repo, session))

			await asyncio.gather(*tasks)
			session.commit()

if __name__ == '__main__':
	asyncio.run(main())
