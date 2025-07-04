#!/usr/bin/python3
import asyncio
from pathlib import Path
import argparse
from loguru import logger
from sqlalchemy.orm import sessionmaker
from dbstuff import GitRepo, GitFolder
from dbstuff import get_engine, db_init, drop_database
from repotools import check_update_dupes, insert_update_git_folder, insert_update_starred_repo, populate_repo_data
from gitstars import get_git_list_stars, get_git_stars, fetch_starred_repos
from utils import flatten


def dbcheck(session) -> dict:
	"""
	run db checks:
		* todo check for missing folders
		* todo check for missing repos
	"""
	repos = session.query(GitRepo).all()
	folders = session.query(GitFolder).all()
	result = {'repo_count': len(repos), 'folder_count': len(folders),}
	return result

async def process_git_folder(git_path, session, args):
	"""Process a single git folder asynchronously"""
	# logger.info(f'Processing {git_path}')
	try:
		# Always ensure the session is in a valid state
		if not session.is_active:
			session.rollback()
		result = await insert_update_git_folder(git_path, session, args)
		return result
	except Exception as e:
		logger.error(f'Error processing {git_path}: {e} {type(e)}')
		if session.is_active:
			session.rollback()
		return None

async def process_starred_repo(repo, session, args):
	"""Process a single starred repo asynchronously"""
	try:
		await insert_update_starred_repo(repo, session, args)
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
	myparse.add_argument('--fetch_stars', help='fetch_stars', action='store_true', default=False, dest='fetch_stars')
	myparse.add_argument('--max_pages', help='gitstars max_pages', action='store', default=100, dest='max_pages', type=int)
	myparse.add_argument('--debug', help='debug', action='store_true', default=True, dest='debug')
	myparse.add_argument('--use_cache', help='use_cache', action='store_true', default=True, dest='use_cache')
	myparse.add_argument('--nodl', help='disable all downloads/api call', action='store_true', default=False, dest='nodl')
	# myparse.add_argument('--rungui', action='store_true', default=False, dest='rungui')
	args = myparse.parse_args()
	return args

async def main():
	args = get_args()
	engine = get_engine(args)
	s = sessionmaker(bind=engine)
	session = s()
	db_init(engine)
	print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')

	if args.dropdatabase:
		drop_database(engine)
		logger.info('Database dropped')
		session.close()
		return

	if args.dbcheck:
		res = dbcheck(session)
		print(f'dbcheck res: {res}')
		return

	if args.dbinfo:
		git_folders = session.query(GitFolder).count()
		git_repos = session.query(GitRepo).count()
		dupes = check_update_dupes(session)
		chk = dbcheck(session)
		print(f'dbcheck: {chk}')
		print(f'Git Folders: {git_folders} Git Repos: {git_repos} Dupes: {dupes['dupe_repos']} ')
		return

	if args.gitstars:
		starred_repos = []
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(session, args)
		starred_repos = await get_git_stars(args, session)  # Updated call
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'Git Lists: {len(git_lists)} git_list_count: {len(git_lists)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')
		return

	if args.fetch_stars:
		fetched_repos = await fetch_starred_repos(args, session)  # Updated call
		print(f'Fetched {len(fetched_repos)} ( {type(fetched_repos)} ) starred repos from GitHub API')
		return

	if args.scanpath:
		scanpath = Path(args.scanpath[0])
		if scanpath.is_dir():
			# Find git folders
			git_folders = [k for k in scanpath.glob('**/.git') if Path(k).is_dir() and '.cargo' not in str(k) and 'developmenttest' not in str(k)]
			logger.info(f'Scan path: {scanpath} found {len(git_folders)} git folders')
			tasks = set()
			for git_folder in git_folders:
				git_path = git_folder.parent
				tasks.add(process_git_folder(git_path, session, args))
			await asyncio.gather(*tasks)
			session.commit()
			print(f'Processed {len(git_folders)} git folders')
		else:
			logger.error(f'Scan path: {scanpath} is not a valid directory')
		return

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
		return

	if args.scanstars:
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(session, args)
		# git_list_count = sum([len(git_lists[k]['hrefs']) for k in git_lists])
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')
		return

	if args.create_stars:
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(session, args)
		# git_list_count = sum([len(git_lists[k]['hrefs']) for k in git_lists])
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
				tasks.append(process_starred_repo(repo, session, args))

			await asyncio.gather(*tasks)
			session.commit()
	return

if __name__ == '__main__':
	asyncio.run(main())
