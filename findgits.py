#!/usr/bin/python3
import traceback
import asyncio
from datetime import datetime
from pathlib import Path
import argparse
from loguru import logger
from sqlalchemy.orm import sessionmaker
from dbstuff import GitRepo, GitFolder, GitStar, GitList
from dbstuff import get_engine, db_init, drop_database, check_git_dates, mark_repo_as_starred
from repotools import create_repo_to_list_mapping, verify_star_list_links, check_update_dupes, insert_update_git_folder, insert_update_starred_repo, populate_repo_data
from gitstars import get_lists, get_git_list_stars, get_git_stars, fetch_starred_repos, get_starred_repos_by_list
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
	try:
		# Always ensure the session is in a valid state
		if not session.is_active:
			logger.warning(f'Session is not active, rolling back for {git_path}')
			session.rollback()

		result = await insert_update_git_folder(git_path, session, args)
		if result:
			git_repo = session.query(GitRepo).filter(GitRepo.id == result.gitrepo_id).first()

			# Check if this repo is in our starred repos
			star_entry = session.query(GitStar).filter(GitStar.gitrepo_id == git_repo.id).first()
			if star_entry:
				# Use the mark_repo_as_starred function
				mark_repo_as_starred(session, git_repo.id)

				# Update GitFolder starred fields if they exist
				if hasattr(result, 'is_starred'):
					result.is_starred = True
				if hasattr(result, 'star_id'):
					result.star_id = star_entry.id
				if hasattr(result, 'list_id') and star_entry.gitlist_id:
					result.list_id = star_entry.gitlist_id
			else:
				git_repo.is_starred = False
				git_repo.starred_at = None
				if hasattr(result, 'is_starred'):
					result.is_starred = False
				if hasattr(result, 'star_id'):
					result.star_id = None
				if hasattr(result, 'list_id'):
					result.list_id = None

			session.commit()
			return result
	except Exception as e:
		logger.error(f'Error processing {git_path}: {e} {type(e)}')
		if session.is_active:
			session.rollback()
		return None

async def process_starred_repo(repo, session, args):
	"""Process a single starred repo asynchronously"""
	try:
		await insert_update_starred_repo(github_repo=repo, session=session, args=args, create_new=True)
	except Exception as e:
		logger.error(f'Error processing {repo}: {e} {type(e)}')

async def link_existing_repos_to_stars(session, args):
	"""Link existing GitRepo entries to their GitStar counterparts and associate with lists"""
	try:
		# Get all starred repos from GitHub
		starred_repos = await get_git_stars(args, session)
		starred_lookup = {repo['full_name']: repo for repo in starred_repos}

		# Get all git lists and their associated repos
		git_lists_data = await get_git_list_stars(session, args)

		# Create a mapping of repo URLs to list names
		repo_to_list_mapping = {}
		for list_name, list_data in git_lists_data.items():
			for href in list_data.get('hrefs', []):
				# Convert href to full_name format
				if href.startswith('/'):
					href = href[1:]  # Remove leading slash
				if 'github.com/' in href:
					full_name = href.split('github.com/')[-1]
				else:
					full_name = href

				# Remove .git suffix if present
				if full_name.endswith('.git'):
					full_name = full_name[:-4]

				repo_to_list_mapping[full_name] = list_name

		# Get all local repos
		local_repos = session.query(GitRepo).all()

		linked_count = 0
		list_linked_count = 0

		for local_repo in local_repos:
			# Check if this local repo is in the starred list
			full_name = local_repo.full_name or f"{local_repo.github_owner}/{local_repo.github_repo_name}"

			if full_name in starred_lookup:
				starred_data = starred_lookup[full_name]

				# Check if GitStar entry already exists
				existing_star = session.query(GitStar).filter(GitStar.gitrepo_id == local_repo.id).first()
				if not existing_star:
					# Create GitStar entry
					git_star = GitStar()
					git_star.gitrepo_id = local_repo.id
					git_star.starred_at = datetime.now()
					git_star.stargazers_count = starred_data.get('stargazers_count')
					git_star.description = starred_data.get('description')
					git_star.full_name = starred_data.get('full_name')
					git_star.html_url = starred_data.get('html_url')

					session.add(git_star)
					session.flush()  # Get the ID
					existing_star = git_star

					# Mark the repo as starred
					local_repo.is_starred = True
					local_repo.starred_at = datetime.now()

					linked_count += 1

				# Now link to appropriate list if found
				if full_name in repo_to_list_mapping:
					list_name = repo_to_list_mapping[full_name]

					# Find the GitList entry
					git_list = session.query(GitList).filter(GitList.list_name == list_name).first()
					if git_list and existing_star.gitlist_id != git_list.id:
						existing_star.gitlist_id = git_list.id
						list_linked_count += 1
						if args.debug:
							logger.info(f"Linked {full_name} to list '{list_name}'")

		session.commit()
		logger.info(f"Linked {linked_count} existing repos to their starred counterparts, Linked {list_linked_count} starred repos to their respective lists")

	except Exception as e:
		logger.error(f"Error linking existing repos to stars: {e}")
		logger.error(f"Traceback: {traceback.format_exc()}")
		session.rollback()

async def populate_git_lists(session, args):
	# fetch lists from GitHub
	list_data = await get_lists(args)
	if args.debug:
		logger.debug(f'populate_git_lists: {len(list_data)} lists fetched from GitHub')
	for entry in list_data:
		# Check if list already exists by name or URL
		db_list = session.query(GitList).filter((GitList.list_name == entry['name']) | (GitList.list_url == entry['list_url'])).first()
		if db_list:
			# Update existing entry
			db_list.list_description = entry.get('description', '')
			db_list.repo_count = entry.get('repo_count', '0')
			db_list.list_url = entry.get('list_url', '')
		else:
			# Create new entry
			db_list = GitList(list_name=entry.get('name', ''), list_description=entry.get('description', ''), list_url=entry.get('list_url', ''),)
			if args.debug:
				logger.debug(f'Adding new GitList: {db_list.list_name} with URL: {db_list.list_url}')
			# Optionally add count if you want to store it
			if hasattr(GitList, 'repo_count'):
				db_list.list_count = entry.get('repo_count', '0')
			session.add(db_list)
	try:
		logger.info("Pre-populating list stars cache...")
		git_lists = await get_git_list_stars(session, args)
		logger.info(f"Pre-populated list stars cache with {len(git_lists)} lists")
	except Exception as e:
		logger.warning(f"Failed to pre-populate list stars cache: {e}")

	session.commit()
	return list_data

def get_args():
	myparse = argparse.ArgumentParser(description="findgits")
	myparse.add_argument('--scanpath','-sp', help='Scan path for git repos', action='store', dest='scanpath', nargs=1)
	# info
	myparse.add_argument('--checkdates', help='checkdates', action='store_true', default=False, dest='checkdates')
	myparse.add_argument('--list-by-group', help='show starred repos grouped by list', action='store_true', default=False, dest='list_by_group')
	myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
	# db
	myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
	myparse.add_argument('--db_file', help='sqlitedb filename', default='gitrepo.db', dest='db_file', action='store', metavar='db_file')
	myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
	# stars/lists
	myparse.add_argument('--gitstars', help='gitstars info', action='store_true', default=False, dest='gitstars')
	myparse.add_argument('--scanstars', help='scam starts from github', action='store_true', dest='scanstars', default=False)
	myparse.add_argument('--create_stars', help='add repos from git stars', action='store_true', default=False, dest='create_stars')
	myparse.add_argument('--populate', help='gitstars populate', action='store_true', default=False, dest='populate')
	myparse.add_argument('--fetch_stars', help='fetch_stars', action='store_true', default=False, dest='fetch_stars')
	# tuning, debug, etc
	myparse.add_argument('--scanpath_threads', '-spt', help='scanpath_threads', action='store', dest='scanpath_threads')
	myparse.add_argument('--max_pages', help='gitstars max_pages', action='store', default=100, dest='max_pages', type=int)
	myparse.add_argument('--global_limit', help='global limit', action='store', default=0, dest='global_limit', type=int)
	myparse.add_argument('--debug', help='debug', action='store_true', default=True, dest='debug')
	myparse.add_argument('--use_cache', help='use_cache', action='store_true', default=True, dest='use_cache')
	myparse.add_argument('--disable_cache', help='disable_cache', action='store_true', default=False, dest='disable_cache')
	myparse.add_argument('--nodl', help='disable all downloads/api call', action='store_true', default=False, dest='nodl')
	args = myparse.parse_args()
	if args.disable_cache:
		args.use_cache = False
		logger.info('Cache disabled')
	return args

def get_session(args):
	engine = get_engine(args)
	s = sessionmaker(bind=engine)
	session = s()
	db_init(engine)
	print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')
	return session, engine

async def main():
	args = get_args()
	session, engine = get_session(args)

	if args.dropdatabase:
		drop_database(engine)
		logger.info('Database dropped')
		session.close()
		return

	if args.checkdates:
		check_git_dates(session)

	if args.dbinfo:
		git_folders = session.query(GitFolder).count()
		git_repos = session.query(GitRepo).count()
		dupes = check_update_dupes(session)
		chk = dbcheck(session)
		print(f'dbcheck: {chk}')
		print(f'Git Folders: {git_folders} Git Repos: {git_repos} Dupes: {dupes['dupe_repos']} ')
		return

	if args.list_by_group:
		grouped_repos = await get_starred_repos_by_list(session, args)
		total_repos = sum(len(repos) for repos in grouped_repos.values())
		print(f"\nFound {len(grouped_repos)} lists with {total_repos} total repositories:\n")
		for list_name, repos in grouped_repos.items():
			print(f"\n{list_name} ({len(repos)} repos):")
			print("-" * (len(list_name) + 10))
			for repo in repos:
				stars = repo.get('stargazers_count', 0)
				# lang = repo.get('language', 'Unknown')
				if repo.get('language'):
					lang = repo.get('language', 'Unknown')[:15].ljust(15)  # Ensure fixed width
				else:
					lang = 'Unknown'.ljust(15)
				if repo.get('description'):
					desc = repo.get('description', 'No description')
				else:
					desc = 'No description'
				if len(desc) > 60:
					desc = desc[:57] + "..."
				print(f"⭐ {stars:7d} | {lang:15} | {repo['full_name']:40} | {desc}")
		return

	if args.gitstars:
		starred_repos = []
		git_repos = session.query(GitRepo).all()
		git_lists = await get_git_list_stars(session, args)
		starred_repos = await get_git_stars(args, session)
		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'Git Lists: {len(git_lists)} git_list_count: {len(git_lists)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')
		return

	if args.fetch_stars:
		fetched_repos = await fetch_starred_repos(args, session)
		print(f'Fetched {len(fetched_repos)} ( {type(fetched_repos)} ) starred repos from GitHub API')
		return

	if args.scanpath:
		scanpath = Path(args.scanpath[0])

		if args.debug:
			logger.debug(f'Scan path: {scanpath}')

		# Fetch starred repos ONCE at the beginning
		starred_repos = await get_git_stars(args, session)

		if args.debug:
			logger.debug(f'get_git_stars done, starred_repos: {len(starred_repos)} starting populate_git_lists')

		list_data = await populate_git_lists(session, args)
		if args.debug:
			logger.debug(f'Populated git lists from GitHub, got {len(list_data)} ... starting populate_repo_data')

		# Pass the already-fetched starred_repos to populate_repo_data instead of letting it fetch again
		stats = await populate_repo_data(session, args, starred_repos=starred_repos)  # Modified

		if args.debug:
			logger.debug(f'populate_repo_data done stats: {stats}')

		# Remove this redundant call since we already have starred_repos
		# fetched_repos = await fetch_starred_repos(args, session)  # REMOVE THIS LINE

		# Use the existing starred_repos data
		print(f'Using {len(starred_repos)} starred repos from GitHub API')

		if args.global_limit > 0:
			logger.warning(f'Global limit set to {args.global_limit}, this will limit the number of repositories processed.')
			git_repos = session.query(GitRepo).limit(args.global_limit).all()
		else:
			git_repos = session.query(GitRepo).all()

		if args.debug:
			logger.debug(f'Git Repos: {len(git_repos)} starting get_git_list_stars')

		git_lists = await get_git_list_stars(session, args)

		urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))
		localrepos = [k.github_repo_name for k in git_repos]
		notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
		foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
		print(f'Git Lists: {len(git_lists)} git_list_count: {len(git_lists)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

		# PRE-FETCH the repo-to-list mapping ONCE
		logger.info("Creating repo-to-list mapping...")
		repo_to_list_mapping = await create_repo_to_list_mapping(session, args)
		logger.info(f"Created mapping for {len(repo_to_list_mapping)} repositories")

		fetched_repos = await fetch_starred_repos(args, session)
		print(f'Fetched {len(fetched_repos)} ( {type(fetched_repos)} ) starred repos from GitHub API urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')
		# Process repos in parallel
		batch_size = 20
		for i in range(0, len(notfoundrepos), batch_size):
			batch = notfoundrepos[i:i+batch_size]
			tasks = []

			for repo in batch:
				if '20142995/pocsuite3' in repo:
					logger.warning(f'problematic repo: {repo}')
				tasks.append(process_starred_repo(repo, session, args))

			await asyncio.gather(*tasks)
			session.commit()
		await link_existing_repos_to_stars(session, args)

		verification_results = await verify_star_list_links(session, args)
		if verification_results:
			print(f"Star-List Link Verification: {verification_results}")

		if scanpath.is_dir():
			# Find git folders
			git_folders = [k for k in scanpath.glob('**/.git') if Path(k).is_dir() and '.cargo' not in str(k) and 'developmenttest' not in str(k)]
			if args.global_limit > 0:
				git_folders = git_folders[:args.global_limit]
				logger.warning(f'Global limit set to {args.global_limit}, processing only first {len(git_folders)} git folders')
			print(f'Scan path: {scanpath} found {len(git_folders)} git folders')
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
			folder.get_folder_stats()
			if args.debug:
				logger.debug(f'Git Folder: {folder.git_path} ID: {folder.id} Scan Count: {folder.scan_count} ')
			# Commit changes in batches to avoid large transactions
			if (i + 1) % batch_size == 0 or i == len(git_folders) - 1:
				session.commit()
				if args.debug:
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
