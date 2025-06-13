from __future__ import annotations
import time
import shutil
import aiohttp
import asyncio
import aiofiles
import os
import json
from datetime import datetime
from pathlib import Path
from loguru import logger
from dbstuff import GitRepo, GitFolder, get_dupes
from utils import valid_git_folder, get_remote, ensure_datetime
from gitstars import get_git_stars, update_repo_cache, get_auth_param, CACHE_DIR

async def insert_update_git_folder(git_folder_path, session):
	"""
	Insert a new GitFolder or update an existing one in the database
	"""
	try:
		git_folder_path = str(Path(git_folder_path))

		# Ensure this is a valid git folder before proceeding
		if not valid_git_folder(os.path.join(git_folder_path, '.git')):
			logger.warning(f'{git_folder_path} is not a valid git folder')
			return None

		# Get remote URL for this repository
		if git_folder_path.endswith('/'):
			git_folder_path = git_folder_path[:-1]

		# Get remote URL and normalize it
		remote_url = get_remote(git_folder_path).lower().strip()
		if not remote_url or remote_url == '[no remote]':
			logger.warning(f'Could not determine remote URL for {git_folder_path}')
			remote_url = f"file://{git_folder_path}"  # Use local path as fallback URL

		# TRANSACTION START
		try:
			# First check if the repo already exists by URL
			git_repo = session.query(GitRepo).filter(
				GitRepo.git_url.ilike(remote_url)
			).first()

			# If not found by exact URL, try alternative lookups
			if not git_repo:
				# Try without .git suffix
				if remote_url.endswith('.git'):
					git_repo = session.query(GitRepo).filter(
						GitRepo.git_url.ilike(remote_url[:-4])
					).first()
				# Try with .git suffix
				else:
					git_repo = session.query(GitRepo).filter(
						GitRepo.git_url.ilike(remote_url + '.git')
					).first()

			# Extract repo name from URL for name-based lookup if needed
			repo_name = os.path.basename(git_folder_path)
			if '/' in remote_url:
				parts = remote_url.split('/')
				if len(parts) > 1:
					repo_name = parts[-1]
					if repo_name.endswith('.git'):
						repo_name = repo_name[:-4]

			# Try lookup by name if URL lookup failed
			if not git_repo:
				git_repo = session.query(GitRepo).filter(
					GitRepo.github_repo_name == repo_name
				).first()

			# Check if folder already exists in database
			git_folder = session.query(GitFolder).filter(
				GitFolder.git_path == git_folder_path
			).first()

			# If no repo exists, create a new one with safeguards
			if not git_repo:
				logger.debug(f'Creating new GitRepo for {remote_url}')
				# Double check once more with a broader query
				git_repo = session.query(GitRepo).filter((GitRepo.git_url.ilike(f"%{repo_name}%")) | (GitRepo.github_repo_name == repo_name)).first()

				if not git_repo:
					git_repo = GitRepo(remote_url, git_folder_path)
					git_repo.first_scan = datetime.now()
					git_repo.last_scan = datetime.now()
					git_repo.scan_count = 1
					session.add(git_repo)
					session.flush()  # Get the ID without committing
					logger.debug(f'Created new GitRepo: {git_repo}')
			else:
				# Update existing repo
				logger.debug(f'Found existing GitRepo: {git_repo}')
				git_repo.last_scan = datetime.now()
				git_repo.scan_count += 1

			# Now handle the GitFolder entry
			if git_folder:
				# Update existing folder
				git_folder.gitrepo_id = git_repo.id
				git_folder.scan_count += 1
				git_folder.last_scan = datetime.now()
			else:
				# Create new folder
				git_folder = GitFolder(git_folder_path, git_repo.id)
				session.add(git_folder)

			# Commit the transaction
			session.commit()
			return git_folder

		except Exception as e:
			logger.error(f'Database error: {e}')
			if session.is_active:
				session.rollback()
			return None

	except Exception as e:
		logger.error(f'Error processing git folder {git_folder_path}: {e}')
		if session.is_active:
			session.rollback()
		return None

async def insert_update_git_folder_v1(git_folder_path, session):
	"""
	Insert a new GitFolder or update an existing one in the database

	Parameters:
		git_folder_path (str): Path to the git folder (containing .git directory)
		session: SQLAlchemy session

	Returns:
		GitFolder: The inserted or updated GitFolder object
	"""
	try:
		git_folder_path = str(Path(git_folder_path))

		# Ensure this is a valid git folder before proceeding
		if not valid_git_folder(os.path.join(git_folder_path, '.git')):
			logger.warning(f'{git_folder_path} is not a valid git folder')
			return None

		# Get remote URL for this repository
		if git_folder_path.endswith('/'):
			git_folder_path = git_folder_path[:-1]

		# Get remote URL and normalize it
		remote_url = get_remote(git_folder_path).lower().strip()
		if not remote_url or remote_url == '[no remote]':
			logger.warning(f'Could not determine remote URL for {git_folder_path}')
			remote_url = f"file://{git_folder_path}"  # Use local path as fallback URL

		# Normalize the URL for consistent comparison
		normalized_url = remote_url.rstrip('/')
		if normalized_url.endswith('.git'):
			normalized_url_no_git = normalized_url[:-4]
		else:
			normalized_url_no_git = normalized_url

		# TRANSACTION START - use a nested transaction scope
		try:
			# First check if folder already exists in database
			git_folder = session.query(GitFolder).filter(GitFolder.git_path == git_folder_path).first()

			# Check if a repo with this remote URL exists, using multiple variations for matching
			git_repo = None

			# Try direct match first
			git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()

			# Try without .git suffix if it exists
			if not git_repo and normalized_url != normalized_url_no_git:
				git_repo = session.query(GitRepo).filter(GitRepo.git_url == normalized_url_no_git).first()

			# Try with .git suffix if it doesn't exist
			if not git_repo and not normalized_url.endswith('.git'):
				git_repo = session.query(GitRepo).filter(GitRepo.git_url == normalized_url + '.git').first()

			# Try looking up by the repository name as a fallback
			if not git_repo:
				# Extract repo name from URL
				repo_name = os.path.basename(git_folder_path)
				# Try to match by repo name as a fallback
				if '/' in remote_url:
					parts = remote_url.split('/')
					if len(parts) > 1:
						repo_name = parts[-1]
						if repo_name.endswith('.git'):
							repo_name = repo_name[:-4]

				# Try to find by github_repo_name
				git_repo = session.query(GitRepo).filter(
					GitRepo.github_repo_name == repo_name
				).first()

			# If no repo exists, create a new one
			if not git_repo:
				logger.debug(f'Creating new GitRepo for {remote_url}')
				git_repo = GitRepo(remote_url, git_folder_path)

				# Extract repository name from URL for better metadata
				if '/' in remote_url:
					parts = remote_url.split('/')
					if len(parts) > 1:
						git_repo.github_repo_name = parts[-1]
						if git_repo.github_repo_name.endswith('.git'):
							git_repo.github_repo_name = git_repo.github_repo_name[:-4]

						if len(parts) > 2:
							git_repo.github_owner = parts[-2]

				# Set all datetime fields properly
				git_repo.first_scan = datetime.now()
				git_repo.last_scan = datetime.now()

				# Get config path for stats
				git_config_path = os.path.join(git_folder_path, '.git', 'config')
				if os.path.exists(git_config_path):
					stat = os.stat(git_config_path)
					git_repo.config_ctime = ensure_datetime(datetime.fromtimestamp(stat.st_ctime))
					git_repo.config_atime = ensure_datetime(datetime.fromtimestamp(stat.st_atime))
					git_repo.config_mtime = ensure_datetime(datetime.fromtimestamp(stat.st_mtime))

				# Try to get metadata from GitHub, but don't fail if not available
				try:
					metadata = await fetch_metadata(git_repo)
					if metadata:
						git_repo = populate_from_metadata(git_repo, metadata)
				except Exception as e:
					logger.warning(f"Error fetching metadata for {git_repo.github_repo_name}: {e}")

				git_repo.scan_count = 1
				session.add(git_repo)

				# Flush to get the ID without committing
				session.flush()
				logger.debug(f'Created new GitRepo: {git_repo}')
			else:
				# Update existing repo
				logger.debug(f'Found existing GitRepo: {git_repo}')
				git_repo.last_scan = datetime.now()
				git_repo.scan_count += 1

			# Now handle the GitFolder entry
			if git_folder:
				# Update existing folder
				git_folder.scan_count += 1
				git_folder.last_scan = datetime.now()
				git_folder.gitrepo_id = git_repo.id

				# Ensure all datetime fields are valid
				try:
					git_folder.get_folder_time()
					git_folder.get_folder_stats()
				except Exception as e:
					logger.warning(f"Error updating folder stats: {e}")

				logger.debug(f'Updated GitFolder: {git_folder}')
			else:
				# Create new folder
				git_folder = GitFolder(git_folder_path, git_repo.id)
				git_folder.scan_count = 1
				session.add(git_folder)
				logger.debug(f'Created new GitFolder: {git_folder}')

			# Commit the transaction
			session.commit()
			return git_folder

		except Exception as e:
			# Roll back on any error
			logger.error(f'Error in database transaction: {e}')
			if session.is_active:
				session.rollback()
			return None

	except Exception as e:
		# Catch any other errors
		logger.error(f'Error processing git folder {git_folder_path}: {e}')
		if session.is_active:
			session.rollback()
		return None

async def insert_update_starred_repo(github_repo, session, create_new=False):
	"""
	Insert a new GitRepo or update an existing one in the database

	Parameters:
		github_repo: repository object from GitHub API or owner/repo string
		session: SQLAlchemy session
		create_new: bool - whether to create a new repo if it doesn't exist
	"""
	git_folder_path = '[notcloned]'

	# Check if github_repo is a string (URL or owner/repo) or a dict
	if isinstance(github_repo, dict):
		# Already have full repo data
		repo_data = github_repo
		remote_url = repo_data.get('html_url') or f"https://github.com/{repo_data.get('full_name')}"
	else:
		# Normalize the path by removing leading/trailing slashes
		clean_path = github_repo.strip('/')

		# It's a string, construct proper GitHub URL
		if 'github.com' in clean_path:
			# It's already a URL
			if clean_path.startswith('http'):
				remote_url = clean_path
			else:
				remote_url = f'https://{clean_path}'
		else:
			# It's just an owner/repo path
			remote_url = f'https://github.com/{clean_path}'

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()

	# Get full repository data from GitHub API
	repo_data = await update_repo_cache(clean_path)

	if not git_repo:
		# Create new repository with all available data
		if create_new:
			git_repo = GitRepo(remote_url, git_folder_path, repo_data)
			git_repo.scan_count = 1
			session.add(git_repo)
			logger.debug(f'Created new GitRepo with full data: {git_repo} remote_url: {remote_url}')
	else:
		logger.info(f'update GitRepo: {git_repo} remote_url: {remote_url}')
		# Update existing repository if we have repo_data
		if repo_data:
			git_repo.last_scan = datetime.now()
			git_repo.scan_count += 1

			# Update with API data
			if repo_data.get('description'):
				git_repo.description = repo_data.get('description')
			if repo_data.get('stargazers_count'):
				git_repo.stargazers_count = repo_data.get('stargazers_count')
			if repo_data.get('language'):
				git_repo.language = repo_data.get('language')

			# Add other fields you want to update
			logger.debug(f'Updated existing GitRepo with API data: {git_repo.github_repo_name}')
	if create_new:
		# Save changes
		session.commit()
	return git_repo

def check_update_dupes(session) -> dict:
	"""
	Check for duplicate GitRepo entries (same git_url) and update their dupe_flag and dupe_count.
	A duplicate repository is one with the same git_url in multiple locations.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary of results containing:
			- total_repos: Total number of repositories
			- unique_repos: Number of unique repositories
			- dupe_repos: Number of repositories that are duplicates
			- dupes_updated: Number of repositories updated
	"""

	# Get all repositories
	all_repos = session.query(GitRepo).all()
	total_repos = len(all_repos)

	# Reset duplicate flags on all repos
	for repo in all_repos:
		repo.dupe_flag = False
		repo.dupe_count = 0

	# Get list of duplicates (repos with same git_url)
	dupes = get_dupes(session)
	dupe_urls = set()
	dupes_updated = 0

	# Process each duplicate group
	for dupe in dupes:
		dupe_id = dupe.id
		dupe_url = dupe.git_url
		dupe_count = dupe.count
		dupe_urls.add(dupe_url)

		# Find all repos with this URL
		same_url_repos = session.query(GitRepo).filter(GitRepo.git_url == dupe_url).all()

		# Update their dupe flags
		for repo in same_url_repos:
			repo.dupe_flag = True
			repo.dupe_count = dupe_count
			dupes_updated += 1

	# Commit the changes
	session.commit()

	# Prepare result summary
	result = {
		'total_repos': total_repos,
		'unique_repos': total_repos - len(dupes),
		'dupe_repos': len(dupe_urls),
		'dupes_updated': dupes_updated
	}

	# logger.info(f"Found {result['dupe_repos']} duplicate repo URLs among {total_repos} total repos")
	return result

async def populate_starred_repos(session, max_pages=90, use_cache=True):
	"""
	Update existing GitRepo entries in database with detailed information from GitHub starred repositories.

	Parameters:
		session: SQLAlchemy session
		max_pages: Maximum number of pages to fetch (0 for all)
		use_cache: Whether to use cached data if available

	Returns:
		dict: Summary statistics about the operation
	"""
	from datetime import datetime
	import os

	# Fetch starred repositories from GitHub API
	logger.info(f"Fetching starred repositories (max_pages={max_pages}, use_cache={use_cache})")
	starred_repos = await get_git_stars(max_pages=max_pages, use_cache=use_cache)

	stats = {
		"total_db_repos": 0,
		"total_starred_repos": len(starred_repos),
		"matched": 0,
		"updated": 0,
		"not_found": 0,
		"errors": 0
	}

	# Create lookup dictionaries for faster matching
	github_repos_by_url = {}
	github_repos_by_name = {}

	for repo_data in starred_repos:
		# Index by various URLs
		if repo_data.get('clone_url'):
			github_repos_by_url[repo_data['clone_url']] = repo_data
		if repo_data.get('html_url'):
			github_repos_by_url[repo_data['html_url']] = repo_data
		if repo_data.get('ssh_url'):
			github_repos_by_url[repo_data['ssh_url']] = repo_data
		if repo_data.get('git_url'):
			github_repos_by_url[repo_data['git_url']] = repo_data

		# Index by name
		if repo_data.get('full_name'):
			github_repos_by_name[repo_data['full_name'].lower()] = repo_data
		if repo_data.get('name'):
			github_repos_by_name[repo_data['name'].lower()] = repo_data

	# Process all repositories in the database
	try:
		db_repos = session.query(GitRepo).all()
		stats["total_db_repos"] = len(db_repos)
		logger.info(f"Processing {stats['total_db_repos']} database repositories github_repos_by_url:{len(github_repos_by_url)}")

		for db_repo in db_repos:
			try:
				# Try to find matching GitHub data
				repo_data = None

				# Try matching by URL first
				if db_repo.git_url and db_repo.git_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.git_url} {stats["matched"]}')
				if db_repo.git_url and db_repo.git_url.replace('.git','') in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url.replace('.git','')]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.git_url.replace('.git','')} {stats["matched"]}')
				elif db_repo.clone_url and db_repo.clone_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.clone_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.clone_url} {stats["matched"]}')
				elif db_repo.html_url and db_repo.html_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.html_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.html_url} {stats["matched"]}')
				elif db_repo.ssh_url and db_repo.ssh_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.ssh_url]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.ssh_url} {stats["matched"]}')

				# Then try by name
				elif db_repo.full_name and db_repo.full_name.lower() in github_repos_by_name:
					repo_data = github_repos_by_name[db_repo.full_name.lower()]
					stats["matched"] += 1
					# logger.debug(f'Matched: {db_repo.full_name.lower()} {stats["matched"]}')
				elif db_repo.github_repo_name:
					# logger.debug(f"Trying to match by name: {db_repo.github_repo_name} {stats['matched']}")
					# Try owner/name format
					full_name = f"{db_repo.github_owner}/{db_repo.github_repo_name}".lower()
					if full_name in github_repos_by_name:
						repo_data = github_repos_by_name[full_name]
						stats["matched"] += 1
						# logger.debug(f"match by name: {db_repo.github_repo_name} {len(repo_data)} {stats['matched']}")
					# Try just the name
					elif db_repo.github_repo_name.lower() in github_repos_by_name:
						repo_data = github_repos_by_name[db_repo.github_repo_name.lower()]
						stats["matched"] += 1
						# logger.debug(f"match by name: {db_repo.github_repo_name} {len(repo_data)} {stats['matched']}")

				# If we found matching data, update the database entry
				if repo_data:
					# Update the entry with all GitHub data
					db_repo.last_scan = datetime.now()
					db_repo.scan_count += 1

					# Update with all API data
					db_repo.node_id = repo_data.get('node_id')
					db_repo.full_name = repo_data.get('full_name')
					db_repo.private = repo_data.get('private')
					db_repo.html_url = repo_data.get('html_url')
					db_repo.description = repo_data.get('description')
					db_repo.fork = repo_data.get('fork')
					db_repo.clone_url = repo_data.get('clone_url')
					db_repo.ssh_url = repo_data.get('ssh_url')
					db_repo.git_url_api = repo_data.get('git_url')
					db_repo.svn_url = repo_data.get('svn_url')
					db_repo.homepage = repo_data.get('homepage')

					# Statistics
					db_repo.size = repo_data.get('size')
					db_repo.stargazers_count = repo_data.get('stargazers_count')
					db_repo.watchers_count = repo_data.get('watchers_count')
					db_repo.forks_count = repo_data.get('forks_count')
					db_repo.open_issues_count = repo_data.get('open_issues_count')
					db_repo.language = repo_data.get('language')

					# Repository features
					db_repo.has_issues = repo_data.get('has_issues')
					db_repo.has_projects = repo_data.get('has_projects')
					db_repo.has_downloads = repo_data.get('has_downloads')
					db_repo.has_wiki = repo_data.get('has_wiki')
					db_repo.has_pages = repo_data.get('has_pages')
					db_repo.has_discussions = repo_data.get('has_discussions')
					db_repo.archived = repo_data.get('archived')
					db_repo.disabled = repo_data.get('disabled')
					db_repo.allow_forking = repo_data.get('allow_forking')
					db_repo.is_template = repo_data.get('is_template')
					db_repo.web_commit_signoff_required = repo_data.get('web_commit_signoff_required')

					# Topics
					if repo_data.get('topics'):
						db_repo.topics = ','.join(repo_data.get('topics'))

					db_repo.visibility = repo_data.get('visibility')
					db_repo.default_branch = repo_data.get('default_branch')

					# License information
					if repo_data.get('license'):
						db_repo.license_key = repo_data.get('license', {}).get('key')
						db_repo.license_name = repo_data.get('license', {}).get('name')
						db_repo.license_url = repo_data.get('license', {}).get('url')

					# Try to parse timestamps
					try:
						if repo_data.get('created_at'):
							db_repo.created_at = datetime.strptime(repo_data.get('created_at'), '%Y-%m-%dT%H:%M:%SZ')
						if repo_data.get('updated_at'):
							db_repo.updated_at = datetime.strptime(repo_data.get('updated_at'), '%Y-%m-%dT%H:%M:%SZ')
						if repo_data.get('pushed_at'):
							db_repo.pushed_at = datetime.strptime(repo_data.get('pushed_at'), '%Y-%m-%dT%H:%M:%SZ')
					except ValueError as e:
						logger.warning(f"Error parsing timestamps: {e}")

					stats["updated"] += 1
					# logger.debug(f"Updated repository: {db_repo.id} - {db_repo.github_repo_name}")
				else:
					# No matching GitHub data found
					stats["not_found"] += 1
					# logger.warning(f"No GitHub data for id: {db_repo.id} db_repo: {db_repo}")

				# Commit periodically to avoid large transactions
				if (stats["updated"] + stats["not_found"]) % 100 == 0:
					session.commit()
					logger.warning(f"Progress: {stats['updated'] + stats['not_found']}/{stats['total_db_repos']} repositories processed")

			except Exception as e:
				logger.error(f"Error processing repo {db_repo.id} - {db_repo.github_repo_name}: {e}")
				stats["errors"] += 1

		# Final commit
		session.commit()
		logger.info(f"Finished processing repositories: {stats}")

	except Exception as e:
		logger.error(f"Error fetching database repositories: {e}")
		stats["errors"] += 1
		return {"errors": stats["errors"], "message": str(e)}

	return stats

async def populate_repo_data(session, args):
	"""
	Update existing GitRepo entries in database with detailed information from GitHub starred repositories.
	Uses the provided git_repos list rather than querying again.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary statistics about the operation
	"""
	from datetime import datetime
	import os

	# Use the existing git_repos variable
	git_repos = session.query(GitRepo).all()

	# Fetch starred repositories from GitHub API
	starred_repos = await get_git_stars()

	stats = {
		"total_db_repos": len(git_repos),
		"total_starred_repos": len(starred_repos['repos']),
		"matched": 0,
		"updated": 0,
		"not_found": 0,
		"errors": 0
	}

	# Create lookup dictionaries for faster matching
	github_repos_by_url = {}
	github_repos_by_name = {}

	# Build lookup dictionaries
	for repo_data in starred_repos['repos']:
		# Index by various URLs
		if repo_data.get('clone_url'):
			github_repos_by_url[repo_data['clone_url']] = repo_data
		if repo_data.get('html_url'):
			github_repos_by_url[repo_data['html_url']] = repo_data
		if repo_data.get('ssh_url'):
			github_repos_by_url[repo_data['ssh_url']] = repo_data
		if repo_data.get('git_url'):
			github_repos_by_url[repo_data['git_url']] = repo_data

		# Index by name
		if repo_data.get('full_name'):
			github_repos_by_name[repo_data['full_name'].lower()] = repo_data
		if repo_data.get('name'):
			github_repos_by_name[repo_data['name'].lower()] = repo_data

	logger.info(f"Processing {stats['total_db_repos']} database repositories against {stats['total_starred_repos']} starred repos")

	# Process each repository from the git_repos list
	try:
		for db_repo in git_repos:
			try:
				# Try to find matching GitHub data
				repo_data = None
				# Try matching by URL first
				if db_repo.git_url and db_repo.git_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url]
					stats["matched"] += 1
				elif db_repo.git_url and db_repo.git_url.replace('.git', '') in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.git_url.replace('.git', '')]
					stats["matched"] += 1
				elif db_repo.clone_url and db_repo.clone_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.clone_url]
					stats["matched"] += 1
				elif db_repo.html_url and db_repo.html_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.html_url]
					stats["matched"] += 1
				elif db_repo.ssh_url and db_repo.ssh_url in github_repos_by_url:
					repo_data = github_repos_by_url[db_repo.ssh_url]
					stats["matched"] += 1

				# Then try by name
				elif db_repo.full_name and db_repo.full_name.lower() in github_repos_by_name:
					repo_data = github_repos_by_name[db_repo.full_name.lower()]
					stats["matched"] += 1
				elif db_repo.github_repo_name:
					# Try owner/name format
					full_name = f"{db_repo.github_owner}/{db_repo.github_repo_name}".lower()
					if full_name in github_repos_by_name:
						repo_data = github_repos_by_name[full_name]
						stats["matched"] += 1
					# Try just the name
					elif db_repo.github_repo_name.lower() in github_repos_by_name:
						repo_data = github_repos_by_name[db_repo.github_repo_name.lower()]
						stats["matched"] += 1

				# If we found matching data, update the database entry
				if repo_data:
					# Update the entry with all GitHub data
					update_repo_from_data(db_repo, repo_data)
					stats["updated"] += 1
					if args.debug:
						logger.debug(f"Updated repository: {db_repo.id} - {db_repo.github_repo_name} scan_count: {db_repo.scan_count} ")
				else:
					# No matching GitHub data found
					stats["not_found"] += 1

				# Commit periodically to avoid large transactions
				if (stats["updated"] + stats["not_found"]) % 100 == 0:
					session.commit()
					logger.info(f"Progress: {stats['updated'] + stats['not_found']}/{stats['total_db_repos']} repositories processed")

			except Exception as e:
				logger.error(f"Error processing repo {db_repo.id} - {db_repo.github_repo_name}: {e}")
				stats["errors"] += 1

		# Final commit
		session.commit()
		logger.info(f"Finished processing repositories: {stats}")

	except Exception as e:
		logger.error(f"Error processing repositories: {e}")
		stats["errors"] += 1
		return {"errors": stats["errors"], "message": str(e)}

	return stats

async def fetch_missing_repo_data(session, update_all=False):
	"""
	Fetch GitHub API data for repositories that weren't found in starred repos
	"""

	stats = {
		"total_repos": 0,
		"processed": 0,
		"updated": 0,
		"failed": 0,
		"skipped": 0,
		"from_cache": 0
	}

	# Load cache first
	stars_cache_file = f'{CACHE_DIR}/missing_repo_data.json'
	cache_data = {'repos': []}

	if os.path.exists(stars_cache_file):
		try:
			async with aiofiles.open(stars_cache_file, 'r') as f:
				cache_content = await f.read()
				cache_data = json.loads(cache_content)
				logger.info(f"Loaded {len(cache_data['repos'])} repositories from cache")
		except Exception as e:
			logger.error(f"Failed to load cache: {e}")

	# Build lookup dictionary for cached repos
	cache_repos_by_name = {}
	for repo in cache_data.get('repos', []):
		if repo.get('full_name'):
			cache_repos_by_name[repo['full_name'].lower()] = repo
		if repo.get('name'):
			# Also index by name only as fallback
			cache_repos_by_name[repo['name'].lower()] = repo

	# Get GitHub auth token
	auth = get_auth_param()
	if not auth:
		logger.error('fetch_missing_repo_data: no auth provided')
		return stats

	# Setup GitHub API headers
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	# Query repos from database that need updating
	if update_all:
		db_repos = session.query(GitRepo).all()
	else:
		# Only get repos that are missing key GitHub data
		db_repos = session.query(GitRepo).filter((GitRepo.node_id.is_(None)) | (GitRepo.full_name.is_(None)) | (GitRepo.description.is_(None))).all()

	stats["total_repos"] = len(db_repos)
	logger.info(f"Found {stats['total_repos']} repositories that need GitHub data")

	# Process repositories in batches for efficiency
	batch_size = 10
	for i in range(0, len(db_repos), batch_size):
		batch = db_repos[i:i+batch_size]
		tasks = []

		async with aiohttp.ClientSession() as api_session:
			for repo in batch:
				tasks.append(process_repo(repo, api_session, headers, cache_repos_by_name, cache_data, stats, session))

			await asyncio.gather(*tasks)

			# Commit after each batch
			session.commit()
			logger.info(f"Progress: {i+len(batch)}/{stats['total_repos']} repositories processed")

	# Final cache update
	try:
		if not os.path.exists(CACHE_DIR):
			os.makedirs(CACHE_DIR)

		async with aiofiles.open(stars_cache_file, 'w') as f:
			cache_data['timestamp'] = str(datetime.now())
			await f.write(json.dumps(cache_data, indent=4))
			logger.info("Final update to cache file with new data")
	except Exception as e:
		logger.error(f"Failed to update cache: {e}")

	# Final commit
	session.commit()
	logger.info(f"Finished fetching repository data: {stats}")

	return stats

async def process_repo(repo, api_session, headers, cache_repos_by_name, cache_data, stats, session):
	"""Helper function to process a single repository"""
	stats["processed"] += 1

	# Skip if no owner/name information is available
	if not repo.github_owner or not repo.github_repo_name:
		stats["skipped"] += 1
		logger.warning(f"Skipping repo with missing owner/name: {repo}")
		return

	# Construct the repository path
	repo_path = f"{repo.github_owner}/{repo.github_repo_name}"
	api_url = f"https://api.github.com/repos/{repo_path}"

	# First check if repo exists in cache
	repo_data = None
	repo_key = repo_path.lower()

	if repo_key in cache_repos_by_name:
		repo_data = cache_repos_by_name[repo_key]
		stats["from_cache"] += 1
	else:
		# Not in cache, make API request
		logger.info(f"Fetching GitHub data for {repo_path} ({stats['processed']}/{stats['total_repos']})")

		try:
			# Make the API request
			async with api_session.get(api_url, headers=headers) as response:
				if response.status == 200:
					repo_data = await response.json()

					# Add to cache
					cache_data['repos'].append(repo_data)
					cache_repos_by_name[repo_path.lower()] = repo_data

				elif response.status == 404:
					logger.warning(f"Repository not found on GitHub: {repo_path} (possibly private, renamed or deleted)")
					stats["failed"] += 1
					return
				else:
					error_text = await response.text()
					logger.error(f"GitHub API error ({response.status}): {error_text}")
					stats["failed"] += 1
					return

		except Exception as e:
			logger.error(f"Error processing {repo_path}: {e}")
			stats["failed"] += 1
			return

	# If we have data (either from cache or API), update the repository
	if repo_data:
		try:
			# Update repository with data
			update_repo_from_data(repo, repo_data)
			stats["updated"] += 1

		except Exception as e:
			logger.error(f"Error updating repository {repo_path}: {e}")
			stats["failed"] += 1

def update_repo_from_data(repo, repo_data):
	"""Update a repository with data from GitHub API"""
	repo.last_scan = datetime.now()
	repo.scan_count += 1

	# Update all fields with API data
	repo.node_id = repo_data.get('node_id')
	repo.full_name = repo_data.get('full_name')
	repo.private = repo_data.get('private')
	repo.html_url = repo_data.get('html_url')
	repo.description = repo_data.get('description')
	repo.fork = repo_data.get('fork')
	repo.clone_url = repo_data.get('clone_url')
	repo.ssh_url = repo_data.get('ssh_url')
	repo.git_url_api = repo_data.get('git_url')
	repo.svn_url = repo_data.get('svn_url')
	repo.homepage = repo_data.get('homepage')

	# Statistics
	repo.size = repo_data.get('size')
	repo.stargazers_count = repo_data.get('stargazers_count')
	repo.watchers_count = repo_data.get('watchers_count')
	repo.forks_count = repo_data.get('forks_count')
	repo.open_issues_count = repo_data.get('open_issues_count')
	repo.language = repo_data.get('language')

	# Repository features
	repo.has_issues = repo_data.get('has_issues')
	repo.has_projects = repo_data.get('has_projects')
	repo.has_downloads = repo_data.get('has_downloads')
	repo.has_wiki = repo_data.get('has_wiki')
	repo.has_pages = repo_data.get('has_pages')
	repo.has_discussions = repo_data.get('has_discussions')
	repo.archived = repo_data.get('archived')
	repo.disabled = repo_data.get('disabled')
	repo.allow_forking = repo_data.get('allow_forking')
	repo.is_template = repo_data.get('is_template')
	repo.web_commit_signoff_required = repo_data.get('web_commit_signoff_required')

	# Topics
	if repo_data.get('topics'):
		repo.topics = ','.join(repo_data.get('topics'))

	repo.visibility = repo_data.get('visibility')
	repo.default_branch = repo_data.get('default_branch')

	# License information
	if repo_data.get('license'):
		repo.license_key = repo_data.get('license', {}).get('key')
		repo.license_name = repo_data.get('license', {}).get('name')
		repo.license_url = repo_data.get('license', {}).get('url')

	# Parse timestamps
	try:
		if repo_data.get('created_at'):
			repo.created_at = datetime.strptime(repo_data.get('created_at'), '%Y-%m-%dT%H:%M:%SZ')
		if repo_data.get('updated_at'):
			repo.updated_at = datetime.strptime(repo_data.get('updated_at'), '%Y-%m-%dT%H:%M:%SZ')
		if repo_data.get('pushed_at'):
			repo.pushed_at = datetime.strptime(repo_data.get('pushed_at'), '%Y-%m-%dT%H:%M:%SZ')
	except ValueError as e:
		logger.warning(f"Error parsing timestamps: {e}")


def populate_from_metadata(repo, metadata):
	"""
	Populate a GitRepo object with metadata from GitHub API

	Parameters:
		repo: GitRepo object to populate
		metadata: Dictionary containing GitHub repository metadata

	Returns:
		GitRepo: The updated repo object
	"""
	if not metadata:
		logger.warning("No metadata provided to populate_from_metadata")
		return repo

	# Record the update time
	repo.last_scan = datetime.now()
	repo.scan_count = repo.scan_count + 1 if repo.scan_count else 1

	# Basic repository information
	repo.github_repo_name = metadata.get('name')
	repo.full_name = metadata.get('full_name')
	repo.node_id = metadata.get('node_id')
	repo.private = metadata.get('private')

	# URLs
	repo.html_url = metadata.get('html_url')
	repo.git_url = metadata.get('git_url')
	repo.ssh_url = metadata.get('ssh_url')
	repo.clone_url = metadata.get('clone_url')
	repo.svn_url = metadata.get('svn_url')
	repo.git_url_api = metadata.get('url')
	repo.homepage = metadata.get('homepage')

	# Description
	repo.description = metadata.get('description')

	# Repository type information
	repo.fork = metadata.get('fork')
	repo.visibility = metadata.get('visibility')
	repo.default_branch = metadata.get('default_branch')

	# Statistics
	repo.size = metadata.get('size')
	repo.stargazers_count = metadata.get('stargazers_count')
	repo.watchers_count = metadata.get('watchers_count')
	repo.forks_count = metadata.get('forks_count')
	repo.open_issues_count = metadata.get('open_issues_count')
	repo.language = metadata.get('language')

	# Repository features
	repo.has_issues = metadata.get('has_issues')
	repo.has_projects = metadata.get('has_projects')
	repo.has_downloads = metadata.get('has_downloads')
	repo.has_wiki = metadata.get('has_wiki')
	repo.has_pages = metadata.get('has_pages')
	repo.has_discussions = metadata.get('has_discussions')
	repo.archived = metadata.get('archived')
	repo.disabled = metadata.get('disabled')
	repo.allow_forking = metadata.get('allow_forking')
	repo.is_template = metadata.get('is_template')
	repo.web_commit_signoff_required = metadata.get('web_commit_signoff_required')

	# Topics - stored as comma-separated string
	if metadata.get('topics'):
		repo.topics = ','.join(metadata.get('topics'))

	# License information
	if metadata.get('license') and isinstance(metadata['license'], dict):
		repo.license_key = metadata['license'].get('key')
		repo.license_name = metadata['license'].get('name')
		repo.license_url = metadata.get('license').get('url')

	# Owner information
	if metadata.get('owner') and isinstance(metadata['owner'], dict):
		repo.github_owner = metadata['owner'].get('login')
		# Store additional owner data if needed

	# Parse timestamps
	try:
		if metadata.get('created_at'):
			repo.created_at = ensure_datetime(metadata.get('created_at'))
		if metadata.get('updated_at'):
			repo.updated_at = ensure_datetime(metadata.get('updated_at'))
		if metadata.get('pushed_at'):
			repo.pushed_at = ensure_datetime(metadata.get('pushed_at'))
	except ValueError as e:
		logger.warning(f"Error parsing timestamps: {e}")

	# Store complete metadata if needed for reference
	# repo.raw_metadata = json.dumps(metadata)  # If you want to store the raw JSON

	logger.debug(f"Populated metadata for {repo.full_name} ({repo.id})")
	return repo

async def fetch_metadata(repo):
	"""
	Fetch metadata from GitHub API for this repository with caching

	Parameters:
		repo: GitRepo object to fetch metadata for

	Returns:
		dict: Repository metadata or None if fetch failed
	"""
	# Generate a cache key based on repository owner and name
	repo_path = f"{repo.github_owner}/{repo.github_repo_name}" if repo.github_owner and repo.github_repo_name else None

	if not repo_path:
		logger.warning(f"Can't fetch metadata for repo without owner/name: {repo}")
		return None

	# Setup cache file
	metadata_cache_file = f'{CACHE_DIR}/repo_metadata_cache.json'
	cache_data = {'repos': {}, 'last_updated': {}}

	# Create cache directory if it doesn't exist
	try:
		os.makedirs(CACHE_DIR, exist_ok=True)
	except Exception as e:
		logger.warning(f"Failed to create cache directory: {e}")

	# Try to load existing cache
	try:
		if os.path.exists(metadata_cache_file):
			with open(metadata_cache_file, 'r') as f:
				try:
					cache_data = json.load(f)
					# logger.debug(f"Loaded metadata cache with {len(cache_data['repos'])} entries")
				except json.JSONDecodeError as e:
					logger.warning(f"Corrupted JSON cache file: {e}. Creating backup and using empty cache.")
					# Create a backup of the corrupted file
					try:
						backup_file = f"{metadata_cache_file}.bak.{int(time.time())}"
						shutil.copy2(metadata_cache_file, backup_file)
						logger.warning(f"Created backup of corrupted cache file: {backup_file}")
					except Exception as backup_err:
						logger.error(f"Failed to create backup of corrupted cache: {backup_err}")

					# Use empty cache data
					cache_data = {'repos': {}, 'last_updated': {}}
	except Exception as e:
		logger.error(f"Failed to load metadata cache: {e} {type(e)}")

	# Check if we have cached data for this repo and it's less than 1 day old
	current_time = datetime.now().timestamp()
	one_day_seconds = 86400  # 24 hours in seconds

	cached_repo = cache_data.get('repos', {}).get(repo_path)
	cache_time = cache_data.get('last_updated', {}).get(repo_path, 0)
	cache_age = current_time - cache_time

	# Use cached data if it exists and is less than 1 day old
	if cached_repo and cache_age < one_day_seconds:
		logger.info(f"Using cached metadata for {repo_path} (age: {cache_age:.1f} seconds)")
		return cached_repo

	auth = get_auth_param()
	if not auth:
		logger.error('fetch_metadata: no auth provided')
		return None

	api_url = f'https://api.github.com/repos/{repo_path}'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	repo_metadata = None
	try:
		# Otherwise, we need to fetch from the API
		logger.info(f"Fetching metadata from GitHub API for {repo_path} from {api_url}")
		async with aiohttp.ClientSession() as session:
			async with session.get(api_url, headers=headers) as r:
				if r.status == 200:
					repo_metadata = await r.json()

					# Update cache
					if 'repos' not in cache_data:
						cache_data['repos'] = {}
					if 'last_updated' not in cache_data:
						cache_data['last_updated'] = {}

					cache_data['repos'][repo_path] = repo_metadata
					cache_data['last_updated'][repo_path] = current_time

					# Save updated cache safely
					try:
						# Ensure cache directory exists before writing
						os.makedirs(os.path.dirname(metadata_cache_file), exist_ok=True)

						# Write directly to the file - safer for simple operations
						with open(metadata_cache_file, 'w') as f:
							json.dump(cache_data, f, indent=2)
						logger.debug(f"Updated cache with new data for {repo_path}")
					except Exception as e:
						logger.warning(f"Failed to write to cache file: {e} {type(e)}")

					return repo_metadata
				elif r.status == 403:
					# Check rate limit headers
					reset_time = r.headers.get('X-RateLimit-Reset')
					if reset_time:
						reset_datetime = datetime.fromtimestamp(int(reset_time))
						wait_time = (reset_datetime - datetime.now()).total_seconds()
						logger.warning(f"Rate limit exceeded. Resets in {wait_time:.1f} seconds")
					else:
						logger.warning(f"Rate limit exceeded. reset_time not in headers {r.headers}")

					# Return cached data even if stale when rate limited
					if cached_repo:
						logger.warning(f"Using stale cache due to rate limiting for {repo_path}")
						return cached_repo
				else:
					logger.error(f"Failed to fetch metadata from {api_url} : {r.status} {await r.text()}")

					# Return cached data even if stale when API fails
					if cached_repo:
						logger.warning(f"Using stale cache due to API error for {repo_path}")
						return cached_repo
	except Exception as e:
		logger.error(f"Error fetching metadata: {e} {type(e)} from {api_url}")
		# Return cached data even if stale when exception occurs
		if cached_repo:
			logger.warning(f"Using stale cache due to exception for {repo_path}")
			return cached_repo
	return repo_metadata
