from __future__ import annotations
import os
import json
import traceback
from datetime import datetime
from pathlib import Path
from loguru import logger
from dbstuff import GitRepo, GitFolder, RepoInfo, GitStar, GitList, get_dupes
from utils import valid_git_folder, get_remote_url, ensure_datetime
from gitstars import get_lists_and_stars_unified, fetch_github_starred_repos
from cacheutils import update_repo_cache, get_cache_entry, RateLimitExceededError

async def verify_star_list_links(session, args):
	"""Verify that GitStar entries are properly linked to GitList entries"""
	try:
		# Count total GitStar entries
		total_stars = session.query(GitStar).count()

		# Count GitStar entries with list links
		linked_stars = session.query(GitStar).filter(GitStar.gitlist_id.isnot(None)).count()

		# Count GitStar entries without list links
		unlinked_stars = session.query(GitStar).filter(GitStar.gitlist_id.is_(None)).count()

		logger.info(f"GitStar verification: Total={total_stars}, Linked={linked_stars}, Unlinked={unlinked_stars}")

		return {
			'total_stars': total_stars,
			'linked_stars': linked_stars,
			'unlinked_stars': unlinked_stars
		}

	except Exception as e:
		logger.error(f"Error verifying star-list links: {e} {type(e)}")
		logger.error(f'traceback: {traceback.format_exc()}')
		return None

async def insert_update_git_folder(git_folder_path, session, args):
	"""
	Insert a new GitFolder or update an existing one in the database
	"""
	git_folder_path = str(Path(git_folder_path))
	# Ensure this is a valid git folder before proceeding
	if not valid_git_folder(os.path.join(git_folder_path, '.git')):
		logger.warning(f'{git_folder_path} is not a valid git folder')
		return None
	# Get remote URL for this repository
	if git_folder_path.endswith('/'):
		git_folder_path = git_folder_path[:-1]

	# Get remote URL and normalize it
	remote_url = get_remote_url(git_folder_path).lower().strip()
	if not remote_url or remote_url == '[no remote]':
		remote_url = f"file://{git_folder_path}"  # Use local path as fallback URL
		logger.warning(f'Could not determine remote URL for {git_folder_path}  ... skipping')
		return None

	# Extract repo name and owner from URL for GitHub API lookup
	repo_name = None
	owner = None
	# Parse GitHub URL to extract owner and repo name
	if 'github.com' in remote_url:
		if remote_url.startswith('git@github.com:'):
			# git@github.com:owner/repo.git
			path = remote_url.split("git@github.com:")[1].split(".git")[0]
			parts = path.split("/")
			if len(parts) >= 2:
				owner = parts[0]
				repo_name = parts[1]
		elif "github.com/" in remote_url:
			# https://github.com/owner/repo.git
			path = remote_url.split("github.com/")[1].split(".git")[0]
			parts = path.split("/")
			if len(parts) >= 2:
				owner = parts[0]
				repo_name = parts[1]

	# Construct the repo path for API lookup
	repo_path = None
	repo_metadata = None
	if owner and repo_name:
		repo_path = f"{owner}/{repo_name}"
		try:
			# Create a simple object with the required attributes
			repo_info = RepoInfo(owner, repo_name)
			repo_metadata = await fetch_metadata(repo_info, session, args)
		except Exception as e:
			logger.error(f'Failed to fetch metadata for {repo_path}: {e} {type(e)}')
			logger.error(f'traceback: {traceback.format_exc()}')
			repo_metadata = None

	try:
		# First check if the repo already exists by URL
		git_repo = session.query(GitRepo).filter(GitRepo.git_url.ilike(remote_url)).first()

		# If not found by exact URL, try alternative lookups
		if not git_repo:
			# Try without .git suffix
			if remote_url.endswith('.git'):
				git_repo = session.query(GitRepo).filter(GitRepo.git_url.ilike(remote_url[:-4])).first()
			# Try with .git suffix
			else:
				git_repo = session.query(GitRepo).filter(GitRepo.git_url.ilike(remote_url + '.git')).first()

		# Extract repo name from URL for name-based lookup if needed
		if not repo_name:
			repo_name = os.path.basename(git_folder_path)
			if '/' in remote_url:
				parts = remote_url.split('/')
				if len(parts) > 1:
					repo_name = parts[-1]
					if repo_name.endswith('.git'):
						repo_name = repo_name[:-4]

		# Try to fetch metadata one more time if needed
		if not repo_metadata and owner and repo_name:
			repo_path = f"{owner}/{repo_name}"
			try:
				repo_metadata = await update_repo_cache(repo_path, session, args)
			except RateLimitExceededError as e:
				logger.warning(f'Failed to fetch metadata for {repo_path}: {e}')
				repo_metadata = None
			except Exception as e:
				logger.error(f'Failed to fetch metadata for {repo_path}: {e}')
				logger.error(f'traceback: {traceback.format_exc()}')
				repo_metadata = None

		# Try lookup by name if URL lookup failed
		if not git_repo:
			git_repo = session.query(GitRepo).filter(GitRepo.github_repo_name == repo_name).first()

		# Check if folder already exists in database
		git_folder = session.query(GitFolder).filter(GitFolder.git_path == git_folder_path).first()
		if 'BLANK_REPO_DATA' in remote_url or 'BLANK_REPO_DATA' in git_folder_path or 'BLANK_REPO_DATA' in repo_name:
			logger.warning(f"BLANK_REPO_DATA found in remote_url: {remote_url} or git_folder_path: {git_folder_path} or repo_name: {repo_name}")

		# If no repo exists, create a new one with safeguards
		if not git_repo:
			# Double check once more with a broader query
			git_repo = session.query(GitRepo).filter((GitRepo.git_url.ilike(f"%{repo_name}%")) | (GitRepo.github_repo_name == repo_name)).first()

			if not git_repo:
				git_repo = GitRepo(remote_url, git_folder_path)
				git_repo.github_repo_name = repo_name
				git_repo.github_owner = owner
				git_repo.first_scan = datetime.now()
				git_repo.last_scan = datetime.now()
				git_repo.scan_count = 1
				git_repo.update_local_git_info()

				# Populate with metadata if available
				if repo_metadata:
					if 'BLANK_REPO_DATA' in repo_metadata:
						logger.warning(f"BLANK_REPO_DATA found in repo_metadata for {repo_name} repo_metadata: {repo_metadata}")
					git_repo = populate_from_metadata(git_repo, repo_metadata)
				session.add(git_repo)
				session.flush()  # Get the ID without committing
				if 'BLANK_REPO_DATA' in git_repo.git_url or 'BLANK_REPO_DATA' in git_repo.github_repo_name:
					logger.warning(f"BLANK_REPO_DATA found in git_repo.git_url: {git_repo.git_url} or git_repo.github_repo_name: {git_repo.github_repo_name}")
					session.rollback()
					return None
				else:
					logger.info(f'Created new GitRepo: github_repo_name {git_repo.github_repo_name} full_name: {git_repo.full_name}')

		else:
			# Update existing repo
			git_repo.last_scan = datetime.now()
			git_repo.scan_count += 1
			git_repo.update_config_times()
			git_repo.update_local_git_info()

			# Update with metadata if available
			if repo_metadata:
				git_repo = populate_from_metadata(git_repo, repo_metadata)
			else:
				logger.warning(f'No metadata found for {git_repo} git_url: {git_repo.git_url}')

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
		logger.error(f'Database error: {e} {type(e)} for git_folder_path: {git_folder_path}')
		logger.error(f'traceback: {traceback.format_exc()}')
		if repo_metadata:
			logger.error(f'repo_metadata: {repo_metadata}')
		logger.error(f'traceback: {traceback.format_exc()}')
		logger.error(f'tracebackstack: {traceback.print_stack()}')
		if session.is_active:
			session.rollback()
		return None

async def create_repo_to_list_mapping(session, args):
	"""Create a mapping of repo URLs to list names - fetch once and reuse"""
	git_lists_data = await get_lists_and_stars_unified(session, args)
	repo_to_list_mapping = {}

	# Debug the structure
	if args.debug:
		logger.debug(f"git_lists_data keys: {git_lists_data.keys()}")
		logger.debug(f"git_lists_data structure: {type(git_lists_data)}")

	# Extract the correct data structure
	lists_with_repos = git_lists_data.get('lists_with_repos', {})

	# Handle the nested structure more carefully
	if isinstance(lists_with_repos, dict):
		if 'lists_with_repos' in lists_with_repos:
			# Handle double-nested structure
			actual_lists = lists_with_repos['lists_with_repos']
		else:
			# Handle single-level structure
			actual_lists = lists_with_repos
	else:
		logger.warning(f"Unexpected lists_with_repos type: {type(lists_with_repos)}")
		return repo_to_list_mapping

	if args.debug:
		logger.debug(f"actual_lists type: {type(actual_lists)}, keys: {actual_lists.keys() if isinstance(actual_lists, dict) else 'N/A'}")

	# Ensure actual_lists is a dictionary before iterating
	if not isinstance(actual_lists, dict):
		logger.warning(f"actual_lists is not a dictionary: {type(actual_lists)}")
		return repo_to_list_mapping

	# Now iterate over the actual list data
	for list_name, list_data in actual_lists.items():
		# Ensure list_data is a dictionary
		if not isinstance(list_data, dict):
			logger.warning(f"list_data for {list_name} is not a dictionary: {type(list_data)}")
			continue

		hrefs = list_data.get('hrefs', [])
		if args.debug:
			logger.debug(f"List {list_name} has {len(hrefs)} hrefs")

		for href in hrefs:
			href_clean = href.strip('/').split('github.com/')[-1].rstrip('.git')
			repo_to_list_mapping[href_clean] = list_name

	logger.info(f"Created repo-to-list mapping with {len(repo_to_list_mapping)} entries")
	return repo_to_list_mapping

async def insert_update_starred_repo(github_repo, session, args, create_new=False, list_name=None):
	"""
	Insert a new GitRepo or update an existing one in the database
	Also create GitStar entry and link to GitList if provided
	"""
	git_folder_path = '[notcloned]'

	# Check if github_repo is a string (URL or owner/repo) or a dict
	if isinstance(github_repo, dict):
		repo_data = github_repo
		remote_url = repo_data.get('html_url') or f"https://github.com/{repo_data.get('full_name')}"
		full_name = repo_data.get('full_name')
		clean_path = full_name
	else:
		clean_path = github_repo.strip('/')
		if 'github.com' in clean_path:
			remote_url = f"https://{clean_path}"
			full_name = clean_path.split('github.com/')[-1]
		else:
			remote_url = f"https://github.com/{clean_path}"
			full_name = clean_path

	# Get or create GitRepo object
	git_repo = session.query(GitRepo).filter(GitRepo.git_url == remote_url).first()

	# Get full repository data from GitHub API
	try:
		repo_data = await update_repo_cache(clean_path if isinstance(github_repo, str) else full_name, session, args)
	except RateLimitExceededError as e:
		logger.warning(f'Rate limit exceeded while fetching metadata for {clean_path if isinstance(github_repo, str) else full_name}: {e}')
		raise e

	if not git_repo:
		if create_new:
			git_repo = GitRepo(remote_url, git_folder_path, repo_data)
			session.add(git_repo)
			session.flush()  # Get the ID
			logger.info(f'Created new GitRepo: github_repo_name {git_repo.github_repo_name} full_name: {git_repo.full_name}')
		else:
			logger.warning(f'Skipping creation of new GitRepo for {remote_url} as create_new is False')
			return None
	else:
		logger.info(f'update GitRepo: {git_repo} remote_url: {remote_url}. repo_data: {type(repo_data)}')
		if repo_data:
			update_repo_from_data(git_repo, repo_data)

	# Create or update GitStar entry
	if git_repo:
		# Mark repo as starred
		git_repo.is_starred = True
		git_repo.starred_at = datetime.now()

		# Check if GitStar entry exists
		git_star = session.query(GitStar).filter(GitStar.gitrepo_id == git_repo.id).first()
		if not git_star:
			git_star = GitStar()
			git_star.gitrepo_id = git_repo.id
			git_star.starred_at = datetime.now()
			if repo_data:
				git_star.stargazers_count = repo_data.get('stargazers_count')
				git_star.description = repo_data.get('description')
				git_star.full_name = repo_data.get('full_name')
				git_star.html_url = repo_data.get('html_url')
			session.add(git_star)
			session.flush()  # Get the ID

		# Link to GitList if list_name provided
		if list_name:
			git_list = session.query(GitList).filter(GitList.list_name == list_name).first()
			if git_list:
				git_star.gitlist_id = git_list.id
				logger.info(f"Linked GitStar {git_star.id} to GitList {git_list.id} ({list_name})")
	else:
		logger.warning(f'No GitRepo found for remote_url: {remote_url}')

	if create_new:
		session.commit()
	return git_repo

def check_update_dupes(session) -> dict:
	"""
	Check for duplicate GitRepo entries (same git_url) and update their dupe_count.
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
		repo.dupe_count = 0

	# Get list of duplicates (repos with same git_url)
	dupes = get_dupes(session)
	dupe_urls = set()
	dupes_updated = 0

	# Process each duplicate group
	for dupe in dupes:
		# dupe_id = dupe.id
		dupe_url = dupe.git_url
		dupe_count = dupe.count
		dupe_urls.add(dupe_url)

		# Find all repos with this URL
		same_url_repos = session.query(GitRepo).filter(GitRepo.git_url == dupe_url).all()

		# Update their dupe flags
		for repo in same_url_repos:
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

async def populate_repo_data(session, args, starred_repos=None):
	"""
	Update existing GitRepo entries in database with detailed information from GitHub starred repositories.
	Uses the provided git_repos list rather than querying again.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary statistics about the operation
	"""
	if args.global_limit > 0:
		logger.warning(f'Global limit set to {args.global_limit}, this will limit the number of repositories processed.')
		git_repos = session.query(GitRepo).limit(args.global_limit).all()
	else:
		# Use the existing git_repos variable
		git_repos = session.query(GitRepo).all()

	# Use provided starred_repos or fetch if not provided
	if starred_repos is None:
		starred_repos = await fetch_github_starred_repos(args, session)

	stats = {
		"total_db_repos": len(git_repos),
		"total_starred_repos": len(starred_repos),
		"matched": 0,
		"updated": 0,
		"not_found": 0,
		"errors": 0
	}

	# Create lookup dictionaries for faster matching
	github_repos_by_url = {}
	github_repos_by_name = {}

	# Build lookup dictionaries
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
					# if args.debug:
					# 	logger.debug(f"Updated repository: {db_repo.id} - {db_repo.github_repo_name} scan_count: {db_repo.scan_count} ")
				else:
					# No matching GitHub data found
					stats["not_found"] += 1

				# Commit periodically to avoid large transactions
				if (stats["updated"] + stats["not_found"]) % 100 == 0:
					session.commit()
					logger.info(f"Progress: {stats['updated'] + stats['not_found']}/{stats['total_db_repos']} repositories processed")

			except Exception as e:
				logger.error(f"Error processing repo {db_repo.id} - {db_repo.github_repo_name}: {e} {type(e)}")
				logger.error(f'traceback: {traceback.format_exc()}')
				stats["errors"] += 1

		# Final commit
		session.commit()
		logger.info(f"Finished processing repositories: {stats}")

	except Exception as e:
		logger.error(f"Error processing repositories: {e} {type(e)}")
		logger.error(f'traceback: {traceback.format_exc()}')
		stats["errors"] += 1
		return {"errors": stats["errors"], "message": str(e)}

	return stats

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

	# logger.debug(f"Populated metadata for repoid: {repo.id} - {repo.full_name}")
	return repo

async def fetch_metadata(repo, session, args):
	"""
	Fetch metadata from GitHub API for this repository with database caching

	Parameters:
		repo: GitRepo object to fetch metadata for
		session: SQLAlchemy session

	Returns:
		dict: Repository metadata or None if fetch failed
	"""
	# Generate a cache key based on repository owner and name
	# Handle both dictionary and object access
	owner = repo.github_owner if hasattr(repo, 'github_owner') else repo.get('github_owner')
	repo_name = repo.github_repo_name if hasattr(repo, 'github_repo_name') else repo.get('github_repo_name')
	repo_path = f"{owner}/{repo_name}" if owner and repo_name else None

	if not repo_path:
		logger.warning(f"Can't fetch metadata for repo without owner/name: {repo}")
		return None

	cache_key = f"metadata:{repo_path}"
	cache_type = "repo_metadata"

	# Try to load existing cache
	cached_repo = None
	cache_entry = get_cache_entry(session, cache_key, cache_type)

	# Check if we have cached data for this repo and it's less than 1 day old
	current_time = datetime.now().timestamp()
	one_day_seconds = 86400  # 24 hours in seconds

	if cache_entry:
		try:
			cached_repo = json.loads(cache_entry.data)
			cache_time = cache_entry.timestamp.timestamp()
			cache_age = current_time - cache_time

			# Use cached data if it exists and is less than 1 day old
			if cached_repo and cache_age < one_day_seconds:
				logger.info(f"Using cached metadata for {repo_path} (age: {cache_age:.1f} seconds)")
				return cached_repo
		except Exception as e:
			logger.error(f"Error parsing cached metadata: {e} {type(e)} for {repo}")
			logger.error(f'traceback: {traceback.format_exc()}')
	else:
		# Use update_repo_cache to get metadata
		repo_metadata = None
		try:
			repo_metadata = await update_repo_cache(repo_path, session, args)
			return repo_metadata
		except TimeoutError as e:
			logger.warning(f"Timeout fetching metadata for {repo_path}: {e}")
			return None
		except RateLimitExceededError as e:
			logger.warning(f"Rate limit exceeded for repository {repo_path}: {e}")
			raise e
		except AttributeError as e:
			logger.warning(f"Error fetching repository metadata: {e} for {repo_path}")
			logger.warning(f'traceback: {traceback.format_exc()}')
		except Exception as e:
			logger.error(f"Error fetching repository metadata: {e} {type(e)} for {repo_path}")
			logger.error(f'traceback: {traceback.format_exc()}')
			# return None
		if not repo_metadata:
			logger.warning(f"No cache entry found for {repo_path}")
			return None

