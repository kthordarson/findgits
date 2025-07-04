#!/usr/bin/python3
import asyncio
import traceback
import aiohttp
import json
import os
from loguru import logger
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
from datetime import datetime
from dbstuff import CacheEntry, BLANK_REPO_DATA

def RateLimitExceededError(Exception):
	"""Custom exception for rate limit exceeded errors"""
	pass

async def update_repo_cache(repo_name_or_url, session, args):
	"""
	Fetch repository data from GitHub API and update the database cache
	"""
	# Extract repo name if URL is provided, with improved path handling
	if '/' in repo_name_or_url:
		# Handle URLs with leading slash
		clean_path = repo_name_or_url.lstrip('/')

		if 'github.com' in clean_path:
			parts = clean_path.split('github.com/')
			if len(parts) > 1:
				repo_name = parts[1].rstrip('.git')
			else:
				logger.error(f"Invalid GitHub URL format: {repo_name_or_url}")
				return None
		else:
			# Assume format is already owner/repo
			repo_name = clean_path
	else:
		logger.error(f"Invalid repository name or URL: {repo_name_or_url}")
		return None

	# Ensure repo_name doesn't have leading/trailing slashes
	repo_name = repo_name.strip('/')

	# Create cache key for this repository
	cache_key = f"repo:{repo_name}"
	cache_type = "repo_data"

	# Load existing cache if available and requested
	cache_data = []
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				cache_data = json.loads(cache_entry.data)
				return cache_data[0]
				# logger.debug(f"Loaded cache for {repo_name} from database")
			except Exception as e:
				logger.error(f"Failed to parse cache data: {e}")
		else:
			logger.warning(f"No cache entry found for {repo_name} in database")
			# Continue with empty cache_data
	if args.nodl:
		logger.warning(f"Skipping API call for {repo_name} due to --nodl flag, returning cached data if available {type(cache_data)} {len(cache_data) if cache_data else 0}")
		if args.debug:
			# logger.debug(f"cache_data for repo: {repo_name} : {cache_data}")
			logger.debug(f"cache_entry: {cache_entry}")
		return cache_data[0]
	auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("FINDGITSTOKEN",''))
	if not auth:
		logger.error('update_repo_cache: no auth provided')
		return None

	api_url = f'https://api.github.com/repos/{repo_name}'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	# Fetch repository data from GitHub API
	try:
		async with aiohttp.ClientSession() as api_session:
			async with api_session.get(api_url, headers=headers) as r:
				if args.debug:
					logger.debug(f"Fetching repository data from GitHub API: {api_url}")
				if r.status == 200:
					repo_data = await r.json()
					# Check if repo already exists in cache
					existing_index = None
					for i, repo in enumerate(cache_data):
						if repo.get('id') == repo_data.get('id'):
							existing_index = i
							# logger.debug(f"Found existing repository in cache: {repo_data.get('name')} at index {i} cache_data: {len(cache_data)}")
							break

					# Update or add to cache
					if existing_index is not None:
						cache_data[existing_index] = repo_data
						# logger.info(f"Updated existing repository in cache: {repo_name} cache_data: {len(cache_data)}")
					else:
						cache_data.append(repo_data)
						# logger.info(f"Added new repository to cache: {repo_name}")

					# Update the database cache
					set_cache_entry(session, cache_key, cache_type, json.dumps(cache_data))
					session.commit()

					return repo_data
				elif r.status == 403:
					rtext = await r.text()
					ratelimit_reset = datetime.fromtimestamp(int(r.headers.get('X-RateLimit-Reset')))
					logger.error(f"Rate limit exceeded for repository {repo_name}: {r.status} rtext: {rtext}")
					logger.error(f"xratelimits: {r.headers.get('X-RateLimit-Used')}/{r.headers.get('X-RateLimit-Remaining')}/{r.headers.get('X-RateLimit-Limit')} 'X-RateLimit-Reset': {ratelimit_reset}")
					logger.error(f'rheaders: {r.headers}')
					# Handle rate limiting by returning cached data if available
					if cache_data:
						logger.debug(f"Returning cached data for {repo_name} due to rate limit")
						await asyncio.sleep(60)  # Wait before retrying
						return cache_data[0]
					else:
						logger.error(f"No cached data available for {repo_name} after rate limit exceeded")
						await asyncio.sleep(60)  # Wait before retrying
						raise RateLimitExceededError(f"Rate limit exceeded for repository {repo_name}, no cached data available")
				elif r.status == 404 or r.status == 451:
					logger.warning(f"Repository error {r.status}: {api_url} - Creating default data structure")

					# Create default repo data structure based on what we know
					# Parse owner and repo name from the repo_name
					owner, name = None, None
					if '/' in repo_name:
						parts = repo_name.split('/')
						if len(parts) >= 2:
							owner = parts[0]
							name = parts[1]
					else:
						name = repo_name

					# Generate a current timestamp for consistency
					current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

					# Create default repository data structure

					# Add to cache so we don't keep trying to fetch it
					default_repo_data = BLANK_REPO_DATA.copy()
					default_repo_data['name'] = name
					default_repo_data['full_name'] = f"{owner}/{name}" if owner else name
					default_repo_data['owner'] = {"login": owner} if owner else {"login": "unknown"}
					default_repo_data['error_code'] = r.status

					cache_data.append(default_repo_data)
					set_cache_entry(session, cache_key, cache_type, json.dumps(cache_data))
					session.commit()
					logger.debug(f"Added default data for unavailable repository to cache: {repo_name}")

					return default_repo_data
				else:
					logger.error(f"Failed to fetch repository data: {r.status} {await r.text()} api_url: {api_url}")
					return None
	except Exception as e:
		logger.error(f"Error fetching repository data: {e} {type(e)}")
		logger.error(f'traceback: {traceback.format_exc()}')
		logger.error(f'tracebackstack: {traceback.print_stack()}')
		return None

def get_cache_entry(session, cache_key, cache_type):
	"""Get a cache entry from the database"""
	return session.query(CacheEntry).filter_by(cache_key=cache_key, cache_type=cache_type).first()

def set_cache_entry(session, cache_key, cache_type, data):
	"""Set or update a cache entry in the database"""
	entry = get_cache_entry(session, cache_key, cache_type)
	if entry:
		# Update existing entry
		entry.data = data
		entry.timestamp = datetime.now()
		entry.last_scan = datetime.now()
	else:
		# Create new entry
		entry = CacheEntry(cache_key, cache_type, data)
		entry.last_scan = datetime.now()
		session.add(entry)

	# The caller is responsible for committing the session
	return entry
