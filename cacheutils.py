#!/usr/bin/python3
import asyncio
import traceback
import aiohttp
import json
import os
import sqlite3
from loguru import logger
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dbstuff import CacheEntry, BLANK_REPO_DATA, RepoCacheExpanded
from utils import get_client_session, ensure_datetime, get_auth_params

class RateLimitExceededError(Exception):
	"""Custom exception for rate limit exceeded errors"""
	pass

async def get_api_rate_limits(args):
	rate_limits = {'limit_hit':False, 'rate_limits': {}}
	try:
		# async with aiohttp.ClientSession() as api_session:
		async with get_client_session(args) as api_session:
			async with api_session.get('https://api.github.com/rate_limit') as r:
				rates = await r.json()
	except aiohttp.client_exceptions.ContentTypeError as e:
		logger.error(f"ContentTypeError while fetching rate limits: {e}")
		rates = {}
		rate_limits['limit_hit'] = True
		rate_limits['rate_limits'] = {'error': 'ContentTypeError', 'message': str(e)}
		return rate_limits
	except Exception as e:
		logger.error(f'fatal {e} {type(e)}')
	finally:
		rate_limits['rate_limits'] = rates
		return rate_limits

async def is_rate_limit_hit(args, threshold_percent=10):
	"""
	Check if any GitHub API rate limits are hit or approaching their limits
	Args:
		threshold_percent (int): Percentage of remaining calls below which the limit is considered hit (default: 10%)
	Returns:
		bool: True if any rate limit is hit or approaching the threshold, False otherwise
	"""
	try:
		# Get current rate limits from GitHub API
		rate_limits_data = await get_api_rate_limits(args)

		# Check if limit_hit is already set
		if rate_limits_data.get('limit_hit', False):
			return True

		# Get the resources section
		resources = rate_limits_data.get('rate_limits', {}).get('resources', {})

		# Also check the overall rate limit
		rate = rate_limits_data.get('rate_limits', {}).get('rate', {})
		if rate:
			resources['overall'] = rate

		# Check each resource type
		for resource_name, resource_data in resources.items():
			# Skip if missing essential data
			if not all(k in resource_data for k in ('limit', 'remaining', 'reset')):
				continue

			limit = resource_data.get('limit', 0)
			remaining = resource_data.get('remaining', 0)
			reset_time = resource_data.get('reset', 0)

			# Skip if limit is 0 (unlimited)
			if limit == 0:
				continue

			# Calculate threshold value
			threshold_value = max(1, int(limit * threshold_percent / 100))

			# Check if below threshold
			if remaining <= threshold_value:
				logger.warning(f"Rate limit approaching for {resource_name}: {remaining}/{limit} remaining, resets at {datetime.fromtimestamp(reset_time)}")
				return True

		# No limits hit
		return False

	except Exception as e:
		logger.error(f"Error checking rate limits: {e}")
		# Return True as a precaution when we can't determine limits
		return True

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
			except Exception as e:
				logger.error(f"Failed to parse cache data: {e}")
	if args.nodl:
		logger.warning(f"Skipping API call for {repo_name} due to --nodl flag, returning cached data if available {type(cache_data)} {len(cache_data) if cache_data else 0}")
		if args.debug:
			logger.debug(f"repo: {repo_name} cache_entry: {cache_entry}")
		return cache_data[0]
	auth = await get_auth_params()
	if not auth:
		logger.error('update_repo_cache: no auth provided')
		return None

	api_url = f'https://api.github.com/repos/{repo_name}'

	if await is_rate_limit_hit(args):
		logger.warning(f"Rate limit hit for repository {repo_name}, returning cached data if available")
		await asyncio.sleep(1)
		return None

	# Fetch repository data from GitHub API
	try:
		# async with aiohttp.ClientSession() as api_session:
		async with get_client_session(args) as api_session:
			async with api_session.get(api_url) as r:
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
					logger.error(f"Rate limit exceeded for repository {repo_name}: {r.status} xratelimits: {r.headers.get('X-RateLimit-Used')}/{r.headers.get('X-RateLimit-Remaining')}/{r.headers.get('X-RateLimit-Limit')} 'X-RateLimit-Reset': {ratelimit_reset}")
					# rtext: {rtext}
					# logger.error(f"")
					# logger.error(f'rheaders: {r.headers}')
					# Handle rate limiting by returning cached data if available
					if cache_data:
						await asyncio.sleep(1)  # Wait before retrying
						return cache_data[0]
					else:
						logger.warning(f"No cached data available for {repo_name} after rate limit exceeded")
						await asyncio.sleep(1)  # Wait before retrying
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
					if args.debug:
						logger.debug(f"Added default data for unavailable repository to cache: {repo_name}")

					return default_repo_data
				else:
					logger.error(f"Failed to fetch repository data: {r.status} {await r.text()} api_url: {api_url}")
					return None
	except RateLimitExceededError as e:
		logger.warning(f"Rate limit exceeded while fetching repository {repo_name}: {e} api_url: {api_url}")
		return None
	except aiohttp.client_exceptions.ClientConnectorError as e:
		logger.error(f"Rate limit exceeded while fetching repository {repo_name}: {e} api_url: {api_url}")
		return None
	except Exception as e:
		logger.error(f"Error fetching repository data: {e} {type(e)} api_url: {api_url}")
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
		entry.data = data
		entry.timestamp = datetime.now()
		entry.last_scan = datetime.now()
	else:
		entry = CacheEntry(cache_key, cache_type, data)
		session.add(entry)

	# If cache_type is repo_data, expand JSON and store in RepoCacheExpanded
	if cache_type == "repo_data":
		try:
			repos = json.loads(data)
			if isinstance(repos, dict):
				repos = [repos]
			for repo_json in repos:
				if repo_json is None:
					continue  # Skip None entries
				expanded = session.query(RepoCacheExpanded).filter_by(repo_id=repo_json.get("id")).first()
				if expanded:
					# Update existing record with proper handling of None values
					for k, v in repo_json.items():
						if hasattr(expanded, k):
							# Skip updating fields that would be None for unavailable repos
							if k == 'id' and v is None:
								continue  # Don't update primary key with None
							# Convert datetime strings to datetime objects for specific fields
							elif k in ['created_at', 'updated_at', 'pushed_at'] and isinstance(v, str):
								v = ensure_datetime(v)
							# Handle topics conversion
							elif k == 'topics' and isinstance(v, list):
								v = ",".join(v)
							# Handle nested owner fields
							elif k == 'owner' and isinstance(v, dict):
								expanded.owner_login = v.get('login')
								expanded.owner_id = v.get('id')
								expanded.owner_node_id = v.get('node_id')
								expanded.owner_avatar_url = v.get('avatar_url')
								continue
							# Handle license fields
							elif k == 'license' and isinstance(v, dict):
								expanded.license_key = v.get('key')
								expanded.license_name = v.get('name')
								expanded.license_url = v.get('url')
								continue
							setattr(expanded, k, v)
				else:
					# Only create new records for repos that have valid IDs
					if repo_json.get("id") is not None:
						expanded = RepoCacheExpanded(repo_json)
						session.add(expanded)
					else:
						logger.warning(f"Skipping RepoCacheExpanded creation for repo with no ID: {repo_json.get('full_name')}")
		except Exception as e:
			logger.error(f"Failed to expand repo_data cache: {e} {type(e)}")
			logger.error(f"Data: {data}")
			logger.error(f"Traceback: {traceback.format_exc()}")
			# Roll back the session to recover from the error
			session.rollback()
			return None

	try:
		session.commit()
	except sqlite3.IntegrityError as e:
		if "UNIQUE constraint failed" in str(e):
			# Handle race condition - another process may have inserted the same key
			logger.warning(f"Cache key already exists, attempting update: {cache_key}")
			session.rollback()
			# Try to update existing entry
			existing_entry = get_cache_entry(session, cache_key, cache_type)
			if existing_entry:
				existing_entry.data = data
				existing_entry.timestamp = datetime.now()
				existing_entry.last_scan = datetime.now()
				try:
					session.commit()
					logger.info(f"Successfully updated existing cache entry: {cache_key}")
					return existing_entry
				except Exception as update_error:
					logger.error(f"Failed to update existing cache entry: {update_error}")
					session.rollback()
					return None
			else:
				logger.error(f"Cache entry disappeared during update attempt: {cache_key}")
				return None
		else:
			logger.error(f"IntegrityError while committing cache entry: {e}")
			session.rollback()
			return None
	except Exception as e:
		logger.error(f"Failed to commit cache entry: {e}")
		logger.error(f"Traceback: {traceback.format_exc()}")
		session.rollback()
		return None

	return entry
