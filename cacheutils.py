#!/usr/bin/python3
import traceback
import aiohttp
import json
import sqlite3
from sqlalchemy.exc import OperationalError
from loguru import logger
from datetime import datetime
from dbstuff import CacheEntry, BLANK_REPO_DATA, RepoCacheExpanded
from utils import get_client_session, ensure_datetime, get_auth_params, get_semaphore

class RateLimitExceededError(Exception):
	"""Custom exception for rate limit exceeded errors"""
	pass

async def get_api_rate_limits(args) -> dict:
	# Fetch GitHub API rate limits and return them as a dictionary

	rate_limits = {'limit_hit':False, 'rate_limits': {}}
	rates = {}
	try:
		# async with aiohttp.ClientSession() as api_session:
		async with get_client_session(args) as api_session:
			async with api_session.get('https://api.github.com/rate_limit') as r:
				rates = await r.json()
	except aiohttp.ContentTypeError as e:
		logger.error(f"ContentTypeError while fetching rate limits: {e}")
		rate_limits['limit_hit'] = True
		rate_limits['rate_limits'] = {'error': 'ContentTypeError', 'message': str(e)}
		return rate_limits
	except Exception as e:
		logger.error(f'fatal {e} {type(e)}')
		logger.error(f'traceback: {traceback.format_exc()}')
		raise e
	rate_limits['rate_limits'] = rates
	if rate_limits.get('rate_limits', {}).get('status') == 401:
		logger.error(f"Unauthorized access (401) when fetching rate limits - check your authentication. {rate_limits.get('rate_limits','').get('message')}")
		rate_limits['limit_hit'] = True
	return rate_limits

async def is_rate_limit_hit(args, threshold_percent=10) -> bool:
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
		if rate_limits_data:
			# Check if limit_hit is already set
			if rate_limits_data.get('limit_hit', False):
				return True
			if rate_limits_data.get('rate_limits', {}).get('status') == '401':
				logger.error(f"Unauthorized access (401) when checking rate limits - check your authentication. {rate_limits_data}")
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
			if args.debug:
				# logger.debug(f"Rate limits checked core: {resources} graphql: {resources} ")
				try:
					logger.debug(f"Rate limits checked core: {resources.get('core').get('used')}/{resources.get('core').get('remaining')} graphql: {resources.get('graphql').get('used')}/{resources.get('graphql').get('remaining')}")
				except Exception as e:
					logger.error(f"Error logging rate limits: {e} {type(e)} resources: {resources} rate_limits_data: {rate_limits_data}")

			# print(resources
				# print(resources)
			# No limits hit
			return False

	except Exception as e:
		logger.error(f"Error checking rate limits: {e} {type(e)}")
		logger.error(f'traceback: {traceback.format_exc()}')
		# Return True as a precaution when we can't determine limits
		return True
	return False

async def update_repo_cache(repo_name_or_url, session, args) -> dict | None:
	"""
	Fetch repository data from GitHub API and update the database cache
	"""
	if 'BLANK_REPO_DATA' in repo_name_or_url:
		logger.warning(f"BLANK_REPO_DATA repo_name_or_url: {repo_name_or_url}")
	semaphore = get_semaphore()
	async with semaphore:
		# Normalize repo name
		repo_name = repo_name_or_url.strip('/').replace('github.com/', '').replace('.git', '')
		cache_key = f"repo:{repo_name}"
		cache_type = "repo_data"
		# Try cache first
		if args.use_cache:
			cache_entry = get_cache_entry(session, cache_key, cache_type)
			if cache_entry:
				try:
					cache_data = json.loads(cache_entry.data)
					return cache_data[0] if cache_data else None
				except Exception as e:
					logger.error(f"Failed to parse cache data: {e} {type(e)} for {repo_name_or_url}")
					logger.error(f'traceback: {traceback.format_exc()}')
		auth = await get_auth_params()
		if not auth:
			logger.error('update_repo_cache: no auth provided')
			return None
		api_url = f'https://api.github.com/repos/{repo_name}'
		try:
			async with get_client_session(args) as api_session:
				async with api_session.get(api_url) as r:
					if r.status == 200:
						repo_data = await r.json()
						try:
							set_cache_entry(session, cache_key, cache_type, json.dumps([repo_data]))
						except TimeoutError as e:
							logger.error(f"Failed to set cache entry for {repo_name}: {e} {type(e)}")
							if args.debug:
								logger.error(f'traceback: {traceback.format_exc()}')
							return None
						except Exception as e:
							logger.error(f"Fatal Failed to set cache entry for {repo_name}: {e} {type(e)}")
							if args.debug:
								logger.error(f'traceback: {traceback.format_exc()}')
							raise e
						session.commit()
						return repo_data
					elif r.status in (403, 404, 451):
						logger.warning(f"Repository error {r.status}: {api_url}")
						return None
						# default_repo_data = BLANK_REPO_DATA.copy()
						# default_repo_data['name'] = repo_name
						# set_cache_entry(session, cache_key, cache_type, defaultjson)
						# session.commit()
						# return default_repo_data
					elif r.status == 401:
						default_repo_data = BLANK_REPO_DATA.copy()
						default_repo_data['name'] = repo_name
						# defaultjson = json.dumps([default_repo_data])
						# set_cache_entry(session, cache_key, cache_type, defaultjson)
						# session.commit()
						logger.error(f"Unauthorized access (401) to repository: {api_url}")
						return default_repo_data
					else:
						logger.error(f"Failed to fetch repository data: {r.status} from {api_url}")
						return None
		except TimeoutError as e:
			logger.error(f"TimeoutError fetching repository data: {e} {type(e)}")
			# if args.debug:
			# 	logger.error(f'traceback: {traceback.format_exc()}')
			return None
		except Exception as e:
			logger.error(f"Fatal Error fetching repository data: {e} {type(e)}")
			if args.debug:
				logger.error(f'traceback: {traceback.format_exc()}')
			raise e

def get_cache_entry(session, cache_key, cache_type) -> CacheEntry | None:
	"""Get a cache entry from the database"""
	try:
		entry = session.query(CacheEntry).filter_by(cache_key=cache_key, cache_type=cache_type).first()
		return entry
	except OperationalError as e:
		logger.error(f"OperationalError while getting cache entry: {e} {type(e)}")
		return None
	except Exception as e:
		logger.error(f"Failed to get cache entry: {e} {type(e)}")
		# logger.error(f'traceback: {traceback.format_exc()}')
		return None

def set_cache_entry(session, cache_key, cache_type, data) -> CacheEntry | None:
	"""Set or update a cache entry in the database"""
	if 'BLANK_REPO_DATA' in data:
		logger.warning(f"Invalid data for cache entry: {json.loads(data)[0]["name"]} ")
		return None
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
		except OperationalError as e:
			logger.error(f"Failed to expand repo_data cache: {e} {type(e)}")
			session.rollback()
			return None
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
				except Exception as e:
					logger.error(f"Failed to update existing cache entry: {e} {type(e)}")
					# logger.error(f'traceback: {traceback.format_exc()}')
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
