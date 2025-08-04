#!/usr/bin/python3
import asyncio
import aiohttp
import json
import os
from loguru import logger
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import datetime
from typing import Dict, List
from collections import defaultdict
from cacheutils import get_cache_entry, set_cache_entry, is_rate_limit_hit
from utils import get_client_session, get_auth_params

async def get_starred_repos_by_list(session, args) -> Dict[str, List[dict]]:
	"""
	Get starred repositories grouped by list name

	Returns:
		Dict[str, List[dict]]: Dictionary with list names as keys and lists of repos as values
	"""
	try:
		try:
			# Get all lists first
			git_lists = await get_git_list_stars(session, args)
			if not git_lists:
				logger.warning("No GitHub lists found")
				return {}
		except Exception as e:
			logger.error(f"Error getting starred repos by list: {e}")
			return {}
		try:
			# Get all starred repos
			starred_repos = await fetch_github_starred_repos(args, session)
			if not starred_repos:
				logger.warning("No starred repositories found")
				return {}
		except Exception as e:
			logger.error(f"Error getting starred repos by list: {e}")
			return {}
		try:
			# Create lookup dict for starred repos by full name
			repo_lookup = {repo['full_name']: repo for repo in starred_repos}
			# Group repos by list
			grouped_repos = defaultdict(list)
		except Exception as e:
			logger.error(f"Error getting starred repos by list: {e}")
			return {}

		for list_name, list_data in git_lists.items():
			try:
				for href in list_data['hrefs']:
					# Convert href to full_name format (owner/repo)
					full_name = href.strip('/').split('github.com/')[-1]
					if full_name in repo_lookup:
						grouped_repos[list_name].append(repo_lookup[full_name])
					else:
						logger.warning(f"Repository {full_name} found in list but not in starred repos")
			except Exception as e:
				logger.error(f"Error getting starred repos by list: {e}")
				return {}
		# Add "Uncategorized" list for repos not in any list
		# in_lists = {repo for repos in grouped_repos.values() for repo in repos}
		in_list_names = {repo['full_name'] for repos in grouped_repos.values() for repo in repos}
		try:
			uncategorized = []
			# [repo for repo in starred_repos if repo['full_name'] not in {r['full_name'] for r in list(in_list_names)}]
			for repo in starred_repos:
				if repo['full_name'] not in in_list_names:
					uncategorized.append(repo)
			if uncategorized:
				grouped_repos["Uncategorized"] = uncategorized
		except Exception as e:
			logger.error(f"Error getting starred repos by list: {e} {type(e)}")
			# return {}

		# Convert defaultdict to regular dict and sort repos in each list
		result = dict(grouped_repos)
		for list_name in result:
			result[list_name].sort(key=lambda x: x.get('updated_at', ''), reverse=True)

		return result

	except Exception as e:
		logger.error(f"Error getting starred repos by list: {e}")
		return {}

async def get_git_stars(args, session):
	"""
	Get all starred repos with caching support from database
	"""
	cache_key = "starred_repos_list"
	cache_type = "starred_repos"

	jsonbuffer = []

	# Try to load from cache first if use_cache is enabled
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				jsonbuffer = json.loads(cache_entry.data)
				logger.info(f"Loaded {len(jsonbuffer)} starred repos from database cache")
				return jsonbuffer  # Return cached data if successful
			except json.JSONDecodeError as e:
				logger.error(f"Invalid JSON in cache entry: {e}")
			except Exception as e:
				logger.error(f"Error loading from cache: {e}")
		else:
			logger.warning("[get_git_stars]] no cache entry found in database for starred repos")

		# If we reach here, either no cache or cache failed - download fresh data
		logger.info("Downloading fresh starred repos data from GitHub API...")
		git_starred_repos = await fetch_github_starred_repos(args, session)

		# Store the downloaded data in the database cache
		if git_starred_repos:
			try:
				cache_obj = json.dumps(git_starred_repos)
				set_cache_entry(session, cache_key, cache_type, cache_obj)
				session.commit()
				logger.info(f"Stored {len(git_starred_repos)} starred repos in database cache")
			except Exception as e:
				logger.error(f"Failed to store data in cache: {e}")
		if args.global_limit > 0:
			# Limit the number of repos returned based on global limit
			git_starred_repos = git_starred_repos[:args.global_limit]
			logger.warning(f'Global limit set to {args.global_limit}, returning only first {len(git_starred_repos)} repos')
		return git_starred_repos
	else:
		# Cache not enabled - download directly and optionally store
		logger.info("Cache not enabled, downloading starred repos from GitHub API...")
		git_starred_repos = await fetch_github_starred_repos(args, session)

		# Even if cache is disabled, we might want to store for future use
		# Comment out the next block if you don't want to store when cache is disabled
		if git_starred_repos:
			try:
				cache_obj = json.dumps(git_starred_repos)
				set_cache_entry(session, cache_key, cache_type, cache_obj)
				session.commit()
				logger.info(f"Stored {len(git_starred_repos)} starred repos in database cache (cache disabled but stored anyway)")
			except Exception as e:
				logger.error(f"Failed to store data in cache: {e}")

		if args.global_limit > 0:
			# Limit the number of repos returned based on global limit
			git_starred_repos = git_starred_repos[:args.global_limit]
			logger.warning(f'Global limit set to {args.global_limit}, returning only first {len(git_starred_repos)} repos')
		return git_starred_repos

async def download_git_stars(args, session):
	# If we get here, we need to fetch from the API
	jsonbuffer = []
	if args.nodl:
		logger.warning('Skipping API call due to --nodl flag')
		return jsonbuffer

	auth = await get_auth_params()
	if not auth:
		logger.error('no auth provided')
		return None

	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	cache_key = "starred_repos_list"
	cache_type = "starred_repos"

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return None

	async with get_client_session(args) as api_session:
		try:
			# First, get the first page to determine total pages
			async with api_session.get(apiurl, headers=headers) as r:
				if r.status == 401:
					logger.error(f"[r] autherr:401 a:{auth}")
					return jsonbuffer
				elif r.status == 404:
					logger.warning(f"[r] {r.status} {apiurl} not found")
					return jsonbuffer
				elif r.status == 403:
					logger.warning(f"[r] {r.status} {apiurl} API rate limit exceeded")
					return jsonbuffer
				elif r.status == 200:
					data = await r.json()
					jsonbuffer.extend(data)

					# Save first page immediately
					if jsonbuffer:
						cache_obj = json.dumps(jsonbuffer)
						set_cache_entry(session, cache_key, cache_type, cache_obj)
						session.commit()

					# Determine total pages from Link header
					last_page_no = 1
					if 'link' in r.headers:
						links = r.headers['link'].split(',')
						for link in links:
							if 'last' in link:
								lasturl = link.split('>')[0].replace('<','')
								last_page_no = int(lasturl.split('=')[-1])
								break

					# If only one page or max_pages is 1, return early
					if last_page_no == 1 or args.max_pages == 1:
						logger.info(f"Downloaded {len(jsonbuffer)} starred repos (single page)")
						return jsonbuffer

					# Determine how many pages to fetch
					max_pages_to_fetch = last_page_no
					if args.max_pages > 0:
						max_pages_to_fetch = min(args.max_pages, last_page_no)
					if args.global_limit > 0:
						max_pages_to_fetch = min(args.global_limit, max_pages_to_fetch)
						logger.warning(f'Global limit set to {args.global_limit}, adjusting max pages to fetch: {max_pages_to_fetch}')

					logger.info(f"Found {last_page_no} total pages, fetching pages 2-{max_pages_to_fetch} concurrently")

					# Create tasks for remaining pages (starting from page 2)
					tasks = []
					semaphore = asyncio.Semaphore(5)  # Limit concurrent requests to avoid rate limiting
					stop_signal = asyncio.Event()

					# Create tasks for pages 2 through max_pages_to_fetch
					for page_num in range(2, max_pages_to_fetch + 1):
						# tasks.append(fetch_page(page_num))
						tasks.append(fetch_page_generic(api_session, apiurl, page_num, headers, semaphore, args, max_pages_to_fetch, stop_signal))

					# Execute all page requests concurrently
					if tasks:
						logger.info(f"Starting concurrent download of {len(tasks)} pages...")
						page_results = await asyncio.gather(*tasks, return_exceptions=True)

						# Sort results by page number and extend jsonbuffer
						successful_pages = []
						for result in page_results:
							if isinstance(result, Exception):
								logger.error(f"Page fetch failed: {result}")
								continue

							page_num, page_data = result
							if page_data:
								successful_pages.append((page_num, page_data))

						# Sort by page number to maintain order
						successful_pages.sort(key=lambda x: x[0])

						# Add all page data to buffer
						for page_num, page_data in successful_pages:
							jsonbuffer.extend(page_data)

						logger.info(f"Concurrent download completed. Total repos: {len(jsonbuffer)}")

				else:
					logger.error(f"Initial request failed with status {r.status}")
					return jsonbuffer

		except Exception as e:
			logger.error(f"[r] {e}")
			return jsonbuffer

	# Final cache write with all data
	if jsonbuffer:
		try:
			cache_obj = json.dumps(jsonbuffer)
			set_cache_entry(session, cache_key, cache_type, cache_obj)
			session.commit()
			logger.info(f"Cached {len(jsonbuffer)} starred repos in database")
		except Exception as e:
			logger.error(f"Failed to write final cache: {e}")

	return jsonbuffer

async def get_list_members(args, list_url) -> List[dict]:
	"""
	Get members of a GitHub starred repository list by scraping the list page.

	Args:
		args: Command line arguments
		list_url: URL of the GitHub starred repository list

	Returns:
		List[dict]: List of dictionaries with repository details
	"""
	members = []
	if args.nodl:
		logger.warning("Skipping API call due to --nodl flag")
		return members

	async with aiohttp.ClientSession() as api_session:
		if args.debug:
			logger.debug(f"Fetching list members from {list_url}")
		async with api_session.get(list_url) as r:
			if r.status == 200:
				content = await r.text()
	soup = BeautifulSoup(content, 'html.parser')
	# repo_items = soup.find_all('div', class_='Box-row d-flex flex-items-center p-3')
	data_soup = soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"})
	list_soup = data_soup.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
	list_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in list_soup]

	for item in list_soup:
		h3_tag = item.find('h3')
		a_tag = h3_tag.find('a') if h3_tag else None
		repo_url = f"https://github.com{a_tag['href']}" if a_tag else ''
		repo_full_text = a_tag.get_text(strip=True) if a_tag else ''
		owner = ''
		repo_name = ''
		if a_tag:
			owner_span = a_tag.find('span', class_='text-normal')
			if owner_span:
				owner = owner_span.get_text(strip=True).replace('/', '').strip()
				repo_name = repo_full_text.replace(owner_span.get_text(), '').strip()
			else:
				# fallback: parse from href
				parts = a_tag['href'].strip('/').split('/')
				if len(parts) >= 2:
					owner = parts[0]
					repo_name = parts[1]

		# repo_name = item.find('a', class_='Link--primary').get_text(strip=True)
		repo_desc = item.find('p', class_='f6 color-fg-muted my-1').get_text(strip=True) if item.find('p', class_='f6 color-fg-muted my-1') else ''
		# repo_url = f"https://github.com{item.find('a', class_='Link--primary')['href']}"
		members.append({
			'name': repo_name,
			'description': repo_desc,
			'url': repo_url
		})
	if args.debug:
		logger.debug(f"Found {len(members)} members in list {list_url}")
	return members

async def get_lists(args, session) -> dict:
	"""
	Fetches the user's GitHub starred repository lists by scraping the stars page.
	"""

	cache_key = "git_lists_metadata"
	cache_type = "list_metadata"

	# Try cache first if enabled
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				cached_data = json.loads(cache_entry.data)
				if args.debug:
					logger.debug(f"Using cached git lists data: {len(cached_data)} lists")
				return cached_data
			except (json.JSONDecodeError, AttributeError) as e:
				logger.warning(f"Failed to load cached git lists data: {e}")

	if args.nodl:
		logger.warning("Skipping API call for git lists due to --nodl flag")
		return []

	git_lists = []
	auth = await get_auth_params()
	if not auth:
		logger.error("No authentication provided for get_lists")
		return []

	listurl = f'https://github.com/{auth.username}?tab=stars'

	async with aiohttp.ClientSession() as api_session:
		async with api_session.get(listurl) as response:
			content = await response.text()

	soup = BeautifulSoup(content, 'html.parser')
	souplist = soup.find_all('a',class_="d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline")

	for sl in souplist:
		list_info = {
			'name': sl.find('span', class_='text-normal').text.strip() if sl.find('span', class_='text-normal') else 'Unknown',
			'description': sl.find('p', class_='color-fg-muted').text.strip() if sl.find('p', class_='color-fg-muted') else '',
			'list_url': sl.get('href', ''),
			'repo_count': sl.find('span', class_='Counter').text.strip() if sl.find('span', class_='Counter') else '0'
		}
		git_lists.append(list_info)

	# Cache the results
	if git_lists:
		set_cache_entry(session, cache_key, cache_type, json.dumps(git_lists))

	return git_lists

async def get_git_list_stars(session, args) -> dict:
	"""
	Get lists of starred repos using database cache
	"""
	cache_key = "git_list_stars"
	cache_type = "list_stars"

	auth = await get_auth_params()
	if not auth:
		logger.error('get_git_list_stars: no auth provided')
		return {}

	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}','X-GitHub-Api-Version': '2022-11-28'}
	soup = None
	cache_entry = None
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				soup = BeautifulSoup(cache_entry.data, 'html.parser')
				if args.debug:
					logger.debug("Loaded star list from database cache")
			except Exception as e:
				logger.error(f'Failed to parse cached star list: {e} {type(e)} {cache_key} not found in database cache type {cache_type}')
		else:
			logger.warning(f'Failed to parse cached star list: {cache_key} not found in database cache type {cache_type}')
	if args.nodl:
		logger.warning("Skipping API call due to --nodl flag")
		return {}
	if not soup or len(soup) == 0:
		if await is_rate_limit_hit(args):
			logger.warning("Rate limit hit!")
			await asyncio.sleep(1)
			return None

		async with aiohttp.ClientSession() as api_session:
			if args.debug:
				logger.debug(f"Fetching star list from {listurl}")
			async with api_session.get(listurl) as r:
				if r.status == 200:
					content = await r.text()
					soup = BeautifulSoup(content, 'html.parser')
					# Save to database cache
					set_cache_entry(session, cache_key, cache_type, str(soup))
					session.commit()
					logger.info("Saved star list to database cache")
				else:
					logger.error(f"Failed to fetch star list: {r.status} {listurl}")
					return {}

	listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})
	if len(listsoup) == 0:
		logger.warning("No lists found in soup")
		return {}
	try:
		list_items = listsoup[0].find_all('a', attrs={'class':'d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})
	except (TypeError, IndexError) as e:
		logger.error(f'Failed to find list items in soup {e} {type(e)} from listsoup: {type(listsoup)} {len(listsoup)}')
		return {}

	lists = {}
	for item in list_items:
		listname = item.find('h3').text
		list_link = f"https://github.com{item.attrs['href']}"
		list_count_info = item.find('div', class_="color-fg-muted text-small no-wrap").text

		try:
			list_description = item.select('span', class_="Truncate-text color-fg-muted mr-3")[1].text.strip()
		except IndexError:
			list_description = ''

		try:
			list_repos = await get_info_for_list(list_link, headers, session, args)
		except Exception as e:
			logger.warning(f'{e} {type(e)} failed to get list info for {listname}')
			list_repos = []

		lists[listname] = {
			'href': list_link,
			'count': list_count_info,
			'description': list_description,
			'hrefs': list_repos
		}
	return lists

async def get_info_for_list(link, headers, session, args):
	"""
	Get all repository hrefs from a GitHub list, handling pagination.
	Uses database caching to avoid repeated scraping.
	"""
	cache_key = f"list_info:{link.split('/')[-1]}"
	cache_type = "list_info"

	# Try cache first
	if args.use_cache:
		if cache_entry := get_cache_entry(session, cache_key, cache_type):
			try:
				cached_data = json.loads(cache_entry.data)
				logger.debug(f"Loaded list info from cache for {link}")
				return cached_data
			except json.JSONDecodeError as e:
				logger.error(f'Failed to parse cached list info: {e}')

	if args.nodl:
		logger.warning(f"Skipping API call for {link} due to --nodl flag")
		return []

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit, skipping fetch for list.")
		await asyncio.sleep(1)
		return []

	all_hrefs = []
	current_url = link
	page_num = 1

	async with get_client_session(args) as api_session:
		while current_url and page_num <= 100:  # Safety limit
			try:
				async with api_session.get(current_url, headers=headers) as r:
					if r.status != 200:
						logger.error(f"Failed to fetch {current_url}: Status {r.status}")
						break

					soup = BeautifulSoup(await r.text(), 'html.parser')

					# Extract repository links using a more specific selector
					repo_links = soup.select('#user-list-repositories .d-inline-block.mb-1 a')
					page_hrefs = [link['href'] for link in repo_links if 'href' in link.attrs]

					if not page_hrefs:
						logger.debug(f"No more repos found on page {page_num} of {link}.")
						break

					all_hrefs.extend(page_hrefs)
					logger.debug(f"Page {page_num}: found {len(page_hrefs)} repos, total: {len(all_hrefs)}")

					# Find next page link
					next_link = soup.select_one('a.next_page, a[rel=next]')
					current_url = f"https://github.com{next_link['href']}" if next_link else None
					page_num += 1

			except Exception as e:
				logger.error(f"Error scraping {current_url}: {e}")
				break

	if page_num > 100:
		logger.warning(f"Reached maximum page limit (100) for {link}")

	# Cache the complete results
	if all_hrefs:
		try:
			set_cache_entry(session, cache_key, cache_type, json.dumps(all_hrefs))
			session.commit()
			logger.info(f"Cached {len(all_hrefs)} repo links for {link}")
		except Exception as e:
			logger.error(f'Failed to save list info to cache: {e}')

	return all_hrefs

async def fetch_starred_repos(args, session):
	"""
	Fetch user's starred repositories from GitHub API with database caching and concurrent page downloads
	"""
	cache_key = "starred_repos_fetch"
	cache_type = "starred_repos"

	# Check cache first if enabled
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				cache_age = datetime.datetime.now() - cache_entry.timestamp
				# Use cache if it's less than 1 day old
				if cache_age.days < 1:
					cache_data = json.loads(cache_entry.data)
					logger.info(f"Using cached starred repos ({len(cache_data)} items)")
					return cache_data
			except Exception as e:
				logger.warning(f"Failed to load cache: {e}")
		else:
			logger.warning("[fetch_starred_repos] No cache entry found in database for starred repos")

	if args.nodl:
		logger.warning("Skipping API call due to --nodl flag")
		return []

	# Get authentication
	auth = await get_auth_params()
	if not auth:
		logger.error("No GitHub authentication available")
		return []

	# API request setup
	api_url = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	per_page = 100  # Max allowed by GitHub API

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return None

	async with get_client_session(args) as api_session:
		# First, get the first page to determine total pages
		first_page_url = f"{api_url}?page=1&per_page={per_page}"

		try:
			async with api_session.get(first_page_url, headers=headers) as response:
				if response.status == 401:
					logger.error("Authentication failed - check your token")
					return []
				elif response.status == 403:
					reset_time = response.headers.get('X-RateLimit-Reset')
					if reset_time:
						reset_datetime = datetime.datetime.fromtimestamp(int(reset_time))
						wait_time = (reset_datetime - datetime.datetime.now()).total_seconds()
						logger.warning(f"Rate limit exceeded. Resets in {wait_time:.1f} seconds")
					else:
						logger.warning("Rate limit exceeded")
					return []
				elif response.status != 200:
					logger.error(f"API request failed with status {response.status}")
					error_data = await response.text()
					logger.warning(f"Error response: {error_data}")
					return []

				first_page_data = await response.json()
				repos = list(first_page_data)  # Start with first page data

				# Determine total pages from Link header - same logic as download_git_stars
				last_page_no = 1
				if 'link' in response.headers:
					links = response.headers['link'].split(',')
					for link in links:
						if 'last' in link:
							lasturl = link.split('>')[0].replace('<','')
							last_page_no = int(lasturl.split('=')[-1])
							break

				# If only one page or max_pages is 1, return early
				if last_page_no == 1 or args.max_pages == 1:
					logger.info(f"Downloaded {len(repos)} starred repos (single page)")
				else:
					# Determine how many pages to fetch
					max_pages_to_fetch = last_page_no
					if args.max_pages > 0:
						max_pages_to_fetch = min(args.max_pages, last_page_no)
					if args.global_limit > 0:
						max_pages_to_fetch = min(args.global_limit, max_pages_to_fetch)
						logger.warning(f'Global limit set to {args.global_limit}, adjusting max pages to fetch: {max_pages_to_fetch}')
					logger.info(f"Found {last_page_no} total pages, fetching pages 2-{max_pages_to_fetch} concurrently")

					# Create tasks for remaining pages (starting from page 2)
					semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
					stop_signal = asyncio.Event()

					# Create tasks for pages 2 through max_pages_to_fetch
					tasks = []
					for page_num in range(2, max_pages_to_fetch + 1):
						# tasks.append(fetch_page(page_num))
						tasks.append(fetch_page_generic(api_session, api_url, page_num, headers, semaphore, args, max_pages_to_fetch, stop_signal))

					# Execute all page requests concurrently
					if tasks:
						logger.info(f"Starting concurrent download of {len(tasks)} pages...")
						page_results = await asyncio.gather(*tasks, return_exceptions=True)

						# Process results and add to repos list
						successful_pages = []
						for result in page_results:
							if isinstance(result, Exception):
								logger.error(f"Page fetch failed: {result}")
								continue

							page_num, page_data = result
							if page_data:
								successful_pages.append((page_num, page_data))

						# Sort by page number to maintain order
						successful_pages.sort(key=lambda x: x[0])

						# Add all page data to repos list
						for page_num, page_data in successful_pages:
							repos.extend(page_data)
							if args.debug:
								logger.debug(f"Added page {page_num} data, total repos: {len(repos)}")

						logger.info(f"Concurrent download completed. Total repos: {len(repos)}")

		except Exception as e:
			logger.error(f"Request error: {e} {type(e)}")
			return []

	# Cache the results if we got data
	if repos:
		try:
			set_cache_entry(session, cache_key, cache_type, json.dumps(repos))
			session.commit()
			logger.info(f"Cached {len(repos)} starred repositories in database")
		except Exception as e:
			logger.error(f"Failed to write cache: {e}")
	else:
		logger.warning("No starred repositories found or fetched")

	logger.info(f"Fetched {len(repos)} starred repositories from GitHub API")
	return repos

async def fetch_page_generic(api_session, base_url, page_num, headers, semaphore, args, max_pages_to_fetch=None, stop_signal=None):
	"""
	Generic function to fetch a single page from GitHub API

	Args:
		api_session: The aiohttp session
		base_url: Base API URL (without page parameter)
		page_num: Page number to fetch
		headers: HTTP headers for the request
		semaphore: Asyncio semaphore for rate limiting
		args: Command line arguments for debug logging
		max_pages_to_fetch: Total pages being fetched (for logging)
		stop_signal: Asyncio Event to signal when to stop fetching

	Returns:
		Tuple of (page_num, page_data)
	"""

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return None

	async with semaphore:
		# Check if we should stop (other pages found empty results)
		if stop_signal and stop_signal.is_set():
			if args.debug:
				logger.warning(f"Skipping page {page_num} due to stop signal")
			return page_num, []

		# Handle different URL formats
		if '?' in base_url:
			page_url = f"{base_url}&page={page_num}"
		else:
			page_url = f"{base_url}?page={page_num}"

		# Add per_page parameter for fetch_starred_repos
		if 'per_page' not in page_url and 'user/starred' in base_url:
			page_url += "&per_page=100"

		try:
			if args.debug:
				if max_pages_to_fetch:
					logger.debug(f"Fetching page {page_num}/{max_pages_to_fetch} from {page_url}")
				else:
					logger.debug(f"Fetching page {page_num} from {page_url}")

			async with api_session.get(page_url, headers=headers) as page_response:
				if page_response.status == 200:
					page_data = await page_response.json()

					# If we get an empty page, signal other tasks to stop
					if len(page_data) == 0:
						if stop_signal:
							stop_signal.set()
						logger.warning(f"Page {page_num}: got 0 repos - signaling stop")
						return page_num, []

					if args.debug:
						logger.debug(f"Page {page_num}: got {len(page_data)} repos")
					return page_num, page_data
				elif page_response.status == 403:
					logger.warning(f"Rate limit hit on page {page_num}")
					return page_num, []
				else:
					logger.warning(f"Page {page_num} failed with status {page_response.status}")
					return page_num, []
		except Exception as e:
			logger.error(f"Error fetching page {page_num}: {e}")
			return page_num, []

async def fetch_github_starred_repos(args, session, cache_key="starred_repos_list", cache_type="starred_repos"):
	"""
	Unified function to fetch starred repos from GitHub API, with cache and pagination.
	Returns a list of starred repo dicts.
	"""
	# Try cache first
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				cached_data = json.loads(cache_entry.data)
				logger.info(f"Loaded {len(cached_data)} starred repos from cache")
				return cached_data
			except Exception as e:
				logger.error(f"Failed to parse cache: {e}")

	if args.nodl:
		logger.warning("Skipping API call due to --nodl flag")
		return []

	auth = await get_auth_params()
	if not auth:
		logger.error("No GitHub authentication available")
		return []

	api_url = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}
	per_page = 100
	repos = []

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return []

	async with get_client_session(args) as api_session:
		# Fetch first page
		first_page_url = f"{api_url}?page=1&per_page={per_page}"
		async with api_session.get(first_page_url, headers=headers) as response:
			if response.status != 200:
				logger.error(f"API request failed with status {response.status}")
				return []
			first_page_data = await response.json()
			repos.extend(first_page_data)
			# Pagination
			last_page_no = 1
			if 'link' in response.headers:
				links = response.headers['link'].split(',')
				for link in links:
					if 'last' in link:
						lasturl = link.split('>')[0].replace('<','')
						last_page_no = int(lasturl.split('=')[-1])
						break
			# If only one page, return
			if last_page_no == 1 or args.max_pages == 1:
				logger.info(f"Downloaded {len(repos)} starred repos (single page)")
			else:
				max_pages_to_fetch = min(last_page_no, args.max_pages) if args.max_pages > 0 else last_page_no
				if args.global_limit > 0:
					max_pages_to_fetch = min(args.global_limit, max_pages_to_fetch)
				logger.info(f"Fetching pages 2-{max_pages_to_fetch} concurrently")
				semaphore = asyncio.Semaphore(5)
				stop_signal = asyncio.Event()
				tasks = [
					fetch_page_generic(api_session, api_url, page_num, headers, semaphore, args, max_pages_to_fetch, stop_signal)
					for page_num in range(2, max_pages_to_fetch + 1)
				]
				page_results = await asyncio.gather(*tasks, return_exceptions=True)
				successful_pages = []
				for result in page_results:
					if isinstance(result, Exception):
						logger.error(f"Page fetch failed: {result}")
						continue
					page_num, page_data = result
					if page_data:
						successful_pages.append((page_num, page_data))
				successful_pages.sort(key=lambda x: x[0])
				for page_num, page_data in successful_pages:
					repos.extend(page_data)
				logger.info(f"Concurrent download completed. Total repos: {len(repos)}")

	# Cache results
	if repos:
		set_cache_entry(session, cache_key, cache_type, json.dumps(repos))
		session.commit()
		logger.info(f"Cached {len(repos)} starred repos in database")
	return repos
