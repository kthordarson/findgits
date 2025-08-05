#!/usr/bin/python3
import traceback
import asyncio
import json
from loguru import logger
from bs4 import BeautifulSoup
from cacheutils import get_cache_entry, set_cache_entry, is_rate_limit_hit
from utils import get_client_session, get_auth_params

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
				if args.debug:
					logger.debug(f"Loaded list info from cache for {link}")
				return cached_data
			except json.JSONDecodeError as e:
				logger.error(f'Failed to parse cached list info: {e}')

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
						if args.debug:
							logger.debug(f"No more repos found on page {page_num} of {link}.")
						break
					all_hrefs.extend(page_hrefs)
					# Find next page link
					next_link = soup.select_one('a.next_page, a[rel=next]')
					current_url = f"https://github.com{next_link['href']}" if next_link else None
					page_num += 1

			except Exception as e:
				logger.error(f"Error scraping {current_url}: {e} {type(e)}")
				logger.error(f'traceback: {traceback.format_exc()}')
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
			logger.error(f'Failed to save list info to cache: {e} {type(e)}')
			logger.error(f'traceback: {traceback.format_exc()}')

	return all_hrefs

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
			return page_num, []

		# Handle different URL formats
		if '?' in base_url:
			page_url = f"{base_url}&page={page_num}"
		else:
			page_url = f"{base_url}?page={page_num}"

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
			logger.error(f"Error fetching page {page_num}: {e} {type(e)}")
			logger.error(f'traceback: {traceback.format_exc()}')
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
				logger.error(f"Failed to parse cache: {e} {type(e)}")
				logger.error(f'traceback: {traceback.format_exc()}')

	auth = await get_auth_params()
	if not auth:
		logger.error("No GitHub authentication available")
		return []

	api_url = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',  # type: ignore
		'X-GitHub-Api-Version': '2022-11-28'
	}
	per_page = 100
	repos = []

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return []

	async with get_client_session(args) as api_session:
		# Fetch first page to determine total pages
		first_page_url = f"{api_url}?page=1&per_page={per_page}"
		async with api_session.get(first_page_url, headers=headers) as response:
			if response.status != 200:
				logger.error(f"API request failed with status {response.status}")
				return []
			first_page_data = await response.json()
			repos.extend(first_page_data)

			# Parse Link header to get last page number
			last_page_no = 1
			if 'link' in response.headers:
				links = response.headers['link'].split(',')
				for link in links:
					if 'rel="last"' in link:
						# Extract URL from <url>
						url_part = link.split('>')[0].replace('<', '').strip()
						# Extract page number from URL
						if 'page=' in url_part:
							try:
								last_page_no = int(url_part.split('page=')[1].split('&')[0])
							except (ValueError, IndexError):
								logger.warning(f"Could not parse last page from link header: {link}")
						break

			logger.info(f"First page fetched: {len(first_page_data)} repos. Total pages available: {last_page_no}")

			# If only one page, return early
			if last_page_no == 1:
				logger.info(f"Downloaded {len(repos)} starred repos (single page)")
				if repos:
					set_cache_entry(session, cache_key, cache_type, json.dumps(repos))
					session.commit()
				return repos

			# Calculate how many pages to fetch
			max_pages_to_fetch = last_page_no

			# Apply global_limit if set (but don't limit starred repos by default)
			if hasattr(args, 'global_limit') and args.global_limit > 0:
				# Convert global_limit from number of repos to number of pages
				pages_needed = (args.global_limit + per_page - 1) // per_page  # Round up
				max_pages_to_fetch = min(pages_needed, max_pages_to_fetch)
				logger.info(f"Limiting to {max_pages_to_fetch} pages due to global_limit ({args.global_limit} repos)")

			# Don't apply max_pages limit to starred repos - we want all of them
			if hasattr(args, 'max_pages') and args.max_pages > 0 and args.max_pages < last_page_no:
				logger.warning(f"max_pages ({args.max_pages}) is set but will be ignored for starred repos to ensure all {last_page_no * per_page} repos are fetched")

			logger.info(f"Fetching pages 2-{max_pages_to_fetch} concurrently (expecting ~{max_pages_to_fetch * per_page} total repos)")

			# Fetch remaining pages concurrently
			semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
			stop_signal = asyncio.Event()

			tasks = [
				fetch_page_generic(api_session, api_url, page_num, headers, semaphore, args, max_pages_to_fetch, stop_signal)
				for page_num in range(2, max_pages_to_fetch + 1)
			]

			page_results = await asyncio.gather(*tasks, return_exceptions=True)

			# Process results
			successful_pages = []
			failed_pages = 0
			for result in page_results:
				if isinstance(result, Exception):
					logger.error(f"Page fetch failed: {result}")
					failed_pages += 1
					continue
				if result is None:
					failed_pages += 1
					continue
				page_num, page_data = result  # type: ignore
				if page_data:
					successful_pages.append((page_num, page_data))
				else:
					# Empty page - we've reached the end
					logger.info(f"Page {page_num} returned no data - reached end of starred repos")

			# Sort by page number and add to repos
			successful_pages.sort(key=lambda x: x[0])
			for page_num, page_data in successful_pages:
				repos.extend(page_data)
				if len(page_data) < per_page:
					logger.info(f"Page {page_num} returned {len(page_data)} repos (less than {per_page}), likely the last page")

			if failed_pages > 0:
				logger.warning(f"{failed_pages} pages failed to fetch")

	# Cache results
	if repos:
		set_cache_entry(session, cache_key, cache_type, json.dumps(repos))
		session.commit()
		logger.info(f"Successfully fetched and cached {len(repos)} starred repos from {len(successful_pages) + 1} total pages (including first page)")
	else:
		logger.warning("No starred repos fetched!")

	return repos

async def get_lists_and_stars_unified(session, args) -> dict:
	"""
	Unified function that gets both list metadata and repository links from GitHub stars page.
	"""
	cache_key_metadata = "git_lists_metadata"
	cache_key_stars = "git_list_stars"
	cache_type_metadata = "list_metadata"
	cache_type_stars = "list_stars"

	# Try cache first for both types
	cached_metadata = None
	cached_stars = None

	if args.use_cache:
		# Check for cached metadata
		cache_entry_metadata = get_cache_entry(session, cache_key_metadata, cache_type_metadata)
		if cache_entry_metadata:
			try:
				cached_metadata = json.loads(cache_entry_metadata.data)
				if args.debug:
					logger.debug(f"Using cached metadata: {len(cached_metadata)} lists")
			except (json.JSONDecodeError, AttributeError) as e:
				logger.warning(f"Failed to load cached metadata: {e}")

		# Check for cached stars data
		cache_entry_stars = get_cache_entry(session, cache_key_stars, cache_type_stars)
		if cache_entry_stars:
			try:
				cached_stars = json.loads(cache_entry_stars.data)
				if args.debug:
					logger.debug(f"Using cached stars data: {len(cached_stars)} lists")
			except Exception as e:
				logger.warning(f"Failed to load cached stars data: {e} {type(e)}")
				logger.error(f'traceback: {traceback.format_exc()}')

	# If we have both cached, return them
	if cached_metadata and cached_stars:
		return {
			'lists_metadata': cached_metadata,
			'lists_with_repos': cached_stars
		}

	# Get authentication
	auth = await get_auth_params()
	if not auth:
		logger.error('No auth provided for get_lists_and_stars_unified')
		return {
			'lists_metadata': cached_metadata or [],
			'lists_with_repos': cached_stars or {}
		}

	listurl = f'https://github.com/{auth.username}?tab=stars'  # type: ignore

	# Fix: Use proper web scraping headers instead of API headers
	headers = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
		'Accept-Language': 'en-US,en;q=0.5',
		'Accept-Encoding': 'gzip, deflate',
		'Connection': 'keep-alive',
		'Upgrade-Insecure-Requests': '1',
	}

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return {
			'lists_metadata': cached_metadata or [],
			'lists_with_repos': cached_stars or {}
		}

	# Scrape the GitHub stars page once
	soup = None
	async with get_client_session(args) as api_session:
		if args.debug:
			logger.debug(f"Fetching unified star list data from {listurl}")
		async with api_session.get(listurl, headers=headers) as r:
			if r.status == 200:
				content = await r.text()
				soup = BeautifulSoup(content, 'html.parser')
			elif r.status == 406:
				logger.error("GitHub returned 406 Not Acceptable - check your headers. Using cached data if available.")
				return {
					'lists_metadata': cached_metadata or [],
					'lists_with_repos': cached_stars or {}
				}
			else:
				logger.error(f"Failed to fetch star list: {r.status} {listurl}")
				return {
					'lists_metadata': cached_metadata or [],
					'lists_with_repos': cached_stars or {}
				}

	git_lists_metadata = []
	souplist = soup.find_all('a', class_="d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline")

	if args.debug:
		logger.debug(f"Found {len(souplist)} list elements in HTML")

	for sl in souplist:
		# Extract list name
		name_elem = sl.find('h3', class_='f4 text-bold no-wrap mr-3')  # type: ignore
		list_name = name_elem.text.strip() if name_elem else 'Unknown'
		try:
			list_description_elem = sl.find('span', class_='Truncate-text color-fg-muted mr-3')  # type: ignore
			list_description = list_description_elem.text.strip() if list_description_elem else ''
		except AttributeError as e:
			if args.debug:
				logger.warning(f"Failed to extract description for list '{list_name}': {e}")
			list_description = ''
		# Extract repo count
		count_elem = sl.find('div', class_='color-fg-muted text-small no-wrap')  # type: ignore
		repo_count_text = count_elem.text.strip() if count_elem else '0'

		list_info = {
			'name': list_name,
			'description': list_description,
			'list_url': sl.get('href', ''),  # type: ignore
			'repo_count': repo_count_text
		}
		git_lists_metadata.append(list_info)

	lists_with_repos = {}
	listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})

	if len(listsoup) > 0:
		try:
			list_items = listsoup[0].find_all('a', attrs={'class': 'd-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})  # type: ignore

			for item in list_items:
				# Fix: Use the same selectors as above for consistency
				list_name = item.find('h3', class_='f4 text-bold no-wrap mr-3').text.strip() if item.find('h3', class_='f4 text-bold no-wrap mr-3') else 'Unknown'  # type: ignore
				list_link = f"https://github.com{item.attrs['href']}"  # type: ignore
				list_count_info = item.find('div', class_="color-fg-muted text-small no-wrap").text if item.find('div', class_="color-fg-muted text-small no-wrap") else ''  # type: ignore

				# There's no description in this structure
				try:
					list_description_elem = item.find('span', class_='Truncate-text color-fg-muted mr-3')  # type: ignore
					list_description = list_description_elem.text.strip() if list_description_elem else ''
				except AttributeError as e:
					if args.debug:
						logger.warning(f"Failed to extract description for list '{list_name}': {e}")
					list_description = ''
				# list_description = ''

				# Get individual list repository links
				try:
					list_repos = await get_info_for_list(list_link, headers, session, args)
				except Exception as e:
					logger.warning(f'{e} {type(e)} failed to get list info for {list_name}')
					logger.error(f'traceback: {traceback.format_exc()}')
					list_repos = []

				lists_with_repos[list_name] = {
					'href': list_link,
					'count': list_count_info,
					'description': list_description,
					'hrefs': list_repos
				}
		except (TypeError, IndexError) as e:
			logger.error(f'Failed to find list items in soup {e} {type(e)}')
	else:
		logger.warning("No lists found in soup")

	# Cache both results
	if git_lists_metadata:
		set_cache_entry(session, cache_key_metadata, cache_type_metadata, json.dumps(git_lists_metadata))

	if lists_with_repos:
		set_cache_entry(session, cache_key_stars, cache_type_stars, json.dumps(lists_with_repos))

	session.commit()

	logger.info(f"Fetched unified data: {len(git_lists_metadata)} list metadata entries, {len(lists_with_repos)} lists with repos")

	return {
		'lists_metadata': git_lists_metadata,
		'lists_with_repos': lists_with_repos
	}
