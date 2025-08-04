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
from utils import get_client_session

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
			starred_repos = await get_git_stars(args, session)
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
	do_download = False

	# Try to load from cache first if use_cache is enabled
	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				jsonbuffer = json.loads(cache_entry.data)
				logger.info(f"Loaded {len(jsonbuffer)} starred repos from database cache")
			except json.JSONDecodeError as e:
				logger.error(f"Invalid JSON in cache entry: {e}")
				do_download = True
			except Exception as e:
				logger.error(f"Error loading from cache: {e}")
				do_download = True
			finally:
				if len(jsonbuffer) == 0:
					logger.warning("No cache entry found in database for starred repos")
				return jsonbuffer
		else:
			logger.warning("No cache entry found in database for starred repos")
			git_starred_repos = await download_git_stars(args, session)
			logger.info(f"Fetched {len(git_starred_repos)} starred repos from GitHub API.")
			return git_starred_repos
	else:
		logger.info("[todofix] Cache not used, will download starred repos...")
	return jsonbuffer

async def download_git_stars(args, session):
	# If we get here, we need to fetch from the API
	jsonbuffer = []
	if args.nodl:
		logger.warning('Skipping API call due to --nodl flag')
		return jsonbuffer
	auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("FINDGITSTOKEN",''))
	if not auth:
		logger.error('no auth provided')
		return None

	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'}

	cache_key = "starred_repos_list"
	cache_type = "starred_repos"

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return None

	# async with aiohttp.ClientSession() as api_session:
	async with get_client_session(args) as api_session:
		try:
			async with api_session.get(apiurl) as r:
				if r.status == 401:
					logger.error(f"[r] autherr:401 a:{auth}")
				elif r.status == 404:
					logger.warning(f"[r] {r.status} {apiurl} not found")
				elif r.status == 403:
					logger.warning(f"[r] {r.status} {apiurl} API rate limit exceeded")
				elif r.status == 200:
					data = await r.json()
					jsonbuffer.extend(data)
					# Save after first page
					if jsonbuffer:
						cache_obj = json.dumps(jsonbuffer)
						set_cache_entry(session, cache_key, cache_type, cache_obj)
						session.commit()

					# Handle pagination
					if 'link' in r.headers:
						page_count = 1  # We've already got page 1
						links = r.headers['link'].split(',')
						nexturl = None
						lasturl = None
						for k in links:
							if 'next' in k:
								nexturl = k.split('>')[0].replace('<','')
							if 'last' in k:
								lasturl = k.split('>')[0].replace('<','')
						if not nexturl or not lasturl:
							return jsonbuffer
						last_page_no = int(lasturl.split('=')[1])
						# Fetch remaining pages
						while nexturl and (args.max_pages == 0 or page_count < args.max_pages):
							if args.debug:
								logger.debug(f'[r] p:{page_count}/{last_page_no} jsonbuffer: {len(jsonbuffer)} nexturl: {nexturl}')
							async with api_session.get(nexturl, headers=headers) as r:
								if r.status == 200:
									data = await r.json()
									jsonbuffer.extend(data)
									page_count += 1
									# Save after each page
									if jsonbuffer:
										cache_obj = json.dumps(jsonbuffer)
										set_cache_entry(session, cache_key, cache_type, cache_obj)
										session.commit()

									if 'link' in r.headers:
										links = r.headers['link'].split(',')
										nexturl = None
										for k in links:
											if 'next' in k:
												nexturl = k.split('>')[0].replace('<','')
										if not nexturl:
											break
									else:
										logger.warning(f'[r] {r.status} link not in headers: {r.headers} nexturl: {nexturl}')
										break
								else:
									logger.warning(f'[r] {r.status} {nexturl}')
									break
		except Exception as e:
			logger.error(f"[r] {e}")
			return jsonbuffer

	# Write to cache if we got data (final write)
	if jsonbuffer:
		try:
			cache_obj = json.dumps(jsonbuffer)
			set_cache_entry(session, cache_key, cache_type, cache_obj)
			session.commit()
			logger.info(f"Cached {len(jsonbuffer)} starred repos in database")
		except Exception as e:
			logger.error(f"Failed to write cache: {e}")
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

async def get_lists(args) -> dict:
	"""
	Fetches the user's GitHub starred repository lists by scraping the stars page.

	Returns:
		A list of dictionaries, each containing:
			- name: The list's name
			- repo_count: Number of repositories in the list
			- description: List description (if available)
			- list_url: Relative URL to the list on GitHub
	"""
	git_lists = []
	listurl = f'https://github.com/{os.getenv("GITHUB_USERNAME",'')}?tab=stars'
	async with aiohttp.ClientSession() as api_session:
		if args.debug:
			logger.debug(f"Fetching star list from {listurl}")
		async with api_session.get(listurl) as r:
			if r.status == 200:
				content = await r.text()
	soup = BeautifulSoup(content, 'html.parser')
	souplist = soup.find_all('a',class_="d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline")
	# souplist = soup.find_all('div',id="profile-lists-container")
	for sl in souplist:
		list_name = sl.find('h3', class_="f4 text-bold no-wrap mr-3").get_text(strip=True)
		repo_count = sl.find('div', class_="color-fg-muted text-small no-wrap").get_text(strip=True).split()[0]
		list_description = sl.find('span', class_="Truncate-text color-fg-muted mr-3").get_text(strip=True) if sl.find('span', class_="Truncate-text color-fg-muted mr-3") else ''
		list_url = sl['href']
		list_item = {
			'name': list_name,
			'repo_count': repo_count,
			'description': list_description,
			'list_url': list_url,
		}
		git_lists.append(list_item)
		if args.debug:
			logger.debug(f"List found: {list_name}, Count: {repo_count}, Description: {list_description}, total lists: {len(git_lists)}")
	return git_lists

async def get_git_list_stars(session, args) -> dict:
	"""
	Get lists of starred repos using database cache
	"""
	cache_key = "git_list_stars"
	cache_type = "list_stars"

	auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("FINDGITSTOKEN",''))
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

		# async with get_client_session() as api_session:
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
	Get info for a list using database cache
	"""
	cache_key = f"list_info:{link.split('/')[-1]}"
	cache_type = "list_info"

	soup = None

	if args.use_cache:
		cache_entry = get_cache_entry(session, cache_key, cache_type)
		if cache_entry:
			try:
				soup = BeautifulSoup(cache_entry.data, 'html.parser')
				if args.debug:
					logger.debug(f"Loaded list info from database cache for {link}")
			except Exception as e:
				logger.error(f'Failed to parse cached list info: {e}')
		else:
			logger.warning(f"No cache entry found for {link} in database")
	if args.nodl:
		logger.warning(f"Skipping API call for {link} due to --nodl flag")
		return []
	if not soup:
		if await is_rate_limit_hit(args):
			logger.warning("Rate limit hit!")
			await asyncio.sleep(1)
			return None

		# async with aiohttp.ClientSession() as api_session:
		async with get_client_session(args) as api_session:
			if args.debug:
				logger.debug(f"Fetching list info from {link}")
			async with api_session.get(link) as r:
				content = await r.content.read()
				soup = BeautifulSoup(content, 'html.parser')

				try:
					# Save to database cache
					set_cache_entry(session, cache_key, cache_type, str(soup))
					session.commit()
					logger.info(f"Saved list info to database cache for {link}")
				except Exception as e:
					logger.error(f'Failed to save list info: {e} {type(e)}')

	soupdata = soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"})
	listdata = soupdata.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
	list_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in listdata]

	return list_hrefs

async def fetch_starred_repos(args, session):
	"""
	Fetch user's starred repositories from GitHub API with database caching
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
			logger.warning("No cache entry found in database for starred repos")
			# Proceed to download if no cache or cache is too old
	if args.nodl:
		logger.warning("Skipping API call due to --nodl flag")
		return []
	# Get authentication
	auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("FINDGITSTOKEN",''))
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

	repos = []
	page = 1
	per_page = 100  # Max allowed by GitHub API

	if await is_rate_limit_hit(args):
		logger.warning("Rate limit hit!")
		await asyncio.sleep(1)
		return None

	# Make API requests with pagination
	# async with aiohttp.ClientSession() as api_session:
	async with get_client_session(args) as api_session:
		while True:
			url = f"{api_url}?page={page}&per_page={per_page}"
			if args.debug:
				logger.debug(f"Fetching starred repos page {page}")
			if args.max_pages > 0 and page > args.max_pages:
				if args.debug:
					logger.debug(f"hit max: {args.max_pages} page {page}")
				break
			try:
				async with api_session.get(url) as response:
					if response.status == 200:
						page_data = await response.json()
						# No more data to fetch
						if not page_data:
							if args.debug:
								logger.debug(f"No more starred repos found on page {page}")
							break

						repos.extend(page_data)
						if args.debug:
							logger.debug(f"Fetched {len(page_data)} repos from page {page}")

						# Check if we've reached the last page
						if len(page_data) < per_page:
							break

						page += 1
					elif response.status == 401:
						logger.error("Authentication failed - check your token")
						break
					elif response.status == 403:
						# Check rate limit headers
						reset_time = response.headers.get('X-RateLimit-Reset')
						if reset_time:
							reset_datetime = datetime.datetime.fromtimestamp(int(reset_time))
							wait_time = (reset_datetime - datetime.datetime.now()).total_seconds()
							logger.warning(f"Rate limit exceeded. Resets in {wait_time:.1f} seconds")
						else:
							logger.warning("Rate limit exceeded")
						break
					else:
						logger.error(f"API request failed with status {response.status}")
						error_data = await response.text()
						logger.warning(f"Error response: {error_data}")
						break
			except Exception as e:
				logger.error(f"Request error: {e} {type(e)}")
				break

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
