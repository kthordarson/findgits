#!/usr/bin/python3
import aiohttp
import aiofiles
import os
from loguru import logger
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import datetime
import json

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.cache', 'gitstars')


async def update_repo_cache(repo_name_or_url, use_existing_cache=True):
	"""
	Fetch repository data from GitHub API and update the cache
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

	# Load existing cache if available and requested
	cache_data = []
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	if use_existing_cache and os.path.exists(stars_cache_file):
		try:
			async with aiofiles.open(stars_cache_file, 'r') as f:
				cache_content = await f.read()
				cache_data = json.loads(cache_content)
		except Exception as e:
			logger.error(f"Failed to load cache:{type(e)} {e} from {stars_cache_file}")

	# Fetch repository data from GitHub API
	auth = get_auth_param()
	if not auth:
		logger.error('update_repo_cache: no auth provided')
		return None

	api_url = f'https://api.github.com/repos/{repo_name}'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	try:
		async with aiohttp.ClientSession() as session:
			async with session.get(api_url, headers=headers) as r:
				if r.status == 200:
					repo_data = await r.json()

					# Check if repo already exists in cache
					existing_index = None
					for i, repo in enumerate(cache_data):
						try:
							if repo.get('id') == repo_data.get('id'):
								existing_index = i
								break
						except Exception as e:
							logger.error(f"Error checking cache entry: {e} {type(e)} repo={repo}")

					# Update or add to cache
					if existing_index is not None:
						cache_data[existing_index] = repo_data
						logger.info(f"Updated existing repository in cache: {repo_name}")
					else:
						cache_data.append(repo_data)
						logger.info(f"Added new repository to cache: {repo_name}")

					async with aiofiles.open(stars_cache_file, 'w') as f:
						await f.write(json.dumps(cache_data, indent=4))

					return repo_data
				else:
					logger.error(f"Failed to fetch repository data: {r.status} {await r.text()}")
					return None
	except Exception as e:
		logger.error(f"Error fetching repository data: {e} {type(e)}")
		return None

# curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer <YOUR-TOKEN>" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/user/starred

async def get_git_stars(args):
	"""
	Get all starred repos with caching support
	"""
	# Cache file for starred repos
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	jsonbuffer = []
	git_starred_repos = []
	do_download = False
	# Try to load from cache first if use_cache is enabled
	if args.use_cache:
		try:
			async with aiofiles.open(stars_cache_file, 'r') as f:
				cache_content = await f.read()
				jsonbuffer = json.loads(cache_content)
				# jsonbuffer = cache_data
				logger.info(f"Loaded {len(jsonbuffer)} starred repos from cache")
				return jsonbuffer
		except FileNotFoundError as e:
			logger.error(f"{e} Cache file not found: {stars_cache_file}")
			do_download = True
			# return []
		except json.JSONDecodeError as e:
			logger.error(f"Invalid JSON in cache file: {e}")
			do_download = True
			# return None
		except Exception as e:
			logger.error(f"Error loading from cache: {e}")
			do_download = True
			# return None
	if do_download or not args.use_cache:
		logger.info(f"Starting download_git_stars with max_pages={args.max_pages}")
		try:
			git_starred_repos = await download_git_stars(args)
		except Exception as e:
			logger.error(f"Error downloading starred repos: {e} {type(e)}")
		logger.info(f"Fetched {len(git_starred_repos)} starred repos from GitHub API. max_pages={args.max_pages}")
	return git_starred_repos

async def download_git_stars(args):
	# If we get here, we need to fetch from the API
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_stars: no auth provided')
		return None

	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'}
	jsonbuffer = []
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'
	async with aiohttp.ClientSession() as session:
		try:
			async with session.get(apiurl, headers=headers) as r:
				if r.status == 401:
					logger.error(f"[r] autherr:401 a:{auth}")
				elif r.status == 404:
					logger.warning(f"[r] {r.status} {apiurl} not found")
				elif r.status == 403:
					logger.warning(f"[r] {r.status} {apiurl} API rate limit exceeded")
				elif r.status == 200:
					data = await r.json()
					jsonbuffer.extend(data)
					# Handle pagination
					if 'link' in r.headers:
						page_count = 1  # We've already got page 1
						links = r.headers['link'].split(',')
						try:
							nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
						except Exception as e:
							logger.error(f"[r] {e} {r.headers}")
							nexturl = None

						try:
							lasturl = [k for k in links if 'last' in k][0].split('>')[0].replace('<','')
						except Exception as e:
							logger.error(f"[r] {e} {r.headers}")
							lasturl = None
						try:
							last_page_no = int(lasturl.split('=')[1])
						except Exception as e:
							logger.error(f"[r] {e} {r.headers}")
							last_page_no = 0
						# Fetch remaining pages
						try:
							while 'link' in r.headers and (args.max_pages == 0 or page_count < args.max_pages):
								logger.debug(f'[r] p:{page_count}/{last_page_no} nexturl: {nexturl}')
								async with session.get(nexturl, headers=headers) as r:
									if r.status == 200:
										data = await r.json()
										jsonbuffer.extend(data)
										page_count += 1
										if 'link' in r.headers:
											links = r.headers['link'].split(',')
											try:
												nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
											except Exception as e:
												logger.error(f"[r] {e} {type(e)} {r.headers} ")
												break
										else:
											logger.warning(f'[r] {r.status} link not in headers: {r.headers} nexturl: {nexturl}')
											break
									else:
										logger.warning(f'[r] {r.status} {nexturl}')
										break
						except Exception as e:
							logger.error(f"[r] {e} {type(e)} {nexturl} {apiurl} ")
		except Exception as e:
			logger.error(f"[r] {e} {type(e)} {apiurl}")
		await write_jsonbuffer_to_cache(jsonbuffer, stars_cache_file, args)
		return jsonbuffer

async def write_jsonbuffer_to_cache(jsonbuffer, stars_cache_file, args):
	if args.debug:
		logger.debug(f"Writing {len(jsonbuffer)} starred repos to cache file: {stars_cache_file}")
	# Write to cache if we got data
	try:
		async with aiofiles.open(stars_cache_file, 'w') as f:
			await f.write(json.dumps(jsonbuffer, indent=4))
			logger.info(f"Cached {len(jsonbuffer)} starred repos")
	except Exception as e:
		logger.error(f"Failed to write cache: {e} {type(e)}")

async def get_git_list_stars(use_cache=True) -> dict:
	"""
	get lists of starred repos
	"""
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_list_stars: no auth provided')
		return {}

	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}','X-GitHub-Api-Version': '2022-11-28'}
	soup = None
	cache_file = f'{CACHE_DIR}/starlist.cache'

	if use_cache:
		try:
			async with aiofiles.open(cache_file, 'r') as f:
				content = await f.read()
				soup = BeautifulSoup(content, 'html.parser')
		except Exception as e:
			logger.error(f'failed to read {cache_file} {e}')

	if not soup:
		async with aiohttp.ClientSession() as session:
			async with session.get(listurl, headers=headers) as r:
				content = await r.text()
				soup = BeautifulSoup(content, 'html.parser')
				async with aiofiles.open(cache_file, 'w') as f:
					await f.write(str(soup))

	listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})
	list_items = listsoup[0].find_all('a', attrs={'class':'d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})

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
			list_repos = await get_info_for_list(list_link, headers, use_cache)
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

async def get_info_for_list(link, headers, use_cache):
	"""
	get info for a list
	"""
	link_fn = CACHE_DIR + '/' + link.split('/')[-1] + '.cache'
	soup = None

	if use_cache:
		try:
			async with aiofiles.open(link_fn, 'r') as f:
				content = await f.read()
				soup = BeautifulSoup(content, 'html.parser')
		except FileNotFoundError:
			pass  # Not an error
		except Exception as e:
			logger.error(f'failed to read {link_fn} {e}')

	if not soup:
		async with aiohttp.ClientSession() as session:
			async with session.get(link, headers=headers) as r:
				content = await r.content.read()
				soup = BeautifulSoup(content, 'html.parser')

				try:
					async with aiofiles.open(link_fn, 'w') as f:
						await f.write(str(soup))
				except Exception as e:
					logger.error(f'failed to write {link_fn} {e} {type(e)}')

	soupdata = soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"})
	listdata = soupdata.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
	list_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in listdata]

	return list_hrefs

def get_updated_at_sort(x) -> HTTPBasicAuth:
	return x['updated_at']

def get_auth_param():
	try:
		auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("GITHUBAPITOKEN",''))
	except Exception as e:
		logger.error(f'failed to get auth param {e} {type(e)}')
		return None
	return auth

async def fetch_starred_repos(args):
	"""
	Fetch user's starred repositories from GitHub API

	Parameters:
		max_pages: Maximum number of pages to fetch (0 = all pages)
		use_cache: Whether to use cached results if available

	Returns:
		list: List of starred repository data dictionaries
	"""
	# Define cache file path
	# cache_dir = os.path.join(os.path.expanduser('~'), '.cache', 'gitstars')
	# stars_cache_file = f'{cache_dir}/starred_repos.json'
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	# Check cache first if enabled
	if args.use_cache and os.path.exists(stars_cache_file):
		try:
			cache_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(stars_cache_file))
			# Use cache if it's less than 1 day old
			if cache_age.days < 1:
				async with aiofiles.open(stars_cache_file, 'r') as f:
					cache_data = json.loads(await f.read())
					logger.info(f"Using cached starred repos ({len(cache_data)} items)")
					return cache_data
		except Exception as e:
			logger.warning(f"Failed to load cache: {e}")

	# Get authentication
	auth = get_auth_param()
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

	# Make API requests with pagination
	async with aiohttp.ClientSession() as session:
		while True:
			url = f"{api_url}?page={page}&per_page={per_page}"
			if args.debug:
				logger.debug(f"Fetching starred repos page {page}")
			if args.max_pages > 0 and page > args.max_pages:
				if args.debug:
					logger.debug(f"hit max: {args.max_pages} page {page}")
				break
			try:
				async with session.get(url, headers=headers) as response:
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
						logger.debug(f"Error response: {error_data}")
						break
			except Exception as e:
				logger.error(f"Request error: {e} {type(e)}")
				break

	# Cache the results if we got data
	if repos:
		try:
			# os.makedirs(cache_dir, exist_ok=True)
			async with aiofiles.open(stars_cache_file, 'w') as f:
				await f.write(json.dumps(repos, indent=2))
				logger.info(f"Cached {len(repos)} starred repositories")
		except Exception as e:
			logger.error(f"Failed to write cache: {e}")
	else:
		logger.warning("No starred repositories found or fetched")

	logger.info(f"Fetched {len(repos)} starred repositories from GitHub API")
	return repos

if __name__ == '__main__':
	pass
