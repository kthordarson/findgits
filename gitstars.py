#!/usr/bin/python3
import aiohttp
import asyncio
import aiofiles
import os
from loguru import logger
import requests
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
	cache_data = {'repos': []}
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
					for i, repo in enumerate(cache_data['repos']):
						if repo.get('id') == repo_data.get('id'):
							existing_index = i
							break

					# Update or add to cache
					if existing_index is not None:
						cache_data['repos'][existing_index] = repo_data
						logger.info(f"Updated existing repository in cache: {repo_name}")
					else:
						cache_data['repos'].append(repo_data)
						logger.info(f"Added new repository to cache: {repo_name}")

					# Write updated cache
					if not os.path.exists(CACHE_DIR):
						os.makedirs(CACHE_DIR)

					async with aiofiles.open(stars_cache_file, 'w') as f:
						cache_data['timestamp'] = str(datetime.datetime.now())
						await f.write(json.dumps(cache_data, indent=4))

					return repo_data
				else:
					logger.error(f"Failed to fetch repository data: {r.status} {await r.text()}")
					return None

	except Exception as e:
		logger.error(f"Error fetching repository data: {e}")
		return None

async def get_git_stars(max=70, use_cache=True):
	"""
	Get all starred repos with caching support
	"""
	# Cache file for starred repos
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	jsonbuffer = []
	stars_dict = {}

	# Try to load from cache first if use_cache is enabled
	if use_cache:
		try:
			async with aiofiles.open(stars_cache_file, 'r') as f:
				cache_content = await f.read()
				cache_data = json.loads(cache_content)
				jsonbuffer = cache_data['repos']
				# Rebuild the dictionary
				for s in jsonbuffer:
					stars_dict[s['id']] = s
				logger.info(f"Loaded {len(jsonbuffer)} starred repos from cache")
				return jsonbuffer, stars_dict
		except FileNotFoundError:
			logger.debug(f"Cache file not found: {stars_cache_file}")
		except json.JSONDecodeError as e:
			logger.error(f"Invalid JSON in cache file: {e}")
		except Exception as e:
			logger.error(f"Error loading from cache: {e}")

	# If we get here, we need to fetch from the API
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_stars: no auth provided')
		return [], {}

	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'}

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
					for s in data:
						stars_dict[s['id']] = s

					# Handle pagination
					if 'link' in r.headers:
						page_count = 1  # We've already got page 1
						links = r.headers['link'].split(',')
						nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
						lasturl = [k for k in links if 'last' in k][0].split('>')[0].replace('<','')
						last_page_no = int(lasturl.split('=')[1])

						# Fetch remaining pages
						while 'link' in r.headers and (max == 0 or page_count < max):
							logger.debug(f'[r] p:{page_count}/{last_page_no} nexturl: {nexturl}')
							async with session.get(nexturl, headers=headers) as r:
								if r.status == 200:
									data = await r.json()
									jsonbuffer.extend(data)
									for s in data:
										stars_dict[s['id']] = s
									page_count += 1

									if 'link' in r.headers:
										links = r.headers['link'].split(',')
										try:
											nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
										except IndexError as e:
											logger.error(f"[r] {e} {r.headers}")
											break
									else:
										logger.warning(f'[r] {r.status} link not in headers: {r.headers} nexturl: {nexturl}')
										break
								else:
									logger.warning(f'[r] {r.status} {nexturl}')
									break
		except Exception as e:
			logger.error(f"[r] {e}")
			return [], {}

	# Write to cache if we got data
	if jsonbuffer:
		try:
			# Create directory if it doesn't exist
			if not os.path.exists(CACHE_DIR):
				os.makedirs(CACHE_DIR)

			async with aiofiles.open(stars_cache_file, 'w') as f:
				await f.write(json.dumps({'repos': jsonbuffer, 'timestamp': str(datetime.datetime.now())}, indent=4))
				logger.info(f"Cached {len(jsonbuffer)} starred repos")
		except Exception as e:
			logger.error(f"Failed to write cache: {e}")

	return jsonbuffer, stars_dict

async def get_git_lists() -> dict:
	"""
	get lists of starred repos
	"""
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_lists: no auth provided')
		return {}

	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}','X-GitHub-Api-Version': '2022-11-28'}

	soup = None
	use_cache = True
	cache_file = f'{CACHE_DIR}/starlist.tmp'

	if use_cache:
		try:
			async with aiofiles.open(cache_file, 'r') as f:
				content = await f.read()
				soup = BeautifulSoup(content, 'html.parser')
		except Exception as e:
			logger.error(f'failed to read starlist.tmp {e}')

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
	link_fn = CACHE_DIR + '/' + link.split('/')[-1] + '.tmp'
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

if __name__ == '__main__':
	pass
