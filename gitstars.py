#!/usr/bin/python3
import os
from loguru import logger
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import datetime
import json

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.cache', 'gitstars')


def update_repo_cache(repo_name_or_url, use_existing_cache=True):
	"""
	Fetch repository data from GitHub API and update the cache

	Parameters:
		repo_name_or_url (str): Repository name (e.g., 'owner/repo') or full URL
		use_existing_cache (bool): Whether to use existing cache entries

	Returns:
		dict: Repository data or None if not found/error
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

	# logger.debug(f"Normalized repository path: {repo_name}")

	# Load existing cache if available and requested
	cache_data = {'repos': []}
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	if use_existing_cache and os.path.exists(stars_cache_file):
		try:
			with open(stars_cache_file, 'r') as f:
				cache_data = json.load(f)
		except Exception as e:
			logger.error(f"Failed to load cache: {e}")

	# Fetch repository data from GitHub API
	auth = get_auth_param()
	if not auth:
		logger.error('update_repo_cache: no auth provided')
		return None

	session = requests.session()
	api_url = f'https://api.github.com/repos/{repo_name}'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	try:
		# logger.info(f"Fetching repository data for {repo_name}")
		r = session.get(api_url, headers=headers)

		if r.status_code == 200:
			repo_data = r.json()

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

			with open(stars_cache_file, 'w') as f:
				cache_data['timestamp'] = str(datetime.datetime.now())
				json.dump(cache_data, f)

			return repo_data
		else:
			logger.error(f"Failed to fetch repository data: {r.status_code} {r.text}")
			return None

	except Exception as e:
		logger.error(f"Error fetching repository data: {e}")
		return None

def get_git_stars(max=70, use_cache=True):
	"""
	Get all starred repos with caching support

	Parameters:
		max: Maximum number of pages to fetch (0 for all)
		use_cache: Whether to use cached data if available

	Returns:
		Tuple of (list of starred repos, dict of repos by id)
	"""
	# Cache file for starred repos
	stars_cache_file = f'{CACHE_DIR}/starred_repos.json'

	jsonbuffer = []
	stars_dict = {}

	# Try to load from cache first if use_cache is enabled
	if use_cache:
		try:
			with open(stars_cache_file, 'r') as f:
				cache_data = json.load(f)
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
	session = requests.session()
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_stars: no auth provided')
		return [], {}

	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'}

	try:
		r = session.get(apiurl, headers=headers)
	except Exception as e:
		logger.error(f"[r] {e}")
		return [], {}

	if r.status_code == 401:
		logger.error(f"[r] autherr:401 a:{auth}")
	elif r.status_code == 404:
		logger.warning(f"[r] {r.status_code} {apiurl} not found")
	elif r.status_code == 403:
		logger.warning(f"[r] {r.status_code} {apiurl} API rate limit exceeded")
	elif r.status_code == 200:
		jsonbuffer.extend(r.json())
		for s in r.json():
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
			r = session.get(nexturl, headers=headers)

			if r.status_code == 200:
				jsonbuffer.extend(r.json())
				for s in r.json():
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
					logger.warning(f'[r] {r.status_code} link not in headers: {r.headers} nexturl: {nexturl}')
					break
			else:
				logger.warning(f'[r] {r.status_code} {nexturl}')
				break

	# Write to cache if we got data
	if jsonbuffer:
		try:
			# Create directory if it doesn't exist
			if not os.path.exists(CACHE_DIR):
				os.makedirs(CACHE_DIR)

			with open(stars_cache_file, 'w') as f:
				json.dump({'repos': jsonbuffer, 'timestamp': str(datetime.datetime.now())}, f)
				logger.info(f"Cached {len(jsonbuffer)} starred repos")
		except Exception as e:
			logger.error(f"Failed to write cache: {e}")

	return jsonbuffer, stars_dict

def get_git_lists() -> dict:
	"""
	get lists of starred repos
	param auth: HTTPBasicAuth
	returns dict of lists
	"""
	# todo handle pagination better ....
	# todo handle cache better
	auth = get_auth_param()
	if not auth:
		logger.error('get_git_lists: no auth provided')
		return {}
	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}','X-GitHub-Api-Version': '2022-11-28'}
	session = requests.session()
	session.headers.update(headers)
	soup = None
	use_cache = True
	if use_cache:
		try:
			with open(f'{CACHE_DIR}/starlist.tmp', 'r') as f:
				soup = BeautifulSoup(f.read(), 'html.parser')
		except Exception as e:
			logger.error(f'failed to read starlist.tmp {e}')
	if not soup:
		r = session.get(listurl)
		soup = BeautifulSoup(r.text, 'html.parser')
		with open(f'{CACHE_DIR}/starlist.tmp', 'w') as f:
			f.write(str(soup))
	listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})
	list_items = listsoup[0].find_all('a', attrs={'class':'d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})  # type: ignore
	# logger.debug(f'list_items: {len(list_items)} listsoup: {len(listsoup)}')
	lists = {}
	for item in list_items:
		listname = item.find('h3').text  # type: ignore
		list_link = f"https://github.com{item.attrs['href']}"  # type: ignore
		list_count_info = item.find('div', class_="color-fg-muted text-small no-wrap").text  # type: ignore
		# logger.debug(f'listname: {listname} list_link: {list_link} list_count_info: {list_count_info}')
		try:
			list_description = item.select('span', class_="Truncate-text color-fg-muted mr-3")[1].text.strip()  # type: ignore
		except IndexError as e:
			# logger.warning(f'{e} no description for {listname}')
			list_description = ''
		try:
			list_repos = get_info_for_list(list_link, session, use_cache)
		except Exception as e:
			logger.warning(f'{e} {type(e)} failed to get list info for {listname}')
			list_repos = []
		lists[listname] = {'href': list_link, 'count': list_count_info, 'description': list_description, 'hrefs': list_repos}
	return lists

def get_info_for_list(link, session, use_cache):
	"""
	get info for a list
	param list_href: str
	param auth: HTTPBasicAuth
	"""
	# todo handle pagination
	# todo maybe pull more info here
	# todo handle cache better
	link_fn = CACHE_DIR + '/' + link.split('/')[-1] + '.tmp'
	soup = None
	if use_cache:
		try:
			with open(link_fn, 'r') as f:
				soup = BeautifulSoup(f.read(), 'html.parser')
		except FileNotFoundError as e:
			pass  # logger.warning(f'failed to read {link_fn} {e}')
		except Exception as e:
			logger.error(f'failed to read {link_fn} {e}')
	if not soup:
		r = session.get(link)
		soup = BeautifulSoup(r.content, 'html.parser')
	# soup = BeautifulSoup(r.read(), "html.parser")
	try:
		with open(link_fn, 'w') as f:
			f.write(str(soup))
	except Exception as e:
		logger.error(f'failed to write {link_fn} {e} {type(e)}')
	# userlist_repos_data = soup.select_one('div', attrs={"id":"user-list-repositories"})
	# userlist_repos_data = soup.find('div', attrs={"id":"user-list-repositories", "class":"border-top mt-5"})
	# len(soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"}))
	soupdata = soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"})
	listdata = soupdata.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
	list_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in listdata]  # type: ignore
	# logger.debug(f'list_hrefs: {len(list_hrefs)} for {link}')  # type: ignore
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
	# todo add argparse
	# todo handle cache better
	if not os.path.exists(CACHE_DIR):
		logger.debug(f'creating cache dir: {CACHE_DIR}')
		os.makedirs(CACHE_DIR)
	use_cache = True
	max_items = 4

	lists = get_git_lists()
	starred_repos, stars_dict = get_git_stars(max=max_items)
	# sorted(starred_repos,key=get_updated_at_sort,reverse=True)
	idlist = [k['id'] for k in starred_repos]
	logger.debug(f'idlist: {len(idlist)} lists: {len(lists)} stars: {len(starred_repos)}')
	# _ = [print(f'id: {k['id']} {k['name']} {k['updated_at']}') for k in starred_repos]
	# _ = [print(k.get('name')  in ''.join([''.join(lists[k].get('hrefs')) for k in lists]))  for k in starred_repos ]
	# _ = [print(f"{k.get('name')} listed: {k.get('name')  in ''.join([''.join(lists[k].get('hrefs')) for k in lists])}")  for k in starred_repos ]
