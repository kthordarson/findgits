#!/usr/bin/env python3
import os
import aiohttp
import asyncio
from dataclasses import dataclass
from datetime import datetime
from loguru import logger
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import json
import argparse

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.cache', 'gitstars')

@dataclass
class RateLimit:
	limit: int
	used: int
	remaining: int
	reset: int

	@property
	def reset_time(self) -> datetime:
		return datetime.fromtimestamp(self.reset)

	@property
	def is_exceeded(self) -> bool:
		return self.remaining <= 0

	def __str__(self) -> str:
		return f"RateLimit(remaining={self.remaining}/{self.limit}, resets={self.reset_time})"


async def get_rate_limit_async(session: aiohttp.ClientSession) -> dict[str, RateLimit]:
	"""Get current rate limit status asynchronously"""
	url = 'https://api.github.com/rate_limit'
	async with session.get(url) as r:
		if r.status != 200:
			logger.error(f"Failed to get rate limit: {r.status}")
			return {}
		data = await r.json()
		limits = {}
		for resource, values in data.get('resources', {}).items():
			limits[resource] = RateLimit(
				limit=values['limit'],
				used=values['used'],
				remaining=values['remaining'],
				reset=values['reset']
			)
		return limits


def get_rate_limit(auth: HTTPBasicAuth) -> dict[str, RateLimit]:
	"""Get current rate limit status synchronously"""
	url = 'https://api.github.com/rate_limit'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}
	limits = {}
	try:
		r = requests.get(url, headers=headers)
		if r.status_code != 200:
			logger.error(f"Failed to get rate limit: {r.status_code}")
			return limits
		data = r.json()
		for resource, values in data.get('resources', {}).items():
			limits[resource] = RateLimit(
				limit=values['limit'],
				used=values['used'],
				remaining=values['remaining'],
				reset=values['reset']
			)
		return limits
	except Exception as e:
		logger.error(f"Failed to get rate limit: {e}")
		return limits


async def check_rate_limit_async(session: aiohttp.ClientSession, resource: str = 'core', min_remaining: int = 10) -> bool:
	"""
	Check if we have enough rate limit remaining
	Returns True if OK to proceed, False if rate limited
	"""
	limits = await get_rate_limit_async(session)
	if not limits:
		logger.warning("Could not fetch rate limits, proceeding anyway")
		return True

	if resource not in limits:
		logger.warning(f"Unknown resource '{resource}', proceeding anyway")
		return True

	limit = limits[resource]
	# logger.debug(f"Rate limit for {resource}: {limit}")

	if limit.remaining < min_remaining:
		wait_seconds = limit.reset - int(datetime.now().timestamp())
		if wait_seconds > 0:
			logger.warning(f"Rate limit low ({limit.remaining} remaining). Resets in {wait_seconds}s at {limit.reset_time}")
			return False

	return True


async def wait_for_rate_limit_async(session: aiohttp.ClientSession, resource: str = 'core') -> None:
	"""Wait until rate limit resets if exceeded"""
	limits = await get_rate_limit_async(session)
	if not limits or resource not in limits:
		return

	limit = limits[resource]
	if limit.is_exceeded:
		wait_seconds = limit.reset - int(datetime.now().timestamp()) + 1
		if wait_seconds > 0:
			logger.warning(f"Rate limit exceeded. Waiting {wait_seconds}s until {limit.reset_time}")
			await asyncio.sleep(wait_seconds)


async def get_git_stars_async(auth, max=None):
	"""Get all starred repos asynchronously"""
	jsonbuffer = []
	stars_dict = {}
	apiurl = 'https://api.github.com/user/starred'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'
	}

	async with aiohttp.ClientSession(headers=headers) as session:
		# Check rate limit before starting
		if not await check_rate_limit_async(session, 'core', min_remaining=10):
			await wait_for_rate_limit_async(session, 'core')

		# Get first page to find total pages
		async with session.get(apiurl) as r:
			if r.status == 403:
				logger.error("Rate limit exceeded")
				await wait_for_rate_limit_async(session, 'core')
				return [], {}
			if r.status != 200:
				logger.error(f"[r] {r.status}")
				return [], {}

			data = await r.json()
			jsonbuffer.extend(data)
			for s in data:
				stars_dict[s['id']] = s

			if 'link' not in r.headers:
				return jsonbuffer, stars_dict

			# Parse last page number
			links = r.headers['link'].split(',')
			lasturl = [k for k in links if 'last' in k][0].split('>')[0].replace('<', '')
			last_page = int(lasturl.split('=')[-1])

		# Check if we have enough rate limit for all pages
		limits = await get_rate_limit_async(session)
		if limits and 'core' in limits:
			pages_needed = (min(last_page, max) if max else last_page) - 1
			if limits['core'].remaining < pages_needed:
				logger.warning(f"Not enough rate limit for {pages_needed} pages (have {limits['core'].remaining})")
				await wait_for_rate_limit_async(session, 'core')

		# Fetch remaining pages concurrently
		max_page = min(last_page, max) if max else last_page
		tasks = [session.get(f"{apiurl}?page={p}") for p in range(2, max_page + 1)]
		# logger.debug(f"[r] tasks: {len(tasks)} pages to fetch")
		responses = await asyncio.gather(*tasks)
		# logger.debug(f"[r] fetched {len(responses)} pages")
		for resp in responses:
			if resp.status == 200:
				data = await resp.json()
				jsonbuffer.extend(data)
				for s in data:
					stars_dict[s['id']] = s
			elif resp.status == 403:
				logger.warning("Hit rate limit during fetch")
				break

	return jsonbuffer, stars_dict

def get_git_stars(auth, max=None, use_cache=False):
	"""
	get all starred repos
	param auth: HTTPBasicAuth
	"""
	# tmpfn = 'starred.tmp'
	jsonbuffer = []
	star_list = []
	stars_dict = {}
	session = requests.session()
	apiurl = 'https://api.github.com/user/starred?per_page=100'
	headers = {
		'Accept': 'application/vnd.github+json',
		'Authorization': f'Bearer {auth.password}',
		'X-GitHub-Api-Version': '2022-11-28'}
	try:
		r = session.get(apiurl, headers=headers)
	except Exception as e:
		logger.error(f"[r] {e}")
		return []
	logger.info(f"[r] {r.status_code}  ")
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
		logger.debug(f"[r] page:1 jsonbuffer: {len(jsonbuffer)} stars_dict: {len(stars_dict)}")
	if 'link' in r.headers:
		page_count = 0
		links = r.headers['link'].split(',')
		nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
		lasturl = [k for k in links if 'last' in k][0].split('>')[0].replace('<','')
		last_page_no = lasturl.split('=')[1]
		while 'link' in r.headers:
			if max and page_count >= max:
				logger.warning(f'[r] p:{page_count}/{last_page_no} nexturl: {nexturl} hit max: {max}')
				break
			logger.debug(f'[r] p:{page_count}/{last_page_no} nexturl: {nexturl}')
			r = session.get(nexturl, headers=headers)
			if r.status_code == 200:
				jsonbuffer.extend(r.json())
				for s in r.json():
					stars_dict[s['id']] = s
				page_count += 1
				if 'link' in r.headers:
					links = r.headers['link'].split(',')
					if links:
						try:
							nexturl = [k for k in links if 'next' in k][0].split('>')[0].replace('<','')
						except IndexError as e:
							logger.error(f"[r] {e} links: {links} no next link found")
							break
				else:
					logger.warning(f'[r] {r.status_code} link not in headers: {r.headers} nexturl: {nexturl}')
					break
			else:
				logger.warning(f'[r] {r.status_code} {nexturl}')
				break
	else:
		logger.warning(f'[r] {r.status_code} link not in headers: {r.headers} ')
	logger.info(f"[r] {r.status_code}  apiurl: {apiurl} jsonbuffer: {len(jsonbuffer)} stars_dict: {len(stars_dict)}")
	return jsonbuffer, stars_dict

def get_git_lists(auth:HTTPBasicAuth, use_cache=False) -> dict:
	"""
	get lists of starred repos
	param auth: HTTPBasicAuth
	returns dict of lists
	"""
	# todo handle pagination better ....
	# todo handle cache better
	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}','X-GitHub-Api-Version': '2022-11-28'}
	session = requests.session()
	session.headers.update(headers)
	soup = None
	if use_cache:
		try:
			with open(f'{CACHE_DIR}/starlist.tmp', 'r') as f:
				soup = BeautifulSoup(f.read(), 'html.parser')
		except Exception as e:
			logger.error(f'failed to read starlist.tmp {e}')
	if not soup:
		r = session.get(listurl)
		soup = BeautifulSoup(r.text, 'html.parser')
		fn = f'{CACHE_DIR}/starlist.tmp'
		with open(fn, 'w') as f:
			f.write(str(soup))
		logger.debug(f'Wrote cache for to {fn}')
	listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})
	list_items = listsoup[0].find_all('a', attrs={'class':'d-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})  # type: ignore
	logger.debug(f'list_items: {len(list_items)} listsoup: {len(listsoup)}')
	lists = {}
	for item in list_items:
		listname = item.find('h3').text  # type: ignore
		list_link = f"https://github.com{item.attrs['href']}"  # type: ignore
		list_count_info = item.find('div', class_="color-fg-muted text-small no-wrap").text  # type: ignore
		logger.debug(f'listname: {listname} list_link: {list_link} list_count_info: {list_count_info}')
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
		logger.debug(f'wrote cache for list {link} to {link_fn}')
	except Exception as e:
		logger.error(f'failed to write {link_fn} {e} {type(e)}')
	# userlist_repos_data = soup.select_one('div', attrs={"id":"user-list-repositories"})
	# userlist_repos_data = soup.find('div', attrs={"id":"user-list-repositories", "class":"border-top mt-5"})
	# len(soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"}))
	soupdata = soup.select_one('div', attrs={"id":"user-list-repositories","class":"my-3"})
	listdata = soupdata.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
	list_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in listdata]  # type: ignore
	logger.debug(f'list_hrefs: {len(list_hrefs)} for {link}')  # type: ignore
	return list_hrefs

def get_updated_at_sort(x) -> HTTPBasicAuth:
	return x['updated_at']

def get_auth_param():
	try:
		auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("GITSTARSTOKEN",''))
	except Exception as e:
		logger.error(f'failed to get auth param {e} {type(e)}')
		exit(1)
	return auth

async def get_info_for_list_async(link, session: aiohttp.ClientSession, use_cache=False):
	"""
	get info for a list asynchronously with pagination support
	param link: str
	param session: aiohttp.ClientSession
	param use_cache: bool
	"""
	link_fn = CACHE_DIR + '/' + link.split('/')[-1] + '.tmp'
	list_hrefs = []
	page = 1
	max_pages = 50  # Safety limit to prevent infinite loops

	while page <= max_pages:
		page_link = f"{link}?page={page}" if page > 1 else link
		cache_fn = f"{link_fn}.page{page}" if page > 1 else link_fn
		soup = None

		if use_cache:
			try:
				with open(cache_fn, 'r') as f:
					soup = BeautifulSoup(f.read(), 'html.parser')
			except FileNotFoundError:
				pass
			except Exception as e:
				logger.error(f'failed to read {cache_fn} {e}')

		if not soup:
			async with session.get(page_link) as r:
				if r.status == 429:
					logger.warning(f'Rate limited on page {page} for {link}')
					await asyncio.sleep(60)  # Wait a minute and retry
					continue
				content = await r.text()
				soup = BeautifulSoup(content, 'html.parser')

		try:
			with open(cache_fn, 'w') as f:
				f.write(str(soup))
			# logger.debug(f'wrote cached page {page} for {link} to {cache_fn}')
		except Exception as e:
			logger.error(f'failed to write {cache_fn} {e} {type(e)}')

		soupdata = soup.select_one('div', attrs={"id": "user-list-repositories", "class": "my-3"})
		if not soupdata:
			logger.warning(f'no more data on page {page} for {link}')
			break

		listdata = soupdata.find_all('div', class_="col-12 d-block width-full py-4 border-bottom color-border-muted")
		if not listdata:
			logger.warning(f'no list items on page {page} for {link}')
			break

		page_hrefs = [k.find('div', class_='d-inline-block mb-1').find('a').attrs['href'] for k in listdata]  # type: ignore
		list_hrefs.extend(page_hrefs)

		# Check for next page - multiple ways GitHub shows pagination
		has_next = False

		# Method 1: Look for pagination container with next link
		pagination = soup.find('div', class_='paginate-container')
		if pagination:
			next_link = pagination.find('a', string='Next')  # type: ignore
			if not next_link:
				next_link = pagination.find('a', class_='next_page')  # type: ignore
			if next_link and 'disabled' not in next_link.get('class', []):  # type: ignore
				has_next = True

		# Method 2: Look for BtnGroup pagination
		if not has_next:
			btn_group = soup.find('div', class_='BtnGroup')
			if btn_group:
				next_btn = btn_group.find('a', string='Next')  # type: ignore
				if next_btn and 'disabled' not in next_btn.get('class', []):  # type: ignore
					has_next = True

		# Method 3: Check if we got a full page (30 items = likely more pages)
		if not has_next and len(page_hrefs) == 30:
			has_next = True

		if not has_next:
			break

		page += 1

	# logger.info(f'total list_hrefs: {len(list_hrefs)} from {page} pages for {link}')
	return list_hrefs


async def get_git_lists_async(auth: HTTPBasicAuth, use_cache=False) -> dict:
	"""
	get lists of starred repos asynchronously
	param auth: HTTPBasicAuth
	returns dict of lists
	"""
	listurl = f'https://github.com/{auth.username}?tab=stars'
	headers = {'Authorization': f'Bearer {auth.password}', 'X-GitHub-Api-Version': '2022-11-28'}
	soup = None

	if use_cache:
		try:
			with open(f'{CACHE_DIR}/starlist.tmp', 'r') as f:
				soup = BeautifulSoup(f.read(), 'html.parser')
		except Exception as e:
			logger.error(f'failed to read starlist.tmp {e}')

	async with aiohttp.ClientSession(headers=headers) as session:
		# Check rate limit before starting
		if not await check_rate_limit_async(session, 'core', min_remaining=5):
			await wait_for_rate_limit_async(session, 'core')

		if not soup:
			async with session.get(listurl) as r:
				if r.status == 429:
					logger.error("Rate limited fetching lists")
					await wait_for_rate_limit_async(session, 'core')
					return {}
				text = await r.text()
				soup = BeautifulSoup(text, 'html.parser')
				fn = f'{CACHE_DIR}/starlist.tmp'
				with open(fn, 'w') as f:
					f.write(str(soup))
				logger.debug(f'Wrote cache for to {fn}')

		listsoup = soup.find_all('div', attrs={"id": "profile-lists-container"})
		list_items = listsoup[0].find_all('a', attrs={'class': 'd-block Box-row Box-row--hover-gray mt-0 color-fg-default no-underline'})  # type: ignore
		# logger.debug(f'list_items: {len(list_items)} listsoup: {len(listsoup)}')

		# Prepare list metadata
		list_meta = []
		for item in list_items:
			listname = item.find('h3').text  # type: ignore
			list_link = f"https://github.com{item.attrs['href']}"  # type: ignore
			list_count_info = item.find('div', class_="color-fg-muted text-small no-wrap").text  # type: ignore
			try:
				list_description = item.select('span', class_="Truncate-text color-fg-muted mr-3")[1].text.strip()  # type: ignore
			except IndexError:
				list_description = ''
			list_meta.append((listname, list_link, list_count_info, list_description))

		# Fetch all list repos concurrently
		tasks = [get_info_for_list_async(meta[1], session, use_cache) for meta in list_meta]
		results = await asyncio.gather(*tasks, return_exceptions=True)

		lists = {}

		for (listname, list_link, list_count_info, list_description), result in zip(list_meta, results):
			list_hrefs = []
			if isinstance(result, Exception):
				logger.warning(f'{result} {type(result)} failed to get list info for {listname}')
				list_hrefs = []
			else:
				for listresult in result:
					if listresult.startswith('/'):
						list_hrefs.append(listresult[1:])
					else:
						list_hrefs.append(listresult)
			lists[listname] = {'href': list_link, 'count': list_count_info, 'description': list_description, 'hrefs': list_hrefs}

	return lists

def save_to_db(data, tablename, db_path='gitstars.db'):
	"""
	Save starred repositories to a SQLite database
	param stars_dict: dict of starred repositories
	param db_path: path to SQLite database file
	"""
	try:
		# engine = create_engine(f'sqlite:///{db_path}')
		conn = sqlite3.connect(db_path)
		df = pd.DataFrame.from_dict(data, orient='index')
		# df.to_sql('starred_repos', con=engine, if_exists='replace', index=False)

		for col in df.columns:
			if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
				df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

		df.to_sql(tablename, conn, if_exists='replace', index=False)
		logger.info(f'Saved {len(df)} starred repositories to {db_path}')
	except Exception as e:
		logger.error(f'Failed to save stars to database: {e}')

def get_args():
	parser = argparse.ArgumentParser(description='GitHub Starred Repositories Manager')
	# parser.add_argument('--use-cache', action='store_true', help='Use cached data if available')
	# parser.add_argument('--max-pages', type=int, default=None, help='Maximum number of pages to fetch')
	parser.add_argument('--refresh', action='store_true', default=False, help='Refresh data from GitHub')
	parser.add_argument('--refresh_lists', action='store_true', default=False, help='Refresh lists from GitHub')
	parser.add_argument('--refresh_stars', action='store_true', default=False, help='Refresh stars from GitHub')
	return parser.parse_args()

async def main():
	if not os.path.exists(CACHE_DIR):
		logger.debug(f'creating cache dir: {CACHE_DIR}')
		os.makedirs(CACHE_DIR)
	use_cache = False
	auth = get_auth_param()
	args = get_args()

	# Check and display rate limits before starting
	limits = get_rate_limit(auth)
	print(f"API Rate limits - Core: {limits.get('core')}")
	_ = [print(f'{k} {limits.get(k)}') for k in limits if limits.get(k).used > 0]

	if args.refresh_lists:
		lists = await get_git_lists_async(auth, use_cache)
		lists_fn = f'{CACHE_DIR}/starred_lists.json'
		try:
			with open(lists_fn, 'w') as f:
				json.dump(lists, f, indent=2)
			logger.debug(f'wrote lists to {lists_fn}')
		except Exception as e:
			logger.error(f'failed to write lists to {lists_fn} {e} {type(e)}')
		total_hrefs = sum([len(lists.get(k).get('hrefs')) for k in lists])
		print(f'got {len(lists)} lists with {total_hrefs} hrefs')
		save_to_db(lists, 'starred_lists', db_path='gitstars.db')
		# Show rate limits after operations
		limits = get_rate_limit(auth)
		print(f"API Rate limits after - Core: {limits.get('core')}")
		_ = [print(f'{k} {limits.get(k)}') for k in limits if limits.get(k).used > 0]
		return

	if args.refresh_stars:
		jsonbuffer, stars_dict = await get_git_stars_async(auth, max=None)
		print(f'got {len(jsonbuffer)} starred repos')
		stars_fn = f'{CACHE_DIR}/starred_repos.json'
		jsonbuffer_fn = f'{CACHE_DIR}/starred_repos_buffer.json'
		try:
			with open(stars_fn, 'w') as f:
				json.dump(stars_dict, f, indent=2)
			logger.debug(f'wrote stars to {stars_fn}')
			with open(jsonbuffer_fn, 'w') as f:
				json.dump(jsonbuffer, f, indent=2)
			logger.debug(f'wrote jsonbuffer to {jsonbuffer_fn}')
		except Exception as e:
			logger.error(f'failed to write stars to {stars_fn} {e} {type(e)}')
		save_to_db(stars_dict, 'starred_repos', db_path='gitstars.db')

		limits = get_rate_limit(auth)
		print(f"API Rate limits after - Core: {limits.get('core')}")
		_ = [print(f'{k} {limits.get(k)}') for k in limits if limits.get(k).used > 0]
		return

	if args.refresh:
		lists = await get_git_lists_async(auth, use_cache)
		lists_fn = f'{CACHE_DIR}/starred_lists.json'
		try:
			with open(lists_fn, 'w') as f:
				json.dump(lists, f, indent=2)
			logger.debug(f'wrote lists to {lists_fn}')
		except Exception as e:
			logger.error(f'failed to write lists to {lists_fn} {e} {type(e)}')
		total_hrefs = sum([len(lists.get(k).get('hrefs')) for k in lists])
		all_hrefs = set()
		for list in lists:
			list_hrefs = lists.get(list).get('hrefs')
			# print(f'list: {list} hrefs: {len(list_hrefs)} all_hrefs: {len(all_hrefs)}')
			for href in list_hrefs:
				all_hrefs.add(href)
		print(f'got {len(lists)} lists with {total_hrefs} hrefs')
		save_to_db(lists, 'starred_lists', db_path='gitstars.db')

		# Show rate limits after operations
		limits = get_rate_limit(auth)
		print(f"API Rate limits after - Core: {limits.get('core')}")
		_ = [print(f'{k} {limits.get(k)}') for k in limits if limits.get(k).used > 0]

		jsonbuffer, stars_dict = await get_git_stars_async(auth, max=None)
		stars_fn = f'{CACHE_DIR}/starred_repos.json'
		jsonbuffer_fn = f'{CACHE_DIR}/starred_repos_buffer.json'
		try:
			with open(stars_fn, 'w') as f:
				json.dump(stars_dict, f, indent=2)
			logger.debug(f'wrote stars to {stars_fn}')
			with open(jsonbuffer_fn, 'w') as f:
				json.dump(jsonbuffer, f, indent=2)
			logger.debug(f'wrote jsonbuffer to {jsonbuffer_fn}')
		except Exception as e:
			logger.error(f'failed to write stars to {stars_fn} {e} {type(e)}')
		print(f'jsonbuffer: {len(jsonbuffer)} stars_dict: {len(stars_dict)}')
		save_to_db(stars_dict, 'starred_repos', db_path='gitstars.db')

		limits = get_rate_limit(auth)
		print(f"API Rate limits after - Core: {limits.get('core')}")
		_ = [print(f'{k} {limits.get(k)}') for k in limits if limits.get(k).used > 0]

		starred_full_names = set(repo['full_name'] for repo in stars_dict.values())
		missing_in_stars = all_hrefs - starred_full_names
		not_in_any_list = starred_full_names - all_hrefs
		if missing_in_stars:
			print(f'{len(missing_in_stars)} repos from lists missing from stars')
			for missing in missing_in_stars:
				print(f'\trepo: {missing}')
		if not_in_any_list:
			print(f'{len(not_in_any_list)} repos not in any list:')
		return

if __name__ == '__main__':
	# todo add argparse
	# todo handle cache better
	asyncio.run(main())
