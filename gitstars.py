#!/usr/bin/python3
import os
from loguru import logger
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

CACHE_DIR = os.path.join(os.path.expanduser('~'), '.cache', 'gitstars')

def get_git_stars(max=10):
	"""
	get all starred repos
	param auth: HTTPBasicAuth
	"""
	# tmpfn = 'starred.tmp'
	jsonbuffer = []
	star_list = []
	stars_dict = {}
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
	# logger.info(f"[r] {r.status_code}  ")
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
	elif 'link' in r.headers:
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
	else:
		logger.warning(f'[r] {r.status_code} link not in headers: {r.headers} ')
	# logger.info(f"[r] {r.status_code}  apiurl: {apiurl} jsonbuffer: {len(jsonbuffer)} stars_dict: {len(stars_dict)}")
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
