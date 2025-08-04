import os
from pathlib import Path
from loguru import logger
from subprocess import Popen, PIPE
from datetime import datetime
from requests.auth import HTTPBasicAuth
import aiohttp
from contextlib import asynccontextmanager

async def get_auth_params():
	"""Get authentication parameters from environment variables."""
	username = os.getenv("GITHUB_USERNAME", '')
	token = os.getenv("FINDGITSTOKEN", '')
	if not username or not token:
		logger.error("GITHUB_USERNAME or FINDGITSTOKEN environment variables are not set.")
		return None, None
	return HTTPBasicAuth(username, token)

@asynccontextmanager
async def get_client_session(args):
	auth = HTTPBasicAuth(os.getenv("GITHUB_USERNAME",''), os.getenv("FINDGITSTOKEN",''))
	if not auth:
		logger.error('no auth provided')
	if auth:
		headers = {
			'Accept': 'application/vnd.github+json',
			'Authorization': f'Bearer {auth.password}',
			'X-GitHub-Api-Version': '2022-11-28'}

		async with aiohttp.ClientSession(headers=headers) as session:
			try:
				yield session
			finally:
				await session.close()

def flatten(nested_list):
	flattened = []
	for item in nested_list:
		if isinstance(item, list):
			flattened.extend(flatten(item))
		else:
			flattened.append(item)
	return flattened

def valid_git_folder(k: str) -> bool:
	k = Path(k)
	if Path(k).is_dir():
		if os.path.exists(os.path.join(k, 'config')):
			return True
		else:
			logger.warning(f'{k} not valid missing config')
	else:
		logger.warning(f'{k} {type(k)} not valid ')
	return False

def format_bytes(num_bytes):
	"""Format a byte value as a string with a unit prefix (TB, GB, MB, KB, or B).
	Args: num_bytes (int): The byte value to format.
	Returns: str: A string with a formatted byte value and unit prefix.
	"""
	for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
		if abs(num_bytes) < 1024.0:
			return f"{num_bytes:.2f} {unit}"
		num_bytes /= 1024.0
	return f"{num_bytes:.2f} TB"

def get_remote_url(git_path: str) -> str:
	"""
	Get the remote url of a git folder
	Parameters: git_path: str - path to git folder
	Returns: str - remote url
	"""
	os.chdir(git_path)
	cmdstr = ['git', 'remote', '-v']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	remote_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	remote_url = '[no remote]'
	try:
		remote_url = remote_out[0].split()[1]
	except IndexError as e:
		logger.warning(f'[gr] {e} {type(e)} {git_path=} remote_out: {remote_out} {out=} {err=}')
	except Exception as e:
		logger.warning(f'[gr] {e} {type(e)} {git_path=} remote_out: {remote_out} {out=} {err=}')
	return remote_url

def get_directory_size(directory: str) -> int:
	# directory = Path(directory)
	total = 0
	try:
		for entry in os.scandir(directory):
			if entry.is_symlink():
				break
			if entry.is_file():
				try:
					total += entry.stat().st_size
				except FileNotFoundError as e:
					logger.warning(f'[err] {e} dir:{directory} ')
					continue
			elif entry.is_dir():
				try:
					total += get_directory_size(entry.path)
				except FileNotFoundError as e:
					logger.warning(f'[err] {e} dir:{directory} ')
	except NotADirectoryError as e:
		logger.warning(f'[err] {e} dir:{directory} ')
		return os.path.getsize(directory)
	# except (PermissionError, FileNotFoundError) as e:
	# 	logger.warning(f'[err] {e} {type(e)} dir:{directory} ')
	# 	return 0
	# logger.debug(f'[*] get_directory_size {directory} {total} bytes')
	return total

def get_subfilecount(directory: str) -> int:
	directory = Path(directory)
	try:
		filecount = len([k for k in directory.glob('**/*') if k.is_file()])
	except PermissionError as e:
		logger.warning(f'[err] {e} d:{directory}')
		return 0
	return filecount


def get_subdircount(directory: str) -> int:
	directory = Path(directory)
	dc = 0
	try:
		dc = len([k for k in directory.glob('**/*') if k.is_dir()])
	except (PermissionError, FileNotFoundError) as e:
		logger.warning(f'[err] {e} d:{directory}')
	return dc

def ensure_datetime(dt_value):
	"""Convert a value to datetime if it's not already, or return None if conversion fails"""
	if dt_value is None:
		return None
	if isinstance(dt_value, datetime):
		return dt_value

	# Try to convert string to datetime if it's a string
	if isinstance(dt_value, str):
		try:
			return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
		except (ValueError, TypeError):
			try:
				return datetime.strptime(dt_value, '%Y-%m-%dT%H:%M:%SZ')
			except (ValueError, TypeError):
				pass

	# If we can't convert it, return None
	logger.warning(f"Could not convert to datetime: {dt_value} (type: {type(dt_value)})")
	return None

if __name__ == '__main__':
	pass
