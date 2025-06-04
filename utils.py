import os
from pathlib import Path
from loguru import logger
from subprocess import Popen, PIPE
import aiohttp
import asyncio
import aiofiles

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

def get_remote(git_path: str) -> str:
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
		pass  # logger.warning(f'[gr] {e} {type(e)} {git_path=} remote_out: {remote_out}')
	except Exception as e:
		logger.warning(f'[gr] {e} {type(e)} {git_path=} remote_out: {remote_out}')
	return remote_url


if __name__ == '__main__':
	pass
