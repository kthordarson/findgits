import glob
import os
import random
from pathlib import Path

from loguru import logger


# from sqlalchemy.ext.declarative import declarative_base


def generate_id() -> str:
	return ''.join(random.choices('0123456789abcdef', k=16))


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


def xxget_folder_list(startpath: str) -> list:
	return [Path(path).parent for path, subdirs, files in os.walk(startpath) if path.endswith('.git') and os.path.exists(path + '/config')]


# return [Path(path).parent for path,subdirs,files in os.walk(startpath) if path.endswith('.git') and valid_git_folder(path)]

def zget_folder_list(startpath):
	# [path for path,subdirs,files in os.walk(startpath) if path.endswith('.git')]
	for k in glob.glob(str(Path(startpath)) + '/**/.git/', recursive=True, include_hidden=True):
		if valid_git_folder(k):
			yield Path(k).parent


def xget_folder_list(startpath):
	for k in glob.glob(str(Path(startpath)) + '/**/.git', recursive=True, include_hidden=True):
		if Path(k).is_dir() and Path(k).name == '.git':
			if os.path.exists(os.path.join(Path(k), 'config')):
				yield Path(k).parent


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


if __name__ == '__main__':
	pass
