from loguru import logger
import random
import os
from pathlib import Path
from datetime import datetime, timedelta
import glob


def generate_id():
	return ''.join(random.choices('0123456789abcdef', k=16))

def get_directory_size(directory):
	#directory = Path(directory)
	total = 0
	try:
		for entry in os.scandir(directory):
			if entry.is_symlink():
				break
			if entry.is_file():
				total += entry.stat().st_size
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

def get_subfilecount(directory):
	directory = Path(directory)
	try:
		filecount = len([k for k in directory.glob('**/*') if k.is_file()])
	except PermissionError as e:
		logger.warning(f'[err] {e} d:{directory}')
		return 0
	return filecount

def get_subdircount(directory):
	directory = Path(directory)
	dc = 0
	try:
		dc = len([k for k in directory.glob('**/*') if k.is_dir()])
	except (PermissionError,FileNotFoundError) as e:
		logger.warning(f'[err] {e} d:{directory}')
	return dc

def valid_git_folder(k):
	k = Path(k)
	if Path(k).is_dir():
		if os.path.exists(os.path.join(k, 'config')):
			return True
		else:
			logger.warning(f'{k} not valid missing config')
	else:
		logger.warning(f'{k} {type(k)} not valid ')
	return False

def get_folder_list(startpath):
	return [Path(path).parent for path,subdirs,files in os.walk(startpath) if path.endswith('.git') and os.path.exists(path+'/config')]
	#return [Path(path).parent for path,subdirs,files in os.walk(startpath) if path.endswith('.git') and valid_git_folder(path)]

def zget_folder_list(startpath):
	# [path for path,subdirs,files in os.walk(startpath) if path.endswith('.git')]
	for k in glob.glob(str(Path(startpath))+'/**/.git/',recursive=True, include_hidden=True):
		if valid_git_folder(k):
			yield Path(k).parent

def xget_folder_list(startpath):
	for k in glob.glob(str(Path(startpath))+'/**/.git',recursive=True, include_hidden=True):
		if Path(k).is_dir() and Path(k).name == '.git':
			if os.path.exists(os.path.join(Path(k), 'config')):
				yield Path(k).parent
