import glob
import os
import random
from pathlib import Path
from loguru import logger

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

def check_update_dupes(session) -> dict:
	"""
	Check for duplicate GitRepo entries (same git_url) and update their dupe_flag and dupe_count.
	A duplicate repository is one with the same git_url in multiple locations.

	Parameters:
		session: SQLAlchemy session

	Returns:
		dict: Summary of results containing:
			- total_repos: Total number of repositories
			- unique_repos: Number of unique repositories
			- dupe_repos: Number of repositories that are duplicates
			- dupes_updated: Number of repositories updated
	"""
	from dbstuff import get_dupes, GitRepo
	from sqlalchemy import text

	logger.info("Checking for duplicate repositories...")

	# Get all repositories
	all_repos = session.query(GitRepo).all()
	total_repos = len(all_repos)

	# Reset duplicate flags on all repos
	for repo in all_repos:
		repo.dupe_flag = False
		repo.dupe_count = 0

	# Get list of duplicates (repos with same git_url)
	dupes = get_dupes(session)
	dupe_urls = set()
	dupes_updated = 0

	# Process each duplicate group
	for dupe in dupes:
		dupe_id = dupe.id
		dupe_url = dupe.git_url
		dupe_count = dupe.count
		dupe_urls.add(dupe_url)

		# Find all repos with this URL
		same_url_repos = session.query(GitRepo).filter(GitRepo.git_url == dupe_url).all()

		# Update their dupe flags
		for repo in same_url_repos:
			repo.dupe_flag = True
			repo.dupe_count = dupe_count
			dupes_updated += 1

	# Commit the changes
	session.commit()

	# Prepare result summary
	result = {
		'total_repos': total_repos,
		'unique_repos': total_repos - len(dupes),
		'dupe_repos': len(dupe_urls),
		'dupes_updated': dupes_updated
	}

	logger.info(f"Found {result['dupe_repos']} duplicate repo URLs among {total_repos} total repos")
	logger.info(f"Updated {dupes_updated} repository records")

	return result


if __name__ == '__main__':
	pass
