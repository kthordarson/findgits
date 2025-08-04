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

def git_fsck_check(git_path: str) -> dict:
	"""
	Run git fsck --full to check for repository errors
	Parameters: git_path: str - path to git folder
	Returns: dict - fsck results with status and errors
	"""
	original_dir = os.getcwd()
	fsck_result = {
		'fsck_status': None,
		'fsck_errors': [],
		'is_corrupted': False
	}

	if git_path == '[notcloned]':
		fsck_result['fsck_status'] = 'not_applicable'
		return fsck_result

	try:
		os.chdir(git_path)

		# Run git fsck --full to check for repository errors
		fsck_cmd = ['git', 'fsck', '--full']
		fsck_out, fsck_err = Popen(fsck_cmd, stdout=PIPE, stderr=PIPE).communicate()

		if fsck_err:
			fsck_errors = [k.strip() for k in fsck_err.decode('utf8').split('\n') if k.strip()]
			fsck_result['fsck_errors'] = fsck_errors
			fsck_result['fsck_status'] = 'errors_found'
			fsck_result['is_corrupted'] = True
			logger.warning(f'git fsck errors in {git_path}: {fsck_errors}')
		else:
			fsck_result['fsck_status'] = 'clean'
			if fsck_out.decode('utf8').strip():
				# Sometimes fsck outputs warnings to stdout
				fsck_output = [k.strip() for k in fsck_out.decode('utf8').split('\n') if k.strip()]
				if fsck_output:
					fsck_result['fsck_errors'] = fsck_output
					fsck_result['fsck_status'] = 'warnings_found'
					logger.info(f'git fsck warnings in {git_path}: {fsck_output}')

	except Exception as fsck_e:
		fsck_result['fsck_status'] = 'fsck_failed'
		fsck_result['fsck_errors'] = [f"fsck command failed: {fsck_e}"]
		logger.warning(f'git fsck command failed in {git_path}: {fsck_e}')
	finally:
		os.chdir(original_dir)

	return fsck_result

def git_recovery_pull(git_path: str) -> dict:
	"""
	Attempt to recover a corrupted git repository using git pull
	Parameters: git_path: str - path to git folder
	Returns: dict - recovery results
	"""
	original_dir = os.getcwd()
	recovery_result = {
		'recovery_attempted': False,
		'recovery_successful': False,
		'recovery_method': 'git_pull',
		'recovery_steps': [],
		'final_status': None,
		'error_message': None
	}

	try:
		os.chdir(git_path)
		recovery_result['recovery_attempted'] = True

		logger.info(f"Attempting git recovery using pull for {git_path}")

		# Step 1: Check if remote is available
		remote_cmd = ['git', 'remote', '-v']
		remote_out, remote_err = Popen(remote_cmd, stdout=PIPE, stderr=PIPE).communicate()

		if remote_err or not remote_out:
			recovery_result['error_message'] = 'No remote found or remote command failed'
			recovery_result['final_status'] = 'no_remote'
			return recovery_result

		recovery_result['recovery_steps'].append('remote_check_passed')

		# Step 2: Try to fetch from remote first
		logger.info(f"Fetching from remote for {git_path}")
		fetch_cmd = ['git', 'fetch', 'origin']
		fetch_out, fetch_err = Popen(fetch_cmd, stdout=PIPE, stderr=PIPE).communicate()

		if fetch_err:
			logger.warning(f"Fetch failed for {git_path}: {fetch_err.decode('utf8')}")
			recovery_result['recovery_steps'].append('fetch_failed')
		else:
			recovery_result['recovery_steps'].append('fetch_successful')

		# Step 3: Get current branch or try to determine default branch
		branch_cmd = ['git', 'branch', '--show-current']
		branch_out, branch_err = Popen(branch_cmd, stdout=PIPE, stderr=PIPE).communicate()

		current_branch = branch_out.decode('utf8').strip()
		if not current_branch:
			# Try to get default branch from remote
			default_branch_cmd = ['git', 'symbolic-ref', 'refs/remotes/origin/HEAD']
			default_out, default_err = Popen(default_branch_cmd, stdout=PIPE, stderr=PIPE).communicate()

			if default_out:
				current_branch = default_out.decode('utf8').strip().split('/')[-1]
			else:
				# Fallback to common branch names
				for branch_name in ['main', 'master', 'develop']:
					check_branch_cmd = ['git', 'show-ref', '--verify', '--quiet', f'refs/remotes/origin/{branch_name}']
					check_out, check_err = Popen(check_branch_cmd, stdout=PIPE, stderr=PIPE).communicate()
					if not check_err:
						current_branch = branch_name
						break

		if not current_branch:
			recovery_result['error_message'] = 'Could not determine branch to pull from'
			recovery_result['final_status'] = 'no_branch_found'
			return recovery_result

		recovery_result['recovery_steps'].append(f'branch_determined:{current_branch}')

		# Step 4: Try to reset to remote branch
		logger.info(f"Resetting to origin/{current_branch} for {git_path}")
		reset_cmd = ['git', 'reset', '--hard', f'origin/{current_branch}']
		reset_out, reset_err = Popen(reset_cmd, stdout=PIPE, stderr=PIPE).communicate()

		if reset_err:
			logger.warning(f"Reset failed for {git_path}: {reset_err.decode('utf8')}")
			recovery_result['recovery_steps'].append('reset_failed')

			# Step 5: If reset fails, try pull with allow-unrelated-histories
			logger.info(f"Trying pull with allow-unrelated-histories for {git_path}")
			pull_cmd = ['git', 'pull', 'origin', current_branch, '--allow-unrelated-histories', '--strategy=ours']
			pull_out, pull_err = Popen(pull_cmd, stdout=PIPE, stderr=PIPE).communicate()

			if pull_err:
				recovery_result['error_message'] = f"Pull failed: {pull_err.decode('utf8')}"
				recovery_result['final_status'] = 'pull_failed'
				recovery_result['recovery_steps'].append('pull_failed')
				return recovery_result
			else:
				recovery_result['recovery_steps'].append('pull_successful')
		else:
			recovery_result['recovery_steps'].append('reset_successful')

		# Step 6: Verify recovery by running fsck again
		verification_result = git_fsck_check(git_path)
		if verification_result['fsck_status'] == 'clean':
			recovery_result['recovery_successful'] = True
			recovery_result['final_status'] = 'recovered_clean'
			logger.info(f"Successfully recovered git repository: {git_path}")
		elif verification_result['fsck_status'] == 'warnings_found':
			recovery_result['recovery_successful'] = True
			recovery_result['final_status'] = 'recovered_with_warnings'
			logger.info(f"Recovered git repository with warnings: {git_path}")
		else:
			recovery_result['final_status'] = 'recovery_incomplete'
			recovery_result['error_message'] = 'Repository still has errors after recovery attempt'

		recovery_result['recovery_steps'].append(f'verification:{verification_result["fsck_status"]}')

	except Exception as e:
		recovery_result['error_message'] = f"Recovery exception: {e}"
		recovery_result['final_status'] = 'recovery_exception'
		logger.error(f"Git recovery failed for {git_path}: {e}")
	finally:
		os.chdir(original_dir)

	return recovery_result

def get_git_info(git_path: str) -> dict:
	"""
	Get git branch information from a git folder
	Parameters: git_path: str - path to git folder
	Returns: dict - git branch information including current branch, local branches, and remotes
	"""
	original_dir = os.getcwd()
	git_info = {
		'current_branch': None,
		'local_branches': [],
		'remote_branches': [],
		'tracking_info': {},
		'error': None,
		'fsck_status': None,
		'fsck_errors': [],
		'recovery_attempted': False,
		'recovery_result': None
	}
	if git_path == '[notcloned]':
		return git_info
	try:
		os.chdir(git_path)

		# First run git fsck to check for repository errors
		fsck_result = git_fsck_check(git_path)
		git_info.update(fsck_result)

		# If repository is corrupted, attempt recovery
		if fsck_result['is_corrupted']:
			recovery_result = git_recovery_pull(git_path)
			git_info['recovery_attempted'] = recovery_result['recovery_attempted']
			git_info['recovery_result'] = recovery_result

			# Update fsck status after recovery
			if recovery_result['recovery_successful']:
				# Re-run fsck to get updated status
				updated_fsck = git_fsck_check(git_path)
				git_info.update(updated_fsck)

		# Continue with branch information only if repository is in good state
		if git_info['fsck_status'] in ['clean', 'warnings_found', 'recovered_clean', 'recovered_with_warnings']:
			cmdstr = ['git', '--no-pager', 'branch', '-a', '-l', '--no-color', '-vv']
			out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()

			if err:
				git_info['error'] = err.decode('utf8').strip()
				logger.warning(f'git branch error in {git_path}: {git_info["error"]}')
				return git_info

			branch_lines = [k.strip() for k in out.decode('utf8').split('\n') if k.strip()]

			for line in branch_lines:
				if not line:
					continue

				# Check if this is the current branch (starts with *)
				is_current = line.startswith('*')
				if is_current:
					line = line[1:].strip()  # Remove the *

				# Split the line into components
				parts = line.split()
				if len(parts) < 2:
					continue

				branch_name = parts[0]
				commit_hash = parts[1]

				# Extract tracking info (e.g., [ahead 1], [behind 11])
				tracking_info = None
				if '[' in line and ']' in line:
					start_bracket = line.find('[')
					end_bracket = line.find(']')
					tracking_info = line[start_bracket+1:end_bracket]

				# Parse different branch types
				if branch_name.startswith('remotes/'):
					# Remote branch
					if '->' in line:
						# This is a symbolic ref like "remotes/origin/HEAD -> origin/master"
						target = line.split('->')[-1].strip()
						git_info['remote_branches'].append({
							'name': branch_name,
							'commit': commit_hash,
							'type': 'symbolic_ref',
							'target': target,
							'tracking': tracking_info
						})
					else:
						# Regular remote branch
						git_info['remote_branches'].append({
							'name': branch_name,
							'commit': commit_hash,
							'type': 'remote',
							'tracking': tracking_info
						})
				else:
					# Local branch
					branch_info = {
						'name': branch_name,
						'commit': commit_hash,
						'is_current': is_current,
						'tracking': tracking_info
					}

					git_info['local_branches'].append(branch_info)

					if is_current:
						git_info['current_branch'] = branch_name

					# Store tracking info in separate dict for easy lookup
					if tracking_info:
						git_info['tracking_info'][branch_name] = tracking_info

			# Additional parsing for commit messages (everything after commit hash and tracking info)
			for line in branch_lines:
				if not line or line.startswith('*'):
					line = line[1:].strip() if line.startswith('*') else line

				parts = line.split()
				if len(parts) >= 3:
					branch_name = parts[0]
					# Find commit message (after hash and optional tracking info)
					line_parts = line.split()
					if len(line_parts) > 2:
						# Skip branch name and commit hash
						remaining = ' '.join(line_parts[2:])
						# Remove tracking info if present
						if '[' in remaining and ']' in remaining:
							bracket_end = remaining.find(']') + 1
							commit_msg = remaining[bracket_end:].strip()
						else:
							commit_msg = remaining

						# Add commit message to the appropriate branch
						if branch_name.startswith('remotes/'):
							for remote_branch in git_info['remote_branches']:
								if remote_branch['name'] == branch_name:
									remote_branch['commit_message'] = commit_msg
									break
						else:
							for local_branch in git_info['local_branches']:
								if local_branch['name'] == branch_name:
									local_branch['commit_message'] = commit_msg
									break

	except Exception as e:
		git_info['error'] = f"{e} {type(e)}"
		logger.warning(f'[get_git_info] {e} {type(e)} {git_path=}')
	finally:
		os.chdir(original_dir)

	return git_info

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
