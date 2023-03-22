#!/usr/bin/python3
import argparse
import os
from pathlib import Path
import glob
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from threading import Thread
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from subprocess import Popen, PIPE
from dbstuff import (GitFolder, GitParentPath, GitRepo)
from dbstuff import get_engine

from dbstuff import MissingGitFolderException, MissingConfigException

CPU_COUNT = cpu_count()

def run_full_scan(dbmode: str) -> dict:
	"""
	Scan all gitparentpaths for gitfolders and gitrepos
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Returns: dict - results of scan
	"""
	t0 = datetime.now()
	engine = get_engine(dbtype=dbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	tasks = []
	gsp = session.query(GitParentPath).all()
	logger.info(f'[runscan] {datetime.now() - t0} CPU_COUNT={CPU_COUNT} gsp={len(gsp)}')
	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
		# start thread for each gitparentpath
		for git_parentpath in gsp:
			tasks.append(executor.submit(git_parentpath.get_git_folders,))
			git_parentpath.last_scan = datetime.now()
			tx0 = (datetime.now() - t0).total_seconds()
			logger.debug(f'[runscan] {tx0} {git_parentpath} firstscan:{git_parentpath.first_scan} lastscan:{git_parentpath.last_scan} get_folder_list threads {len(tasks)} ')
		for res in as_completed(tasks):
			tx1 = datetime.now()
			try:
				r = res.result()
			except DetachedInstanceError as e:
				logger.error(f'[runscan] {e} {type(e)} res = {res}')
			git_paths = r["res"]
			gitparent = session.query(GitParentPath).filter(GitParentPath.id == r["gitparent"]).first()
			gitparent.last_scan = datetime.now()
			gitparent.scan_time = r["scan_time"] # todo: this needs to be fixed
			session.commit()
			tx0 = (datetime.now() - t0).total_seconds()
			tx1_0 = (datetime.now() - tx1).total_seconds()
			logger.info(f'[runscan] timercheck gitparent.scan_time: {gitparent.scan_time} resscantime: {r["scan_time"]} tx1: {tx1_0}')
			#logger.info(f'[runscan] t0:{tx0} t1:{tx1_0} {len(git_folders)} gitfolders from {gitparent} gpscantime={gitparent.scan_time} gitparent.folder_count:{gitparent.folder_count}')
			new_folders = 0
			updated_folders = 0
			for gitpath in git_paths:
				git_folder = None
				git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(gitpath)).first()
				if not git_folder:
					# add new entries
					_t0_ = datetime.now()
					git_folder = GitFolder(gitpath, gitparent)
					sub_git_folder = [k for k in glob.glob(str(Path(git_folder.git_path))+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir()]
					if len(sub_git_folder) > 1:
						git_folder.is_parent = True
						logger.info(f'[*] {git_folder} is a parent folder with {len(sub_git_folder)} subfolders]')
					git_folder.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_folder)
					session.commit()
					_t0_ = datetime.now()
					git_repo = GitRepo(git_folder)
					git_repo.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_repo)
					session.commit
					new_folders += 1
					gitparent.folder_count += git_folder.subdir_count
					gitparent.folder_size += git_folder.folder_size
					gitparent.file_count += git_folder.file_count
					gitparent.repo_count += 1
				else:
					git_folder.last_scan = datetime.now()
					git_repo = session.query(GitRepo).filter(GitRepo.git_path == str(git_folder.git_path)).first()
					if git_repo:
						git_repo.last_scan = datetime.now()
						updated_folders += 1
				session.commit()
			# logger.debug(f'[runscan] gitparent={gitparent} fc={len(folder_check)} gc0={folder_check[0]} updatefolder_={updatefolder_} gf_update={gf_update} gr_update={gr_update}')
			tx0 = (datetime.now() - t0).total_seconds()
			tx0_1 = (datetime.now() - tx1).total_seconds()
			logger.info(f'[runscan] tx0:{tx0} tx0_1:{tx0_1} reslen:{len(r["res"])} gitfolders from {gitparent} newfolders:{new_folders} updates:{updated_folders} ')

	gsp = session.query(GitParentPath).count()
	repos = session.query(GitRepo).count()
	folders = session.query(GitFolder).count()
	results = {'gsp': gsp, 'repos': repos, 'folders': folders}
	return results

def add_path(newpath: str, session: sessionmaker) -> GitParentPath:
	"""
	Add new gitparentpath to db
	Parameters: newpath: str full path to new gitparentpath , session: sessionmaker
	Returns: GitParentPath object if the path is new, None if the path already exists
	raises MissingGitFolderException if the path does not exist
	"""
	if newpath.endswith('/'):
		newpath = newpath[:-1]
	if not os.path.exists(newpath):
		raise MissingGitFolderException(f'[addpath] {newpath} not found')
	gpp = session.query(GitParentPath).filter(GitParentPath.folder == newpath).first()
	if not gpp:
		gsp = GitParentPath(newpath)
		session.add(gsp)
		session.commit()
		return gsp
	else:
		logger.warning(f'[add_path] path={newpath} {path_check} already in config')
		return None

def scanpath(gpp: GitParentPath, session:sessionmaker) -> None:
	"""
	scan a single gitparentpath, create new or update existing gitfolders and commits to db
	Parameters: gpp: GitParentPath object, session: sqlalchemy session
	Returns: None
	"""
	gfl = get_folder_list(gpp)
	logger.info(f'[scanpath] scanpath={gpp.folder} gpp={gpp} found {len(gfl["res"])} gitfolders gpp.scan_time={gpp.scan_time} gflrescantime={gfl["scan_time"]}')
	for g in gfl['res']:
		git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(g)).first()
		if not git_folder:
			# new git folder
			git_folder = GitFolder(g, gpp)
			sub_git_folder = [k for k in glob.glob(str(Path(git_folder.git_path))+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir()]
			if len(sub_git_folder) > 1:
				git_folder.is_parent = True
				logger.info(f'[*] {git_folder} is a parent folder with {len(sub_git_folder)} subfolders]')
			#logger.info(f'[scanpath] new gitfolder={git_folder.git_path} gpp={gpp}')
			session.add(git_folder)
		else:
			git_folder.get_stats()
		gpp.folder_count += git_folder.subdir_count
		gpp.folder_size += git_folder.folder_size
		gpp.file_count += git_folder.file_count
	session.commit()
	repos = get_repos(gpp, session)
	for repo in repos:
		session.add(repo)
	session.commit()
	gpp_folders = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
	logger.info(f'[scanpath] Done gsp={gpp} res:{len(gfl["res"])} gppfolders={gpp_folders}')

def get_repos(gpp: GitParentPath, session: sessionmaker) -> list:
	"""
	scans all git folders in a gitparentpath for git repos returns a list of GitRepo objects.
	Caller must commit new objects to db
	Prameters: gpp: GitParentPath object, session: sessionmaker object
	Returns: list of GitRepo objects
	"""
	repos = []
	for git_folder in gpp.gitfolders:
		git_repo = session.query(GitRepo).filter(GitRepo.git_path == git_folder.git_path).first()
		if not git_repo:
			# new git repo
			git_repo = GitRepo(git_folder)
			gpp.repo_count += 1
			# logger.info(f'[getrepos] new repo={git_repo} gpp={gpp}')
		else:
			git_repo.get_stats()
		repos.append(git_repo)
	return repos

def get_folder_list(gitparent: GitParentPath) -> dict:
	"""
	uses the find command to search all subfolers of gitparent for .git folders.
	returns a dict with the gitparent, a list of git folders, and the time it took to scan
	Parameters: gitparent: GitParentPath object
	Returns: dict with keys 'gitparent', 'res', 'scan_time'
	"""
	# todo: maybe this should be a method of GitParentPath
	startpath = gitparent.folder
	t0 = datetime.now()
	#cmdstr = ['find', startpath + '/', '-type', 'd', '-name', '.git']
	#out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	#g_out = out.decode('utf8').split('\n')
	#if err != b'':
	#	logger.warning(f'[get_folder_list] {cmdstr} {err}')
	# only return folders that have a config file
	g_out = glob.glob(str(Path(startpath))+'/**/.git',recursive=True, include_hidden=True)
	res = [Path(k).parent for k in g_out if os.path.exists(k + '/config')]
	scan_time = (datetime.now() - t0).total_seconds()
	# logger.debug(f'[get_folder_list] {datetime.now() - t0} gitparent={gitparent} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
	return {'gitparent': gitparent, 'res': res, 'scan_time': scan_time}

def get_git_log(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	os.chdir(gitrepo.folder)
	cmdstr = ['git', '-P', 'log', '--format="%aI %H %T %P %ae subject=%s"']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	log_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return log_out

def get_git_show(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	if os.path.exists(gitrepo.git_path):
		try:
			os.chdir(gitrepo.git_path)
		except FileNotFoundError as e:
			logger.error(f'{e} {type(e)} gitrepo={gitrepo}')
			return None
		#cmdstr = ['git', 'show', '--raw', '--format="%aI %H %T %P %ae subject=%s"']
		cmdstr = ['git', 'show', '--raw', '-s', '--format="date:%at%nsubject:%s%ncommitemail:%ce"']

		out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
		if err != b'':
			logger.warning(f'[get_git_show] {cmdstr} {err} {os.path.curdir}')
			return None
		show_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
		dsplit = show_out[0].split(':')[1]
		last_commit = datetime.fromtimestamp(int(dsplit))
		try:
			result = {
				# 'last_commit':datetime.fromisoformat(show_out[0].split('date:')[1]),
				'last_commit':last_commit,
				'subject': show_out[1].split('subject:')[1],
				'commitemail': show_out[2].split('commitemail:')[1]
				}

		except (IndexError, ValueError, AttributeError) as e:
			logger.error(f'{e} {type(e)} sout={show_out} out={out} err={err}')
			return None
		return result
	else:
		logger.warning(f'[get_git_show] gitrepo={gitrepo} does not exist')
		return None

def get_git_status(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	os.chdir(gitrepo.folder)
	cmdstr = ['git', 'status', '-s']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	status_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return status_out
