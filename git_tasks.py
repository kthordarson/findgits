#!/usr/bin/python3
import argparse
import os
from pathlib import Path
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


def run_scanpath_threads(gsp:GitParentPath, session: sessionmaker):
	t0 = datetime.now()
	#git_sp = session.query(GitParentPath).filter(GitParentPath.id == path_id).first()
	#git_entries = session.query(GitFolder).filter(GitFolder.parent_id == git_sp.id).all()
	#logger.info(f'[scanpath_threads] scanning {git_sp.folder} id={git_sp.id} existing_entries={len(git_entries)}')
	tasks = []
	results = []
	for gf in gsp.gfl:
		gf_chk = session.query(GitFolder).filter(GitFolder.git_path == str(gf)).first()
		if gf_chk:
			gf_chk.get_stats()
			logger.info(f'get_stats {gf} {gf_chk}')
			session.commit()
			results.append(gf_chk)
		else:
			gf = GitFolder(gitfolder=str(gf), gsp=gsp)
			session.add(gf)
			session.commit()
			results.append(gf)
	return results


def run_full_scan(dbmode: str):
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
			#tasks.append(executor.submit(get_folder_list, git_parentpath))
			git_parentpath.last_scan = datetime.now()
			logger.debug(f'[runscan] {datetime.now() - t0} {git_parentpath} firstscan:{git_parentpath.first_scan} lastscan:{git_parentpath.last_scan} get_folder_list threads {len(tasks)} ')
		for res in as_completed(tasks):
			r = res.result()
			git_folders = r["res"]
			gitparent = session.query(GitParentPath).filter(GitParentPath.id == r["gitparent"]).first()
			#gitparent = session.query(GitParentPath).filter(GitParentPath.folder == gitparent_.folder).first()
			gitparent.last_scan = datetime.now()
			gitparent.scan_time = r["scan_time"]
			gitparent.folder_count = len(git_folders)
			session.commit()
			logger.info(f'[runscan] {datetime.now() - t0} {len(git_folders)} gitfolders from {gitparent} gpscantime={gitparent.scan_time} gitparent.folder_count:{gitparent.folder_count}')
			cnt = 0
			ups = 0
			for gf in git_folders:
				folder_check = None
				folder_check = session.query(GitFolder).filter(GitFolder.git_path == str(gf)).first()
				if not folder_check:
					# add new entries
					_t0_ = datetime.now()
					git_folder = GitFolder(gf, gitparent)
					git_folder.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_folder)
					session.commit()
					_t0_ = datetime.now()
					git_repo = GitRepo(git_folder)
					git_repo.scan_time = (datetime.now() - _t0_).total_seconds()
					session.add(git_repo)
					session.commit
					cnt += 1
				else:
					# update stats for existing entries
					# logger.debug(f'[runscan] gitparent={r["gitparent"]} r={len(r["res"])} fc={len(folder_check)} gc0={folder_check[0]}')
					# for updatefolder_ in folder_check:
					# gf_update = session.query(GitFolder).filter(GitFolder.git_path == str(updatefolder_.git_path)).first()
					folder_check.last_scan = datetime.now()
					# scan_time = (datetime.now() - _t0_).total_seconds()
					# _t0_ = datetime.now()
					gr_update = session.query(GitRepo).filter(GitRepo.git_path == str(folder_check.git_path)).first()
					if gr_update:
						gr_update.last_scan = datetime.now()
						# gru.scan_time = (datetime.now() - _t0_).total_seconds()
						#session.add(folder_check)
						#session.add(gr_update)
						ups += 1
					session.commit()
			# logger.debug(f'[runscan] gitparent={gitparent} fc={len(folder_check)} gc0={folder_check[0]} updatefolder_={updatefolder_} gf_update={gf_update} gr_update={gr_update}')
			logger.debug(f'[runscan] {datetime.now() - t0} {len(r["res"])} gitfolders from {r["gitparent"]} entries {cnt}/{ups} ')

	gsp = session.query(GitParentPath).count()
	repos = session.query(GitRepo).count()
	folders = session.query(GitFolder).count()
	results = {'gsp': gsp, 'repos': repos, 'folders': folders}
	return results

def collect_git_folder(gitfolder, session) -> None:
	# create GitFolder objects from gitfolders
	gitfolder = session.query(GitFolder).filter(GitFolder.git_path == str(gitfolder.git_path)).first()
	try:
		if gitfolder:
			# existing gitfolder found, refresh
			gitfolder.get_stats()
			session.commit()
		else:
			# new gitfolder found, add to db
			session.add(gitfolder)
		session.commit()
	# logger.debug(f'[!] New: {k} ')
	except OperationalError as e:
		logger.error(f'[E] {e} g={gitfolder}')

def add_path(newpath: str, session: sessionmaker) -> GitParentPath:
	# add new path to config  db
	# returns gsp object
	if newpath.endswith('/'):
		newpath = newpath[:-1]
	if not os.path.exists(newpath):
		errmsg = f'[addpath] {newpath} not found'
		logger.warning(errmsg)
		raise MissingGitFolderException(errmsg)

	# check db entries for invalid paths and remove
	path_check = None
	path_check = session.query(GitParentPath).filter(GitParentPath.folder == newpath).first()
	if not path_check:
		gsp = GitParentPath(newpath)
		session.add(gsp)
		session.commit()
		#entries = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).count()
		#logger.info(f'[addpath] scanning newgsp {gsp.folder} id={gsp.id} existing_entries={entries}')
		#scanpath(gsp, session)
		#entries_afterscan = session.query(GitFolder).filter(GitFolder.parent_id == gsp.id).count()
		#logger.info(f'[addpath] scanning {gsp.folder} id={gsp.id} existing_entries={entries} after scan={entries_afterscan}')
		return gsp
	else:
		logger.warning(f'[add_path] path={newpath} {path_check} already in config')
		return path_check



def scanpath(gsp: GitParentPath, session:sessionmaker) -> None:
	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	gitfolders = []
	t0 = datetime.now()
	gfl = get_folder_list(gsp)
	scan_time = (datetime.now() - t0).total_seconds()
	gsp.scan_time = scan_time
	gsp.last_scan = datetime.now()
	session.commit()
	logger.info(f'[scanpath] scanpath={gsp.folder} gsp={gsp} found {len(gfl["res"])} gitfolders t:{scan_time} gsp.scan_time={gsp.scan_time} gflrescantime={gfl["scan_time"]}')
	for g in gfl['res']:
		git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(g)).first()
		if not git_folder:
			# new git folder
			git_folder = GitFolder(g, gsp)
			session.add(git_folder)
		else:
			git_folder.get_stats()
		session.commit()
		git_repo = session.query(GitRepo).filter(GitRepo.git_path == git_folder.git_path).first()
		if not git_repo:
			# new git repo
			git_repo = GitRepo(git_folder)
			session.add(git_repo)
		else:
			git_repo.get_stats()
		session.commit()
	logger.info(f'[scanpath] scanpath={gsp.folder} gsp={gsp} found {len(gfl["res"])} gitfolders done')


def show_dbinfo(session: sessionmaker) -> None:
	# mysql 'select (select count(*) from gitfolder) as fc, (select count(*) from gitrepo) as rc, (select count(*) from gitparentpath) as fpc, (select count(*) from dupeview) as dc';
	# sqlite3 gitrepo1.db 'select (select count(*) from gitparentpath) as gpc, (select count(*) from gitfolder) as gfc, (select count(*) from gitrepo) as grc ' -table
	parent_folders = session.query(GitParentPath).all()
	for gpf in parent_folders:
		git_folders = session.query(GitFolder).filter(GitFolder.parent_id == gpf.id).count()
		git_repos = session.query(GitRepo).filter(GitRepo.parent_id == gpf.id).count()
		if git_repos > 0:
			print(f'[dbinfo] gpf={gpf} git_folders={git_folders} git_repos={git_repos} scan_time={gpf.scan_time} {gpf.scan_time / git_folders} {gpf.scan_time / git_repos}')

def get_folder_list(gitparent: GitParentPath) -> dict:
	startpath = gitparent.folder
	t0 = datetime.now()
	cmdstr = ['find', startpath + '/', '-type', 'd', '-name', '.git']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	g_out = out.decode('utf8').split('\n')
	if err != b'':
		logger.warning(f'[get_folder_list] {cmdstr} {err}')
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
