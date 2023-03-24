#!/usr/bin/python3
import argparse
import os
from pathlib import Path
import glob
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from threading import Thread
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError, InvalidRequestError, IllegalStateChangeError)
from subprocess import Popen, PIPE
from dbstuff import (GitFolder, GitParentPath, GitRepo)
from dbstuff import get_engine

from dbstuff import MissingGitFolderException, MissingConfigException

CPU_COUNT = cpu_count()

def update_gitfolder_stats(args) -> dict:
	# todo - this might be run in a sperate process/thread
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	tasks = []
	results = {}
	with Session() as session:
		parent_paths = session.query(GitParentPath).all()
		# start thread for each gitparentpath
		with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
			t0x = datetime.now()
			for gpp in parent_paths:
				# set gitpar path stats to 0
				gpp.folder_size = 0
				gpp.folder_count = 0
				gpp.file_count = 0
				t0 = datetime.now()
				gfl = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).all()
				tasks = [executor.submit(gitfolder.get_folder_stats, gitfolder.id, gitfolder.git_path) for gitfolder in gfl]
				for res in as_completed(tasks):
					try:
						r = res.result()
					except BrokenProcessPool as e:
						logger.error(f'BrokenProcessPool {e} res={res}')
					results[r['id']] = r
					gpp.folder_size += r['folder_size']
					gpp.folder_count += r['subdir_count']
					gpp.file_count += r['file_count']
					# gpp.scan_time += r['scan_time']
					gf = session.query(GitFolder).filter(GitFolder.id == r['id']).first()
					gf.folder_size = r['folder_size']
					gf.folder_count = r['subdir_count']
					gf.file_count = r['file_count']
					gf.scan_time = r['scan_time']
					session.commit()
				t1 = (datetime.now() - t0).total_seconds()
				gpp.scan_time = t1
				#gpp.folder_count = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
				gpp.repo_count = session.query(GitRepo).filter(GitRepo.parent_id == gpp.id).count()
				session.commit()
				logger.debug(f'[ugf] {gpp} done t1={t1}  task results {len(results)} ')
	session.commit()
	return results

def create_git_folders(args, scan_result:dict) -> int:
	"""
	Scan all gitparentspath in db and create gitfolder objects in db
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Parameters: scan_result : dict - scan result from collect_folders
	Returns: int - number of gitfolders added
	"""
	t0 = datetime.now()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	tasks = []
	total_res = 0
	with Session() as session:
		for gp_id in scan_result:
			gpp = session.query(GitParentPath).filter(GitParentPath.id == gp_id).first()
			for f in scan_result[gp_id]:
				gitfolder = session.query(GitFolder).filter(GitFolder.git_path == f).first()
				if not gitfolder:
					if f.endswith('.git'): # use git_path without .git
						f = f[:-4]
					gitfolder = GitFolder(f, gpp)
					session.add(gitfolder)
					total_res += 1
			logger.debug(f'[cgf] id:{gp_id} gpp:{gpp} gitfolders:{total_res} ')
			session.commit()
	return total_res

def create_git_repos(args) -> int:
	"""
	Scan all gitfolders in db and create gitrepo objects in db
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Returns: int - number of gitrepos added
	"""
	t0 = datetime.now()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	tasks = []
	total_res = 0
	with Session() as session:
		# grouped by gitparentpath
		for gpp in session.query(GitParentPath.id).all():
			gfl = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).all()
			for gf in gfl:
				repo = session.query(GitRepo).filter(GitRepo.gitfolder_id == gf.id).first()
				if not repo:
					repo = GitRepo(gf)
					session.add(repo)
					total_res += 1
			logger.debug(f'[cgr] gpp:{gpp.id} gfl={len(gfl)} repos:{total_res} ')
			session.commit()
	return total_res

def collect_folders(args) -> dict:
	"""
	Scan all gitparentpaths, creates gitparentpath objects in db
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Returns: dict - results of scan {'gitparent' :id of gitparent, 'res': list of gitfolders}
	"""
	t0 = datetime.now()
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	#session = Session()
	tasks = []
	#for gp in gpp:
	#	check_missing(gp, session)
	results = {}
	with Session() as session:
		gpp = session.query(GitParentPath).all()
		logger.info(f'[cgf] {len(gpp)} gitparentpaths to scan')
		# start thread for each gitparentpath
		with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
			#for git_parentpath in gpp:
			tasks = [executor.submit(git_parentpath.get_git_folders,) for git_parentpath in gpp]
			logger.debug(f'[cgf] collect_folders threads {len(tasks)}')
			for res in as_completed(tasks): # todo put results on queue and start creating gitfolders
				try:
					r = res.result()
				except DetachedInstanceError as e:
					logger.error(f'[cgf] {e} {type(e)} res = {res}')
				git_parentpath = session.query(GitParentPath).filter(GitParentPath.id == r["gitparent"]).first()
				git_folder_list = r['res']
				git_parentpath.git_parentpath = len(git_folder_list)
				git_parentpath.scan_time = r['scan_time']
				# gitparent = session.query(GitParentPath).filter(GitParentPath.id == r["gitparent"]).first()
				git_parentpath.last_scan = datetime.now()
				session.commit()
				total_t = (datetime.now() - t0).total_seconds()
				results[r['gitparent']] = r['res']
				logger.info(f'[cgf] {total_t} done {git_parentpath} gfl={len(git_folder_list)} res:{len(results)}')
	return results

def add_parent_path(newpath: str, session: sessionmaker) -> GitParentPath:
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
		# logger.debug(f'[add_path] scanning {newpath} for git folders ')
		newgpp = GitParentPath(newpath)
		# newgpp.git_folder_list = [str(Path(k).parent) for k in glob.glob(newpath+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir() and k != newpath+'/']
		# newgpp.base_folders = [k for k in glob.glob(newpath+'/**/', include_hidden=True) if Path(k).is_dir() and k != newpath+'/']
		# logger.info(f'[app] {newgpp}')# gfl:{len(newgpp.git_folder_list)} ')#bfl:{len(newgpp.base_folders)}')
		return newgpp
	else:
		logger.warning(f'[app] path={newpath} {gpp} already in config')
		return None

def scan_subfolders(gitparent:GitParentPath) -> list:
	"""
	Scan a parent folder for subfolders that contain .git folders
	Parameters: newpath: str - full path to parent folder
	Returns: list of GitParentPath objects
	"""
	gp_list = []
	newpath = gitparent.folder
	folderlist = [os.path.dirname(k) for k in glob.glob(newpath+'/*/*',recursive=True, include_hidden=True) if Path(k).is_dir()  and '.git' in k]# and os.path.exists(k+'/config')]
	logger.info(f'folderlist={len(folderlist)}')
	for folder in folderlist:
		#fp = Path(folder).parent
		f = folder + '/**/.git'
		#print(f'scanning {f}')
		sub_folderlist = [str(Path(k).parent) for k in glob.glob(f,recursive=True, include_hidden=True) if Path(k).is_dir() and '.git' in k]
		if 'bomberdudearchive' in f:
			print(f)
		if len(sub_folderlist) > 1:
			# logger.info(f'[subf] f:{folder} has {len(sub_folderlist)} subgitfolders')
			#gpp = GitParentPath(folder)
			#gpp.git_folder_list = [k for k in glob.glob(folder+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir() and k != folder+'/']
			#gpp.base_folders = [k for k in glob.glob(folder+'/**/', include_hidden=True) if Path(k).is_dir() and k != folder+'/']
			if folder not in gp_list:
				gp_list.append(folder)
	# gp_list.append(GitParentPath(newpath))
	logger.info(f'[sps] {gitparent} has {len(gp_list)} subfolders with git folders')
	return gp_list

def scanpath(gpp: GitParentPath, session:sessionmaker) -> None:
	"""
	scan a single gitparentpath, create new or update existing gitfolders and commits to db
	Parameters: gpp: GitParentPath object, session: sqlalchemy session
	Returns: None
	"""
	#gfl = gpp.git_folder_list # get_folder_list(gpp)
	#gfl = [k for k in glob.glob(gpp.folder+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir() and k != gpp.folder+'/']
	logger.info(f'[sp] scanning {gpp.folder}')
	for g in gpp.get_git_folders()['res']: #gfl['res']:
		if g.endswith('.git'):
			logger.warning(f'[sp] {gpp} {g} ends with /.git')
			g = g[:-4]
		if os.path.exists(g+'/.git/config'):
			git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(g)).first()
			if not git_folder:
				# new git folder
				git_folder = GitFolder(g, gpp)
				session.add(git_folder)
				logger.info(f'[sp] new {git_folder}')
			else:
				# update
				git_folder.scan_count += 1
				git_folder.get_folder_time()
				git_folder.get_folder_stats(git_folder.id, git_folder.git_path)
				#logger.info(f'[sp] {git_folder} already in db')
			# check if gitrepo exists with this path
			session.commit()
			git_repo = session.query(GitRepo).filter(GitRepo.git_path == git_folder.git_path).first()
			if not git_repo:
				try:
					# new git repo
					git_repo = GitRepo(git_folder)
				except (MissingGitFolderException, MissingConfigException) as e:
					gf_to_delete = session.query(GitFolder).filter(GitFolder.git_path == git_folder.git_path).first()
					logger.warning(f'[getrepos] {type(e)} {e} path: {git_folder.git_path}\nremoving git_folder={gf_to_delete}')
					session.delete(gf_to_delete)
					session.commit()
					continue
				if git_repo:
					session.add(git_repo)
					gpp.repo_count += 1
				# logger.info(f'[getrepos] new repo={git_repo} gpp={gpp}')
			else:
				git_repo.get_repo_stats()
		else:
			logger.warning(f'[sp] {gpp} {g} does not exist')
	session.commit()
	#repos = get_repos(gpp, session)
	#for repo in repos:
#		session.add(repo)
	#session.commit()
	gpp_folders = session.query(GitFolder).filter(GitFolder.parent_id == gpp.id).count()
	logger.info(f'[sp] Done gpp={gpp} gppfolders={gpp_folders}')

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
			try:
				git_repo = GitRepo(git_folder)
			except MissingConfigException as e:
				logger.warning(f'[getrepos] {e}')
				continue
			gpp.repo_count += 1
			# logger.info(f'[getrepos] new repo={git_repo} gpp={gpp}')
		else:
			git_repo.get_repo_stats()
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
	t0 = datetime.now()
	#cmdstr = ['find', startpath + '/', '-type', 'd', '-name', '.git']
	#out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	#g_out = out.decode('utf8').split('\n')
	#if err != b'':
	#	logger.warning(f'[get_folder_list] {cmdstr} {err}')

	g_out = [str(Path(k).parent) for k in glob.glob(gitparent.folder+'/**/.git',recursive=True, include_hidden=True)]

	# only return folders that have a config file
	res = [Path(k).parent for k in g_out if os.path.exists(k + '/config') if Path(k).is_dir()]
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

def check_missing(gp:GitParentPath, session:sessionmaker) -> None:
	"""
	Checks for missing gitfolders and gitrepos, removes them from db
	Parameters: gp: GitParentPath - gitparentpath to check, session: sessionmaker object
	Returns: None
	"""
	# logger.info(f'[rs] checking {gp} gitfolders = {len(gp.gitfolders)} ')
	if not os.path.exists(gp.folder):
		logger.error(f'[cgf] {gp.folder} does not exist')
	for gf in gp.gitfolders: # check for missing folders, remove that gitfolder and gitrepo from db
		#logger.debug(f'\tchecking {gf} {gf.git_path}')
		if not os.path.exists(gf.git_path):
			repo = session.query(GitRepo).filter(GitRepo.gitfolder_id == gf.id).first()
			logger.error(f'[rc] {gf.git_path} not found, linked to {repo} removing both from db')
			session.delete(gf)
			session.delete(repo)
	repos = session.query(GitRepo).filter(GitRepo.parent_id == gp.id).all()
	for repo in repos:
		if not os.path.exists(repo.git_path):
			gitfolder = session.query(GitFolder).filter(GitFolder.git_path == repo.git_path).first()
			logger.error(f'[rc] {repo} and {gitfolder} not found, removing from db')
			if gitfolder:
				session.delete(gitfolder)
			session.delete(repo)
	session.commit()


if __name__ == '__main__':
	pass