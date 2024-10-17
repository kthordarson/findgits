#!/usr/bin/python3
import os
from pathlib import Path
import glob
from concurrent.futures import (ProcessPoolExecutor, as_completed)
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime
from multiprocessing import cpu_count
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import DetachedInstanceError
from sqlalchemy.exc import UnboundExecutionError
from sqlalchemy.exc import (OperationalError,)
from subprocess import Popen, PIPE
from dbstuff import (GitFolder, GitRepo)
from dbstuff import get_engine
from dbstuff import get_remote
from dbstuff import MissingGitFolderException, MissingConfigException

CPU_COUNT = cpu_count()


def create_git_folders(args, scan_result: dict) -> int:
	"""
	Scan all gitparentspath in db and create gitfolder objects in db
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Parameters: scan_result : dict - scan result from collect_folders
	Returns: int - number of gitfolders added
	"""
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	logger.info(f'scanning {args.gitsearchpath}')
	for fscanres in scan_result['res']:
		try:
			remoteurl = get_remote(fscanres)
			if not remoteurl:
				logger.warning(f'[cgf] {fscanres} not a git folder')
				continue
			git_repo = session.query(GitRepo).filter(GitRepo.git_url == remoteurl).first()
			if not git_repo:
				git_repo = GitRepo(remoteurl)
				session.add(git_repo)
				session.commit()
				git_repo = session.query(GitRepo).filter(GitRepo.id == git_repo.id).first()
				if args.debug:
					logger.debug(f'[cgf] new git_repo {git_repo}')
			if git_repo.dupe_count > 1:
				git_repo.dupe_flag = True
				logger.info(f'[cgf] git_repo {git_repo} dupes: {git_repo.dupe_count}')
			gitfolder = session.query(GitFolder).filter(GitFolder.git_path == str(fscanres)).first()
			if not gitfolder:
				gitfolder = GitFolder(str(fscanres), git_repo.id)
				gitfolder.scan_count += 1
				session.add(gitfolder)
				session.commit()
				if args.debug:
					logger.debug(f'[cgf] new gitfolder {gitfolder.git_path} ')
			# dupecount = session.query(GitRepo).filter(GitRepo.git_url == git_repo.git_url).count()
			dupecount = session.query(GitFolder).filter(GitFolder.gitrepo_id == git_repo.id).count()
			git_repo.dupe_count = dupecount
			session.add(git_repo)
			session.commit()
		except Exception as e:
			logger.error(f'[cgf] {e} {type(e)} {fscanres=}')
			continue
	session.close()
	return True

def create_git_folder(gitlocalpath, args, session) -> int:
	# engine = get_engine(args)
	# Session = sessionmaker(bind=engine)
	# session = Session()
	remoteurl = get_remote(gitlocalpath)
	git_repo = None
	if not remoteurl:
		logger.warning(f'[cgf] {gitlocalpath} not a git folder')
		return False
	else:
		try:
			git_repo = session.query(GitRepo).filter(GitRepo.git_url == remoteurl).first()
		except OperationalError as e:
			logger.error(f'[cgf] {e} {type(e)}\n{remoteurl=} {gitlocalpath=}\n')
			return False
			# git_repo = GitRepo(remoteurl)  # return False
	if not git_repo:
		git_repo = GitRepo(remoteurl)
		session.add(git_repo)
	session.commit()
	if args.debug:
		logger.debug(f'[cgf] new {git_repo.git_url} {git_repo.dupe_count}')
	git_folder = session.query(GitFolder).filter(GitFolder.git_path == gitlocalpath).first()
	if not git_folder:
		git_folder = GitFolder(gitlocalpath, git_repo.id)
		git_folder.scan_count += 1
		git_folder.gitrepo_id = git_repo.id
		session.add(git_folder)
		session.commit()
		if args.debug:
			logger.debug(f'[cgf] new {gitlocalpath} ')
		# dupecount = session.query(GitRepo).filter(GitRepo.git_url == git_repo.git_url).count()
	dupecount = session.query(GitFolder).filter(GitFolder.gitrepo_id == git_repo.id).count()
	git_repo.dupe_count = dupecount
	if git_repo.dupe_count > 1:
		git_repo.dupe_flag = True
		logger.info(f'[cgf] {git_repo.git_url} dupes: {dupecount} {git_repo.dupe_count} {gitlocalpath=}')
	session.add(git_repo)
	session.commit()
	return True


def create_git_repos(args) -> int:
	engine = get_engine(args)
	Session = sessionmaker(bind=engine)
	session = Session()
	total_res = 0
	try:
		git_folders = session.query(GitFolder).all()
	except OperationalError as e:
		logger.error(f'[cr] {e} {type(e)} ')
		return total_res
	logger.info(f'[cr] {len(git_folders)} gitfolders to scan')
	for gf in git_folders:
		try:
			git_url = get_remote(gf.git_path)
		except TypeError as e:
			logger.error(f'[cr] {e} {type(e)} {gf=}')
			continue
		gitrepo = session.query(GitRepo).filter(GitRepo.git_url == git_url).first()
		gitfolder = session.query(GitFolder).filter(GitFolder.git_path == gf.git_path).first()
		if gitrepo:
			gitfolder.gitrepo_id = gitrepo.id
			gitfolder.scan_count += 1
			gitrepo.scan_count += 1
			session.add(gitrepo)
			session.add(gitfolder)
			session.commit()
			if args.debug:
				logger.info(f'[cr] {gitfolder.scan_count}/{gitrepo.scan_count} update {gitrepo.git_url} in {gitfolder.git_path}')
		else:
			gitrepo = GitRepo()
			gitfolder.scan_count += 1
			gitrepo.scan_count += 1
			session.add(gitrepo)
			session.commit()
			gitfolder.gitrepo_id = gitrepo.id
			session.add(gitfolder)
			session.commit()
			if args.debug:
				logger.debug(f'[cr] new {gitrepo.git_url} in {gitfolder.git_path}')
		total_res += 1
		session.commit()
	session.close()
	return total_res

def collect_folders(args) -> dict:
	"""
	Prameters: dbmode: str - database mode (sqlite, mysql, etc)
	Returns: dict - results of scan {'gitparent' :id of gitparent, 'res': list of gitfolders}
	"""
	t0 = datetime.now()
	engine = get_engine(args)
	s = sessionmaker(bind=engine)
	# session = Session()
	tasks = []
	# for gp in gpp:
	# check_missing(gp, session)
	results = []
	return results

def scanpath(gpp: str, session) -> None:
	"""
	scan a single SearchPath, create new or update existing gitfolders and commits to db
	Parameters: gpp: SearchPath object, session: sqlalchemy session
	Returns: None
	"""
	# gfl = gpp.git_folder_list # get_folder_list(gpp)
	# gfl = [k for k in glob.glob(gpp.folder+'/**/.git',recursive=True, include_hidden=True) if Path(k).is_dir() and k != gpp.folder+'/']
	logger.info(f'[sp] scanning {gpp.folder}')
	# ggf_len = len(gpp.get_git_folders()['res'])
	for idx,g in enumerate(gpp.get_git_folders()['res']):  # gfl['res']:
		if os.path.exists(str(g) + '/.git/config'):
			git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(g)).first()
			if not git_folder:
				# check if git_folder contains subdirs...
				# new git folder
				git_folder = GitFolder(g, gpp)
				# logger.info(f'[sp-{idx}] new {git_folder}')
			else:
				# update
				git_folder.scan_count += 1
				git_folder.get_folder_time()
				git_folder.get_folder_stats(git_folder.id, git_folder.git_path)
				# logger.info(f'[sp] {git_folder} already in db')
			# check if gitrepo exists with this path
			git_repo = None  # session.query(GitRepo).filter(GitRepo.git_path == git_folder.git_path).first()
			if not git_repo:
				try:
					# new git repo
					git_repo = GitRepo(git_folder)
					git_repo.scan_count += 1
				except (MissingGitFolderException, MissingConfigException) as e:
					gf_to_delete = session.query(GitFolder).filter(GitFolder.git_path == git_folder.git_path).first()
					logger.warning(f'[getrepos] {type(e)} {e} path: {git_folder.git_path}\nremoving git_folder={gf_to_delete}')
			if git_repo:
				git_repo.scan_count += 1
				gpp.repo_count += 1
			git_folder.gitrepo_id = git_repo.id
			# git_repo.get_repo_stats()
	session.commit()

def get_repos(gpp: str, session) -> list:
	"""
	scans all git folders in a SearchPath for git repos returns a list of GitRepo objects.
	Caller must commit new objects to db
	Prameters: gpp: SearchPath object, sessionmaker object
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
			pass
			# git_repo.get_repo_stats()
		repos.append(git_repo)
	return repos


def get_folder_list(start_path: str) -> dict:
	"""
	uses the find command to search all subfolers of start_path for .git folders.
	returns a dict with the start_path, a list of git folders, and the time it took to scan
	Parameters: start_path: SearchPath object
	Returns: dict with keys 'start_path', 'res', 'scan_time'
	"""
	# todo: maybe this should be a method of SearchPath
	t0 = datetime.now()
	# cmdstr = ['find', startpath + '/', '-type', 'd', '-name', '.git']
	# out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	# g_out = out.decode('utf8').split('\n')
	# if err != b'':
	# logger.warning(f'[get_folder_list] {cmdstr} {err}')

	# g_out = [str(Path(k).parent) for k in glob.glob(start_path + '/**/.git', recursive=True, include_hidden=True)]
	g_out = [k for k in glob.glob(start_path + '/**/.git', recursive=True, include_hidden=True)]

	# only return folders that have a config file
	res = [str(Path(k).parent) for k in g_out if os.path.exists(k + '/config') if Path(k).is_dir()]
	scan_time = (datetime.now() - t0).total_seconds()
	# logger.debug(f'[get_folder_list] {datetime.now() - t0} start_path={start_path} cmd:{cmdstr} gout:{len(g_out)} out:{len(out)} res:{len(res)}')
	return {'start_path': start_path, 'res': res, 'scan_time': scan_time}


def get_git_log(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	os.chdir(gitrepo.folder)
	cmdstr = ['git', '-P', 'log', '--format="%aI %H %T %P %ae subject=%s"']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	log_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return log_out


def get_git_show(gitrepo: GitRepo) -> dict:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	result = {}
	if os.path.exists(gitrepo.git_path):
		try:
			os.chdir(gitrepo.git_path)
		except FileNotFoundError as e:
			logger.error(f'{e} {type(e)} gitrepo={gitrepo}')
		# cmdstr = ['git', 'show', '--raw', '--format="%aI %H %T %P %ae subject=%s"']
		cmdstr = ['git', 'show', '--raw', '-s', '--format="date:%at%nsubject:%s%ncommitemail:%ce"']

		out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
		if err != b'':
			logger.warning(f'[get_git_show] {cmdstr} {err} {os.path.curdir}')
		show_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
		dsplit = show_out[0].split(':')[1]
		last_commit = datetime.fromtimestamp(int(dsplit))
		result['last_commit'] = last_commit
		result['subject'] = show_out[1].split('subject:')[1]
		result['commitemail'] = show_out[2].split('commitemail:')[1]
		return result
	else:
		resmsg = f'[get_git_show] gitrepo={gitrepo} does not exist'
		logger.warning(resmsg)
		result['result'] = resmsg
		return result


def get_git_status(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	os.chdir(gitrepo.folder)
	cmdstr = ['git', 'status', '-s']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	status_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return status_out


def check_missing(gp: str, session) -> None:
	"""
	Checks for missing gitfolders and gitrepos, removes them from db
	Parameters: gp: SearchPath - to check, sessionmaker object
	Returns: None
	"""
	# logger.info(f'[rs] checking {gp} gitfolders = {len(gp.gitfolders)} ')
	if not os.path.exists(gp.folder):
		logger.error(f'[cgf] {gp.folder} does not exist')
	for gf in gp.gitfolders:  # check for missing folders, remove that gitfolder and gitrepo from db
		# logger.debug(f'\tchecking {gf} {gf.git_path}')
		if not os.path.exists(gf.git_path):
			pass

if __name__ == '__main__':
	pass
