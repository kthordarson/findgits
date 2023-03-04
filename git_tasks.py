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


def run_scanpath_threads(path_id: str, session: sessionmaker):
	t0 = datetime.now()
	git_sp = session.query(GitParentPath).filter(GitParentPath.id == path_id).first()
	git_entries = session.query(GitFolder).filter(GitFolder.parent_id == git_sp.id).all()
	logger.info(f'[scanpath_threads] scanning {git_sp.folder} id={git_sp.id} existing_entries={len(git_entries)}')
	tasks = []
	results = []
	for gf in git_entries:
		gf.refresh()
		#logger.debug(f'[spt] tasks={len(tasks)}')
		session.add(gf)
	session.commit()
	logger.debug(f'[spt] gitpathscan t = {datetime.now() - t0}')
	git_repos = session.query(GitRepo).filter(GitRepo.parent_id == git_sp.id).all()
	for gr in git_repos:
		gr.refresh()
		session.add(gr)
	session.commit()
	logger.debug(f'[spt] gitreposcan t = {datetime.now() - t0}')


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
			tasks.append(executor.submit(get_folder_list, git_parentpath))
			git_parentpath.last_scan = datetime.now()
			logger.debug(f'[runscan] {datetime.now() - t0} {git_parentpath} {git_parentpath.first_scan} {git_parentpath.last_scan} get_folder_list threads {len(tasks)} ')
		for res in as_completed(tasks):
			r = res.result()
			gitparent_ = r["gitparent"]
			gitparent = session.query(GitParentPath).filter(GitParentPath.folder == gitparent_.folder).first()
			gitparent.last_scan = datetime.now()
			git_folders = r["res"]
			scan_time = r["scan_time"]
			gitparent.scan_time = scan_time
			# session.add(gitparent)
			session.add(gitparent)
			session.commit()
			# gfl.append(r['res'])
			logger.info(f'[runscan] {datetime.now() - t0} {len(git_folders)} gitfolders from {gitparent} scan_time={scan_time} {gitparent.scan_time} ')
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
						session.add(folder_check)
						session.add(gr_update)
						session.commit()
						ups += 1
			# logger.debug(f'[runscan] gitparent={gitparent} fc={len(folder_check)} gc0={folder_check[0]} updatefolder_={updatefolder_} gf_update={gf_update} gr_update={gr_update}')
			logger.debug(f'[runscan] {datetime.now() - t0} {len(r["res"])} gitfolders from {r["gitparent"]} entries {cnt}/{ups} ')

	gsp = session.query(GitParentPath).all()
	repos = session.query(GitRepo).all()
	folders = session.query(GitFolder).all()
	results = {'gsp': len(gsp), 'repos': len(repos), 'folders': len(folders)}
	return results


def collect_git_folders(gitfolders: list, session: sessionmaker) -> None:
	# create GitFolder objects from gitfolders
	for k in gitfolders:
		g = session.query(GitFolder).filter(GitFolder.git_path == str(k.git_path)).first()
		try:
			if g:
				# existing gitfolder found, refresh
				g.refresh()
				session.add(g)
				session.commit()
			else:
				# new gitfolder found, add to db
				session.add(k)
				session.commit()
		# logger.debug(f'[!] New: {k} ')
		except OperationalError as e:
			logger.error(f'[E] {e} g={g}')
			continue
		finally:
			session.close()
	logger.debug(f'[collect_git_folders] gitfolders={len(gitfolders)}')


def collect_git_folder(gitfolder, session) -> None:
	# create GitFolder objects from gitfolders
	g = session.query(GitFolder).filter(GitFolder.git_path == str(gitfolder.git_path)).first()
	try:
		if g:
			# existing gitfolder found, refresh
			g.refresh()
			session.add(g)
			session.commit()
		else:
			# new gitfolder found, add to db
			session.add(g)
			session.commit()
	# logger.debug(f'[!] New: {k} ')
	except OperationalError as e:
		logger.error(f'[E] {e} g={g}')
	finally:
		session.close()


def collect_repo(gf: GitFolder, session: sessionmaker) -> str:
	try:
		# construct repo object from gf (folder)
		gr = GitRepo(gf)
	except MissingConfigException as e:
		# logger.error(f'[cgr] {e} gf={gf}')
		return f'[cgr] MissingConfigException {e} gf={gf}'
	except TypeError as e:
		errmsg = f'[cgr] TypeError {e} gf={gf}'
		logger.error(errmsg)
		raise TypeError(errmsg)

	repo_q = session.query(GitRepo).filter(GitRepo.giturl == str(gr.giturl)).first()
	folder_q = session.query(GitRepo).filter(GitRepo.git_path == str(gr.git_path)).first()
	if repo_q and folder_q:
		# todo: check if repo exists in other folder somewhere...
		pass
	# repo_q.refresh()
	# session.add(repo_q)
	# session.commit()
	else:
		# new repo found, add to db
		try:
			session.add(gr)
		except IntegrityError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except IllegalStateChangeError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		# logger.debug(f'[!] newgitrepo {gr} ')
		try:
			session.commit()
		except IntegrityError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except IllegalStateChangeError as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			return errmsg
		except Exception as e:
			errmsg = f'[cgr] {e} {type(e)} gf={gf} gr={gr}]'
			logger.error(errmsg)
			# session.rollback()
			raise Exception(errmsg)
	return 'done'


def get_parent_entries(session: sessionmaker) -> list:
	gpf = session.query(GitParentPath).all()
	return [k for k in gpf if os.path.exists(k.folder)]


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
	# gsp_entries = get_parent_entries(session)
	path_check = None
	path_check = session.query(GitParentPath).filter(GitParentPath.folder == newpath).all()
	if len(path_check) == 0:
		gsp = GitParentPath(newpath)
		session.add(gsp)
		session.commit()
		logger.debug(f'[add_path] adding {gsp} to db')
	else:
		logger.warning(f'[add_path] path={newpath} already in config')
	return gsp


def scanpath_thread(gf: GitFolder, argsdbmode: str) -> None:
	engine = get_engine(dbtype=argsdbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	try:
		cr = collect_repo(gf, session)
	except TypeError as e:
		logger.error(f'[!] TypeError {e} gf={gf}')
		return None
	except AttributeError as e:
		logger.error(f'[!] AttributeError {e} gf={gf}')
		return None
	except IntegrityError as e:
		logger.error(f'[!] IntegrityError {e} {type(e)} gf={gf}')
	except InvalidRequestError as e:
		errmsg = f'[!] InvalidRequestError {e} {type(e)} gf={gf}'
		logger.error(errmsg)
		raise InvalidRequestError(errmsg)


def scanpath(gsp: GitParentPath, argsdbmode: str) -> None:
	engine = get_engine(dbtype=argsdbmode)
	Session = sessionmaker(bind=engine)
	session = Session()
	# scan a single path, scanpath is an int corresponding to id of GitParentPath to scan
	gitfolders = []
	gfl = get_folder_list(gsp)
	logger.info(f'[scanpath] scanpath={gsp.folder} gsp={gsp} found {len(gfl["res"])} gitfolders')
	for g in gfl['res']:
		git_folder = session.query(GitFolder).filter(GitFolder.git_path == str(g)).first()
		if not git_folder:
			# new git folder
			git_folder = GitFolder(g, gsp)
		else:
			git_folder.refresh()
		session.add(git_folder)
		git_repo = session.query(GitRepo).filter(GitRepo.git_path == git_folder.git_path).first()
		if not git_repo:
			# new git repo
			git_repo = GitRepo(git_folder)
		else:
			git_repo.refresh()
		session.add(git_repo)
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
			print(f'[dbinfo] gpf={gpf} git_folders={git_folders} git_repos={git_repos} scantime={gpf.scan_time} {gpf.scan_time / git_folders} {gpf.scan_time / git_repos}')


# git_repos = session.query(GitRepo).all()
# dupe_v = session.query()
# sql = text('select * from dupeview;')
# dupes = [k._asdict() for k in session.execute(sql).fetchall()]

# sql = text('select * from nodupes;')
# nodupes = [k._asdict() for k in session.execute(sql).fetchall()]

# sql = text('select * from gitrepo where dupe_flag = 1;')
# dupetest = [k._asdict() for k in session.execute(sql).fetchall()]

# sql = text('select * from gitrepo where dupe_flag is NULL;')
# nodupetest = [k._asdict() for k in session.execute(sql).fetchall()]

# logger.info(f'[dbinfo] parent_folders={len(parent_folders)} git_folders={len(git_folders)} git_repos={len(git_repos)} dupes={len(dupes)} / {len(dupetest)} nodupes={len(nodupes)} / NULL {len(nodupetest)}')

# noinspection PyProtectedMember
def xshow_dbinfo(session: sessionmaker) -> None:
	# mysql 'select (select count(*) from gitfolder) as fc, (select count(*) from gitrepo) as rc, (select count(*) from gitparentpath) as fpc, (select count(*) from dupeview) as dc';
	# sqlite3 gitrepo1.db 'select (select count(*) from gitparentpath) as gpc, (select count(*) from gitfolder) as gfc, (select count(*) from gitrepo) as grc ' -table
	parent_folders = session.query(GitParentPath).all()
	git_folders = session.query(GitFolder).all()
	git_repos = session.query(GitRepo).all()
	dupe_v = session.query()
	sql = text('select * from dupeview;')
	dupes = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from nodupes;')
	nodupes = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from gitrepo where dupe_flag = 1;')
	dupetest = [k._asdict() for k in session.execute(sql).fetchall()]

	sql = text('select * from gitrepo where dupe_flag is NULL;')
	nodupetest = [k._asdict() for k in session.execute(sql).fetchall()]

	logger.info(f'[dbinfo] parent_folders={len(parent_folders)} git_folders={len(git_folders)} git_repos={len(git_repos)} dupes={len(dupes)} / {len(dupetest)} nodupes={len(nodupes)} / NULL {len(nodupetest)}')


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
	os.chdir(gitrepo.folder)
	cmdstr = ['git', 'show', '--raw', '--format="%aI %H %T %P %ae subject=%s"']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	show_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return show_out


def get_git_status(gitrepo: GitRepo) -> list:
	# git -P log    --format="%aI %H %T %P %ae subject=%s"
	os.chdir(gitrepo.folder)
	cmdstr = ['git', 'status', '-s']
	out, err = Popen(cmdstr, stdout=PIPE, stderr=PIPE).communicate()
	status_out = [k.strip() for k in out.decode('utf8').split('\n') if k]
	return status_out
