from loguru import logger
import random
import os
from pathlib import Path
from datetime import datetime, timedelta
import glob
from subprocess import Popen, PIPE
from datetime import datetime
from sqlalchemy import Engine
from threading import Thread
from multiprocessing import cpu_count


from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor, as_completed)
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import (ArgumentError, CompileError, DataError,
                            IntegrityError, OperationalError, ProgrammingError, InvalidRequestError)
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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


def xxget_folder_list(startpath):
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

# def repocollectionthread(git_folder:str, gsp:GitParentPath, session):
# 	git_folder = GitFolder(git_folder, gsp)
# 	session.add(git_folder)
# 	session.commit()
# 	git_repo = GitRepo(git_folder)
# 	session.add(git_repo)
# 	session.commit

# def old_runscan(engine:Engine):
# 	#engine = get_engine(dbtype=dbmode)
# 	Session = sessionmaker(bind=engine)
# 	session = Session()
# 	parents = session.query(GitParentPath).all()
# 	fc_threads = []
# 	rcstatus = []
# 	rc_threads = []
# 	#g_threads = []
# 	#folderlist = []
# 	for p in parents:
# 		pfc = FolderCollector(p)
# 		fc_threads.append(pfc)
# 	logger.info(f'[runscan] FolderCollector {len(fc_threads)} threads...')
# 	_ = [p.start() for p in fc_threads]
# 	for p in fc_threads:
# 		try:
# 			p.join()
# 		except Exception as e:
# 			logger.error(f'[!] {e} {type(e)} in {p}')
# 		for folder in p.folders:
# 			rc_threads.append(RepoCollector(folder, engine))
# 	logger.info(f'[runscan] FolderCollector found {len(p.folders)} folders. Starting {len(rc_threads)} RepoCollector threads ')
# 	for k in rc_threads:
# 		k.start()
# 		#_ = [p.start() for p in k]
# 	for rc in rc_threads:
# 		rc.join()
# 		rcstatus.append(rc.collect_status)
# 	#	_ = [r.start() for r in rcs]
# 	logger.debug(f'[runscan] done rcstatus={len(rcstatus)}')
# 	return rcstatus

# class FolderCollector(Thread):
# 	def __init__(self, gsp:GitParentPath):
# 		Thread.__init__(self)
# 		self.gsp = gsp
# 		self.folders = []
# 		self.kill = False

# 	def __repr__(self):
# 		return f'FolderCollector({self.gsp}) f:{len(self.folders)} k:{self.kill}'

# 	def run(self):
# 		#self.folders = [) for
# 		try:
# 			self.folders = [GitFolder(k, self.gsp) for k in get_folder_list(self.gsp.folder)]
# 		except KeyboardInterrupt as e:
# 			self.kill = True
# 			self.join(timeout=1)

# class RepoCollector(Thread):
# 	def __init__(self, git_path, engine:Engine):
# 		Thread.__init__(self)
# 		self.git_path = git_path
# 		self.kill = False
# 		#self.dbmode = dbmode
# 		self.engine = engine # get_engine(dbtype=dbmode)
# 		Session = sessionmaker(bind=self.engine)
# 		self.session = Session()
# 		self.collect_status = 'unknown'

# 		#self.session = session

# 	def run(self):

# 		try:
# 			self.collect_status = collect_repo(self.git_path, self.session)
# 		except AttributeError as e:
# 			errmsg = f'AttributeError {self} {e} self.git_path {self.git_path} engine={self.engine} session={self.session}'
# 			logger.error(errmsg)
# 			self.collect_status = errmsg
# 			raise AttributeError(errmsg)
# 			#self.join(timeout=1)
# 		except InvalidRequestError as e:
# 			errmsg = f'InvalidRequestError {self} {e} self.git_path {self.git_path} '
# 			logger.error(errmsg)
# 			self.collect_status = errmsg
# 			self.session.rollback()
# 		except Exception as e:
# 			errmsg = f'Unhandled Exception {self} {e} {type(e)} self.git_path {self.git_path} '
# 			self.collect_status = errmsg
# 			logger.error(errmsg)


# def xxxrunscan(dbmode):
# 	CPU_COUNT = cpu_count()

# 	t0 = datetime.now()
# 	engine = get_engine(dbtype=dbmode)
# 	Session = sessionmaker(bind=engine)
# 	session = Session()
# 	gsp = session.query(GitParentPath).all()
# 	gitfolders = []
# 	tasks = []
# 	gfl = []
# 	logger.info(f'[runscan] {datetime.now()-t0} CPU_COUNT={CPU_COUNT} gsp={len(gsp)}')
# 	folder_entries = []
# 	tasks = []
# 	results = []
# 	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
# 		for git_f in gsp:
# 			tasks.append(executor.submit(get_folder_list, git_f))
# 		logger.debug(f'[runscan] {datetime.now()-t0} gfl threads {len(tasks)} gsp={len(gsp)}')
# 		for res in as_completed(tasks):
# 			r = res.result()
# 			#gfl.append(r['res'])
# 			for gf in r['res']:
# 				session.add(GitFolder(gf, git_f))
# 			session.commit()
# 			logger.debug(f'[runscan] {datetime.now()-t0} gfl {len(r["res"])}')

# 		for git_pp in session.query(GitParentPath).all():
# 			gfe = [k for k in session.query(GitFolder).filter(GitFolder.parent_id == git_pp.id).all()]
# 			folder_entries.append(gfe)
# 		logger.debug(f'[runscan] {datetime.now()-t0} gfe {len(folder_entries)} ')
# 	threadend = datetime.now()

# 	tasks = []
# 	results = []

# 	tasks = []
# 	with ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
# 		gfe = [k for k in session.query(GitFolder).all()]
# 		for g in gfe:
# 			tasks.append(executor.submit(scanpath_thread, g, dbmode))
# 		logger.debug(f'[runscan] {datetime.now()-t0} spt {len(tasks)}')
# 		for res in as_completed(tasks):
# 			r = res.result()
# 			results.append(r)
# 		logger.debug(f'[runscan] {datetime.now()-t0} results={len(results)}')
# 	logger.info(f'[runscan] {datetime.now()-t0} ')

# 	return results
