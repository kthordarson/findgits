#!/usr/bin/python3
import os, sys
import time
import argparse
from pathlib import Path
from loguru import logger
from datetime import datetime, timedelta
from threading import Thread
from queue import SimpleQueue as Queue
from sqlalchemy import create_engine
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from concurrent.futures import (ProcessPoolExecutor, as_completed)

from dbstuff import GitRepo, GitFolder, send_to_db, get_engine, db_init, send_gitfolder_to_db, get_folder_entries, get_repo_entries
from utils import get_folder_list

def create_folders_task(gitpath):
	gf = GitFolder(gitpath)
	return gf

def create_remotes_task(gf):
	gr = GitRepo(gf)
	return gr

def worker_task(gitpath):
	gf = GitFolder(gitpath)
	gr = GitRepo(gf)
	return gr,gf

class FolderScanner(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.kill = False
		self.queue = Queue()
	def run(self):
		while not self.kill:
			if self.kill:
				break
			while not self.queue.empty():
				item = self.queue.get()

def main(args):
	engine = get_engine(dbtype='sqlite')
	Session = sessionmaker(bind=engine)
	session = Session()
	db_init(engine)
	#folder_entries = get_folder_entries(session)
	#repo_entries = get_repo_entries(session)
	gitfolders = [GitFolder(k) for k in get_folder_list(args.path)]
	logger.debug(f'gitfolders={len(gitfolders)}')
	gfl = []
	for k in gitfolders:
		if session.query(GitFolder).filter(GitFolder.git_path == str(k.git_path)).first():
			g = session.query(GitFolder).filter(GitFolder.git_path == str(k.git_path)).first()
			g.refresh()
			session.add(g)
			session.commit()
			logger.warning(f'[dupe] k={k} {k.git_path}')
		else:
			gfl.append(k)

	for k in gfl:
		logger.debug(f'gfl={len(gfl)} gitfolders={len(gitfolders)} k={k.git_path}')
		session.add(k)
		session.commit()
		repo = k.get_repo()
		session.add(repo)
		session.commit()

if __name__ == '__main__':
	myparse = argparse.ArgumentParser(description="findgits", exit_on_error=False)
	myparse.add_argument('path', nargs='?',  metavar='searchpath')
	args = myparse.parse_args()
	main(args)
