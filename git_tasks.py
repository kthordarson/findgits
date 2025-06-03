#!/usr/bin/python3
import os
from datetime import datetime
from multiprocessing import cpu_count
from loguru import logger
from subprocess import Popen, PIPE
from dbstuff import (GitRepo)

CPU_COUNT = cpu_count()

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

if __name__ == '__main__':
	pass
