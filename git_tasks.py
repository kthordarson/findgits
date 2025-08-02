#!/usr/bin/python3
import os
from datetime import datetime
from multiprocessing import cpu_count
from loguru import logger
from dbstuff import (GitRepo)
import asyncio

CPU_COUNT = cpu_count()

async def get_git_show(gitrepo: GitRepo) -> dict:
	"""Get git show information for a repository"""
	result = {}

	if os.path.exists(gitrepo.git_path):
		try:
			os.chdir(gitrepo.git_path)
		except FileNotFoundError as e:
			logger.error(f'{e} {type(e)} gitrepo={gitrepo}')
			result['result'] = f'Error: {e}'
			return result

		cmdstr = ['git', 'show', '--raw', '-s', '--format="date:%at%nsubject:%s%ncommitemail:%ce"']

		proc = await asyncio.create_subprocess_exec(
			*cmdstr,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE
		)

		stdout, stderr = await proc.communicate()
		if stderr:
			logger.warning(f'[get_git_show] {cmdstr} {stderr.decode()} {os.path.curdir}')

		show_out = [k.strip() for k in stdout.decode('utf8').split('\n') if k]
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
