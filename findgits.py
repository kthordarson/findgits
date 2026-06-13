#!/usr/bin/python3
import traceback
from typing import cast, Optional, Any, Dict, List, Tuple
import asyncio
import re
from datetime import datetime
from pathlib import Path
import argparse
import json
from loguru import logger
import sqlalchemy
import sqlite3
from sqlalchemy.orm import sessionmaker, Session
from scanpath import main_scanpath
from dbstuff import GitRepo, GitStar, GitList, GitFolder
from dbstuff import get_engine, db_init, drop_database, mark_repo_as_starred
from repotools import create_repo_to_list_mapping, verify_star_list_links, insert_update_git_folder, insert_update_starred_repo, populate_repo_data
from repotools import populate_git_lists, process_starred_repos, run_update_paths
from gitstars import get_lists_and_stars_unified, fetch_github_starred_repos
from utils import flatten, cleanup_shared_session
from cacheutils import set_cache_entry, get_cache_entry
# Import functions from stats.py
from stats import (
    stats_check_git_dates,
    show_starred_repo_stats,
    show_list_by_group,
    show_rate_limits
)

def get_args() -> argparse.Namespace:
    myparse = argparse.ArgumentParser(description="findgits")
    myparse.add_argument('--scanpath','-sp', help='Scan path for git repos', action='store', dest='scanpath', nargs=1)
    myparse.add_argument('--update_paths','-up', help='update_paths', action='store_true', dest='update_paths', default=False)
    # info
    myparse.add_argument('--checkdates', help='checkdates', action='store_true', default=False, dest='checkdates')
    myparse.add_argument('--list-by-group', help='show starred repos grouped by list', action='store_true', default=False, dest='list_by_group')
    myparse.add_argument('--list-stats', help='show starred repo count statistics by list', action='store_true', default=False, dest='list_stats')
    myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
    myparse.add_argument('--dbinfoall', help='show all dbinfo', action='store_true', default=False, dest='dbinfoall')
    myparse.add_argument('--check_rate_limits', help='check_rate_limits', action='store_true', default=False, dest='check_rate_limits')
    # db
    myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
    myparse.add_argument('--db_file', help='sqlitedb filename', default='gitrepo.db', dest='db_file', action='store', metavar='db_file')
    myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
    # tuning, debug, etc
    myparse.add_argument('--batch_size', help='batch_size', action='store', default=5, dest='batch_size', type=int)
    myparse.add_argument('--max_pages', help='gitstars max_pages', action='store', default=100, dest='max_pages', type=int)
    myparse.add_argument('--max_output', help='stats max_output', action='store', default=20, dest='max_output', type=int)
    myparse.add_argument('--global_limit', help='global limit', action='store', default=0, dest='global_limit', type=int)
    myparse.add_argument('--debug', help='debug', action='store_true', default=False, dest='debug')
    myparse.add_argument('--use_cache', help='use_cache', action='store_true', default=True, dest='use_cache')
    myparse.add_argument('--disable_cache', help='disable_cache', action='store_true', default=False, dest='disable_cache')
    args = myparse.parse_args()
    if args.disable_cache:
        args.use_cache = False
        logger.info('Cache disabled')
    if args.debug or args.dbinfoall:
        logger.info('Debug mode enabled')
        args.checkdates = True
        args.dbinfo = True
        args.list_by_group = True
        args.list_stats = True  # Add this to debug mode
        args.check_rate_limits = True
    return args

def get_session(args: argparse.Namespace) -> Tuple[Session, sqlalchemy.Engine]:
    engine = get_engine(args)
    s = sessionmaker(bind=engine)
    session = s()
    db_init(engine)
    print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')
    return session, engine

async def main() -> None:
    args = get_args()
    session, engine = get_session(args)

    if args.dropdatabase:
        drop_database(engine)
        logger.info('Database dropped')
        session.close()
        return
    try:
        if args.scanpath:
            await main_scanpath(args)

        if args.checkdates:
            stats_check_git_dates(session, args)

        if args.list_by_group:
            await show_list_by_group(session, args)

        if args.list_stats:
            show_starred_repo_stats(session, args)

        if args.check_rate_limits:
            await show_rate_limits(session, args)

        if args.update_paths:
            await run_update_paths(session, args)

        # If only info/stats flags were used, close session and return
        if not args.scanpath and (args.checkdates or args.dbinfo or args.list_by_group or args.list_stats or args.check_rate_limits):
            session.close()
            return
    finally:
        await cleanup_shared_session()
        session.close()

if __name__ == '__main__':
    # sqlite3 -header -column gitrepo.db ".tables" | tr ' ' '\n' | grep -v '^$' | grep -v 'sqlite_' | while read table; do count=$(sqlite3 gitrepo.db "SELECT COUNT(*) FROM \"$table\";"); printf "%-25s %8s\n" "$table" "$count rows"; done
    # sqlite3 -header -column gitrepo.db ".tables" | tr ' ' '\n' | grep -v '^$' | grep -v 'sqlite_' | while read table; do count=$(sqlite3 gitrepo.db "SELECT COUNT(*) FROM \"$table\";"); printf "%-25s %8s\n" "$table" "$count rows"; done

    asyncio.run(main())
