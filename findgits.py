#!/usr/bin/python3
from typing import cast, Tuple
import asyncio
import argparse
import json
from loguru import logger
import sqlalchemy
from sqlalchemy.orm import sessionmaker, Session
from scanpath import main_scanpath
from dbstuff import get_engine, db_init, drop_database
from repotools import run_update_paths, populate_git_lists
from gitstars import get_lists_and_stars_unified
from stats import fetch_github_starred_repos
from utils import cleanup_shared_session
from cacheutils import get_cache_entry
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
    myparse.add_argument('--populate_git_lists', help='populate_git_lists', action='store_true', dest='populate_git_lists', default=False)
    # info
    myparse.add_argument('--checkdates', help='checkdates', action='store_true', default=False, dest='checkdates')
    myparse.add_argument('--list-by-group', '-lbg', help='show starred repos grouped by list', action='store_true', default=False, dest='list_by_group')
    myparse.add_argument('--list-stats', '-ls', help='show starred repo count statistics by list', action='store_true', default=False, dest='list_stats')
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
    myparse.add_argument('--cache_ttl_hours', help='hours before a cache entry is considered stale and refetched', action='store', default=24, dest='cache_ttl_hours', type=float)
    args = myparse.parse_args()
    if args.disable_cache:
        args.use_cache = False
        logger.info('Cache disabled')
    if args.dbinfoall:
        args.checkdates = True
        args.dbinfo = True
        args.list_by_group = True
        args.list_stats = True
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
    unified_data = None
    starred_repos = None
    if args.dropdatabase:
        drop_database(engine)
        logger.info('Database dropped')
        session.close()
        return
    try:
        unified_data = await get_lists_and_stars_unified(session, args)
        if args.debug:
            logger.debug(f"[f] unified_data: {len(unified_data)}")
        if not unified_data:
            logger.error("Failed to retrieve unified data for lists and stars. Exiting.")
            return
        starred_repos = await fetch_github_starred_repos(args, session)
        if args.debug:
            logger.debug(f"[f] starred_repos: {len(starred_repos)}")
        if not starred_repos:
            logger.error("Failed to retrieve starred repositories. Exiting.")
            return
        else:
            if args.scanpath:
                await main_scanpath(args, unified_data=unified_data, starred_repos=starred_repos, session=session)

            if args.checkdates:
                stats_check_git_dates(session, args)

            if args.list_by_group:
                await show_list_by_group(session, args, unified_data=unified_data, starred_repos=starred_repos)

            if args.list_stats:
                show_starred_repo_stats(session, args)

            if args.check_rate_limits:
                await show_rate_limits(session, args)

            if args.update_paths:
                await run_update_paths(session, args)

            if args.populate_git_lists:
                list_data = await populate_git_lists(session, args, unified_data=unified_data)
                if args.debug:
                    logger.debug(f'Populated git lists from GitHub, list_data: {len(list_data)}')

            # If only info/stats flags were used, close session and return
            if not args.scanpath and (args.checkdates or args.dbinfo or args.list_by_group or args.list_stats or args.check_rate_limits or args.populate_git_lists):
                session.close()
                return
    finally:
        await cleanup_shared_session()
        session.close()

if __name__ == '__main__':
    # sqlite3 -header -column gitrepo.db ".tables" | tr ' ' '\n' | grep -v '^$' | grep -v 'sqlite_' | while read table; do count=$(sqlite3 gitrepo.db "SELECT COUNT(*) FROM \"$table\";"); printf "%-25s %8s\n" "$table" "$count rows"; done
    # sqlite3 -header -column gitrepo.db ".tables" | tr ' ' '\n' | grep -v '^$' | grep -v 'sqlite_' | while read table; do count=$(sqlite3 gitrepo.db "SELECT COUNT(*) FROM \"$table\";"); printf "%-25s %8s\n" "$table" "$count rows"; done

    asyncio.run(main())
