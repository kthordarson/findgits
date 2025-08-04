#!/usr/bin/python3
import traceback
import asyncio
import re
from datetime import datetime
from pathlib import Path
import argparse
from typing import Dict, List
import json
from collections import defaultdict
from loguru import logger
from sqlalchemy.orm import sessionmaker
from dbstuff import GitRepo, GitFolder, GitStar, GitList
from dbstuff import get_engine, db_init, drop_database, mark_repo_as_starred
from repotools import create_repo_to_list_mapping, verify_star_list_links, check_update_dupes, insert_update_git_folder, insert_update_starred_repo, populate_repo_data
from gitstars import get_lists_and_stars_unified, fetch_github_starred_repos
from utils import flatten
from cacheutils import set_cache_entry, get_cache_entry
# Import functions from stats.py
from stats import (
    dbcheck,
    check_git_dates,
    get_starred_repos_by_list,
    show_starred_repo_stats,
    show_db_info,
    show_list_by_group,
    show_rate_limits
)

async def process_git_folder(git_path, session, args):
    """Process a single git folder asynchronously"""
    try:
        # Always ensure the session is in a valid state
        if not session.is_active:
            logger.warning(f'Session is not active, rolling back for {git_path}')
            session.rollback()

        result = await insert_update_git_folder(git_path, session, args)

        # Fix: Check if result is None (happens when BLANK_REPO_DATA is encountered)
        if result is None:
            logger.warning(f'Failed to process git folder: {git_path} - insert_update_git_folder returned None')
            return None

        # Fix: Check if result.gitrepo_id is valid before querying
        if not result.gitrepo_id:
            logger.warning(f'GitFolder {git_path} has no gitrepo_id')
            return result

        git_repo = session.query(GitRepo).filter(GitRepo.id == result.gitrepo_id).first()

        # Fix: Check if git_repo was found
        if not git_repo:
            logger.warning(f'No GitRepo found for gitrepo_id {result.gitrepo_id} from path {git_path}')
            return result

        # Check if this repo is in our starred repos
        star_entry = session.query(GitStar).filter(GitStar.gitrepo_id == git_repo.id).first()
        if star_entry:
            # Use the mark_repo_as_starred function
            mark_repo_as_starred(session, git_repo.id)

            # Update GitFolder starred fields if they exist
            if hasattr(result, 'is_starred'):
                result.is_starred = True
            if hasattr(result, 'star_id'):
                result.star_id = star_entry.id
            if hasattr(result, 'list_id') and star_entry.gitlist_id:
                result.list_id = star_entry.gitlist_id
        else:
            git_repo.is_starred = False
            git_repo.starred_at = None
            if hasattr(result, 'is_starred'):
                result.is_starred = False
            if hasattr(result, 'star_id'):
                result.star_id = None
            if hasattr(result, 'list_id'):
                result.list_id = None

        session.commit()
        return result

    except Exception as e:
        logger.error(f'Error processing {git_path}: {e} {type(e)}')
        logger.error(f'traceback: {traceback.format_exc()}')
        if session.is_active:
            session.rollback()
        return None

async def process_starred_repo(repo, session, args):
    """Process a single starred repo asynchronously"""
    try:
        await insert_update_starred_repo(github_repo=repo, session=session, args=args, create_new=True)
    except Exception as e:
        logger.error(f'Error processing {repo}: {e} {type(e)}')
        logger.error(f'traceback: {traceback.format_exc()}')

async def link_existing_repos_to_stars(session, args):
    """Link existing GitRepo entries to their GitStar counterparts and associate with lists"""
    try:
        # Get all starred repos from GitHub
        starred_repos = await fetch_github_starred_repos(args, session)
        starred_lookup = {repo['full_name']: repo for repo in starred_repos}

        logger.info(f"Found {len(starred_repos)} starred repos from GitHub API")

        # Get all git lists and their associated repos
        git_lists_data = await get_lists_and_stars_unified(session, args)
        lists_with_repos = git_lists_data.get('lists_with_repos', {})

        if isinstance(lists_with_repos, dict) and 'lists_with_repos' in lists_with_repos:
            actual_lists = lists_with_repos['lists_with_repos']
        else:
            actual_lists = lists_with_repos

        # Create a mapping of repo URLs to list names
        repo_to_list_mapping = {}
        if isinstance(actual_lists, dict):
            for list_name, list_data in actual_lists.items():
                if isinstance(list_data, dict):
                    hrefs = list_data.get('hrefs', [])
                    for href in hrefs:
                        # Convert href to full_name format
                        if href.startswith('/'):
                            href = href[1:]
                        if 'github.com/' in href:
                            full_name = href.split('github.com/')[-1]
                        else:
                            full_name = href
                        if full_name.endswith('.git'):
                            full_name = full_name[:-4]
                        repo_to_list_mapping[full_name] = list_name

        # Process ALL starred repos, not just existing GitRepo entries
        star_entries_created = 0
        star_entries_updated = 0
        linked_count = 0

        for starred_repo in starred_repos:
            try:
                full_name = starred_repo.get('full_name')
                if not full_name:
                    continue

                # Check if GitRepo exists
                git_repo = session.query(GitRepo).filter(
                    (GitRepo.full_name == full_name) | (GitRepo.git_url.ilike(f"%{full_name}%")) | (GitRepo.html_url == starred_repo.get('html_url'))).first()

                # If no GitRepo exists, create one
                if not git_repo:
                    git_repo = GitRepo(
                        starred_repo.get('html_url', f"https://github.com/{full_name}"),
                        '[notcloned]',
                        starred_repo
                    )
                    session.add(git_repo)
                    session.flush()  # Get the ID
                    logger.debug(f"Created new GitRepo for starred repo: {full_name}")

                # Mark repo as starred
                git_repo.is_starred = True
                git_repo.starred_at = datetime.now()

                # Create or update GitStar entry
                git_star = session.query(GitStar).filter(GitStar.gitrepo_id == git_repo.id).first()
                if not git_star:
                    git_star = GitStar()
                    git_star.gitrepo_id = git_repo.id
                    git_star.starred_at = datetime.now()
                    git_star.stargazers_count = starred_repo.get('stargazers_count')
                    git_star.description = starred_repo.get('description')
                    git_star.full_name = starred_repo.get('full_name')
                    git_star.html_url = starred_repo.get('html_url')
                    session.add(git_star)
                    session.flush()
                    star_entries_created += 1
                else:
                    # Update existing GitStar
                    git_star.stargazers_count = starred_repo.get('stargazers_count')
                    git_star.description = starred_repo.get('description')
                    git_star.full_name = starred_repo.get('full_name')
                    git_star.html_url = starred_repo.get('html_url')
                    star_entries_updated += 1

                # Link to list if found in mapping
                if full_name in repo_to_list_mapping:
                    list_name = repo_to_list_mapping[full_name]
                    git_list = session.query(GitList).filter(GitList.list_name == list_name).first()
                    if git_list:
                        git_star.gitlist_id = git_list.id
                        linked_count += 1

            except Exception as e:
                logger.error(f"Error processing starred repo {starred_repo.get('full_name', 'unknown')}: {e} {type(e)}")
                logger.error(f'traceback: {traceback.format_exc()}')
                continue

        session.commit()
        logger.info(f"GitStar processing complete: Created {star_entries_created}, Updated {star_entries_updated}, Linked to lists {linked_count}")

    except Exception as e:
        logger.error(f"Error linking existing repos to stars: {e} {type(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        session.rollback()

# Update populate_git_lists to use the unified function
async def populate_git_lists(session, args):
    # Use the unified function instead of separate calls
    unified_data = await get_lists_and_stars_unified(session, args)
    list_data = unified_data['lists_metadata']

    if args.debug:
        logger.debug(f'populate_git_lists: {len(list_data)} lists fetched from GitHub')

    # Cache the list data
    cache_key = "git_lists_metadata"
    cache_type = "list_metadata"
    set_cache_entry(session, cache_key, cache_type, json.dumps(list_data))

    for entry in list_data:
        # Extract the actual list name from the URL if the name is "Unknown"
        list_name = entry.get('name', 'Unknown')
        if list_name == 'Unknown' and entry.get('list_url'):
            # Extract list name from URL like "/stars/kthordarson/lists/az" -> "az"
            url_parts = entry.get('list_url', '').split('/')
            if len(url_parts) > 0:
                list_name = url_parts[-1]

        # Check if list already exists by name or URL
        db_list = session.query(GitList).filter((GitList.list_name == list_name) | (GitList.list_url == entry.get('list_url', ''))).first()

        # Parse repo count from text like "19 repositories" or "1 repository"
        repo_count = 0
        try:
            repo_count_str = entry.get('repo_count', '0')
            # Extract numbers from strings like "1 repository" or "19 repositories"
            numbers = re.findall(r'\d+', repo_count_str)
            repo_count = int(numbers[0]) if numbers else 0
        except (ValueError, AttributeError, IndexError) as e:
            repo_count = 0
            logger.warning(f"Could not parse repo_count '{entry.get('repo_count')}' for list {list_name}: {e}")

        if db_list:
            # Update existing entry
            db_list.list_name = list_name
            db_list.list_description = entry.get('description', '')
            db_list.list_url = entry.get('list_url', '')
            db_list.repo_count = repo_count

            if args.debug:
                logger.debug(f'Updated GitList: {list_name} with {repo_count} repos')
        else:
            # Create new entry
            db_list = GitList()
            db_list.list_name = list_name
            db_list.list_description = entry.get('description', '')
            db_list.list_url = entry.get('list_url', '')
            db_list.repo_count = repo_count
            db_list.created_at = datetime.now()

            if args.debug:
                logger.debug(f'Adding new GitList: {list_name} with URL: {db_list.list_url} and {repo_count} repos')
            session.add(db_list)

        # Cache individual list data
        list_cache_key = f"git_list:{list_name}"
        list_cache_type = "individual_list"
        entry_to_cache = entry.copy()
        entry_to_cache['name'] = list_name
        entry_to_cache['parsed_repo_count'] = repo_count
        set_cache_entry(session, list_cache_key, list_cache_type, json.dumps(entry_to_cache))

    # Commit the database changes
    session.commit()

    # Log final results
    total_repos = sum(entry.get('parsed_repo_count', 0) for entry in list_data)
    logger.info(f"Populated {len(list_data)} lists with {total_repos} total repositories")

    return list_data

def get_args():
    myparse = argparse.ArgumentParser(description="findgits")
    myparse.add_argument('--scanpath','-sp', help='Scan path for git repos', action='store', dest='scanpath', nargs=1)
    # info
    myparse.add_argument('--checkdates', help='checkdates', action='store_true', default=False, dest='checkdates')
    myparse.add_argument('--list-by-group', help='show starred repos grouped by list', action='store_true', default=False, dest='list_by_group')
    myparse.add_argument('--list-stats', help='show starred repo count statistics by list', action='store_true', default=False, dest='list_stats')
    myparse.add_argument('--dbinfo', help='show dbinfo', action='store_true', default=False, dest='dbinfo')
    myparse.add_argument('--check_rate_limits', help='check_rate_limits', action='store_true', default=False, dest='check_rate_limits')
    # db
    myparse.add_argument('--dbmode', help='mysql/sqlite/postgresql', dest='dbmode', default='sqlite', action='store', metavar='dbmode')
    myparse.add_argument('--db_file', help='sqlitedb filename', default='gitrepo.db', dest='db_file', action='store', metavar='db_file')
    myparse.add_argument('--dropdatabase', action='store_true', default=False, dest='dropdatabase', help='drop database, no warnings')
    # tuning, debug, etc
    myparse.add_argument('--max_pages', help='gitstars max_pages', action='store', default=100, dest='max_pages', type=int)
    myparse.add_argument('--global_limit', help='global limit', action='store', default=0, dest='global_limit', type=int)
    myparse.add_argument('--debug', help='debug', action='store_true', default=True, dest='debug')
    myparse.add_argument('--use_cache', help='use_cache', action='store_true', default=True, dest='use_cache')
    myparse.add_argument('--disable_cache', help='disable_cache', action='store_true', default=False, dest='disable_cache')
    args = myparse.parse_args()
    if args.disable_cache:
        args.use_cache = False
        logger.info('Cache disabled')
    if args.debug:
        logger.info('Debug mode enabled')
        args.checkdates = True
        args.dbinfo = True
        args.list_by_group = True
        args.list_stats = True  # Add this to debug mode
        args.check_rate_limits = True
    return args

def get_session(args):
    engine = get_engine(args)
    s = sessionmaker(bind=engine)
    session = s()
    db_init(engine)
    print(f'DB Engine: {engine} DB Type: {engine.name} DB URL: {engine.url}')
    return session, engine

async def main():
    args = get_args()
    session, engine = get_session(args)

    if args.dropdatabase:
        drop_database(engine)
        logger.info('Database dropped')
        session.close()
        return

    if args.scanpath:
        scanpath = Path(args.scanpath[0])

        if args.debug:
            logger.debug(f'Scan path: {scanpath}')

        # Fetch starred repos ONCE at the beginning
        starred_repos = await fetch_github_starred_repos(args, session)

        list_data = await populate_git_lists(session, args)
        if args.debug:
            logger.debug(f'Populated git lists from GitHub, got {len(list_data)} ... starting populate_repo_data')

        # Pass the already-fetched starred_repos to populate_repo_data instead of letting it fetch again
        stats = await populate_repo_data(session, args, starred_repos=starred_repos)

        if args.debug:
            logger.debug(f'populate_repo_data done stats: {stats}')

        # Use the existing starred_repos data
        print(f'Using {len(starred_repos)} starred repos from GitHub API')

        if args.global_limit > 0:
            logger.warning(f'Global limit set to {args.global_limit}, this will limit the number of repositories processed.')
            git_repos = session.query(GitRepo).limit(args.global_limit).all()
        else:
            git_repos = session.query(GitRepo).all()

        if args.debug:
            logger.debug(f'Git Repos: {len(git_repos)}')

        cache_entry = get_cache_entry(session, "git_list_stars", "list_stars")
        if cache_entry:
            git_lists = json.loads(cache_entry.data)
        else:
            # Fallback if cache somehow failed
            if args.debug:
                logger.debug("[fallback] Cache entry not found, fetching lists and stars from GitHub")
            git_lists = await get_lists_and_stars_unified(session, args)

        urls = list(set(flatten([git_lists[k]['hrefs'] for k in git_lists])))

        localrepos = [k.github_repo_name for k in git_repos]
        notfoundrepos = [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
        foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
        print(f'Git Lists: {len(git_lists)} git_list_count: {len(git_lists)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

        # Process repos in parallel
        batch_size = 20
        for i in range(0, len(notfoundrepos), batch_size):
            batch = notfoundrepos[i:i+batch_size]
            tasks = []

            for repo in batch:
                if '20142995/pocsuite3' in repo:
                    logger.warning(f'problematic repo: {repo}')
                tasks.append(process_starred_repo(repo, session, args))

            await asyncio.gather(*tasks)
            session.commit()
        await link_existing_repos_to_stars(session, args)

        verification_results = await verify_star_list_links(session, args)
        if verification_results:
            print(f"Star-List Link Verification: {verification_results}")

        if scanpath.is_dir():
            # Find git folders
            git_folders = [k for k in scanpath.glob('**/.git') if Path(k).is_dir() and '.cargo' not in str(k) and 'developmenttest' not in str(k)]
            if args.global_limit > 0:
                git_folders = git_folders[:args.global_limit]
                logger.warning(f'Global limit set to {args.global_limit}, processing only first {len(git_folders)} git folders')
            print(f'Scan path: {scanpath} found {len(git_folders)} git folders')
            tasks = set()
            for git_folder in git_folders:
                git_path = git_folder.parent
                tasks.add(process_git_folder(git_path, session, args))
            await asyncio.gather(*tasks)
            session.commit()
            print(f'Processed {len(git_folders)} git folders')
        else:
            logger.error(f'Scan path: {scanpath} is not a valid directory')

        # PRE-FETCH the repo-to-list mapping ONCE
        logger.info("Creating repo-to-list mapping...")
        repo_to_list_mapping = await create_repo_to_list_mapping(session, args)
        logger.info(f"Created mapping for {len(repo_to_list_mapping)} repositories")

    if args.checkdates:
        check_git_dates(session)

    if args.dbinfo:
        await show_db_info(session, args)

    if args.list_by_group:
        await show_list_by_group(session, args)

    if args.list_stats:
        show_starred_repo_stats(session)

    if args.check_rate_limits:
        await show_rate_limits(session, args)

    # If only info/stats flags were used, close session and return
    if not args.scanpath and (args.checkdates or args.dbinfo or args.list_by_group or args.list_stats or args.check_rate_limits):
        session.close()
        return

    session.close()

if __name__ == '__main__':
    asyncio.run(main())
