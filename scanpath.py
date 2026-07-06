import asyncio
from pathlib import Path
import argparse
import json
from loguru import logger
from sqlalchemy.orm import Session
from dbstuff import GitRepo
from repotools import create_repo_to_list_mapping, verify_star_list_links, populate_repo_data
from repotools import populate_git_lists, process_starred_repo, link_existing_repos_to_stars, process_git_folder
from gitstars import fetch_github_starred_repos
from utils import flatten
# Import functions from stats.py

async def main_scanpath(args: argparse.Namespace, unified_data:dict, starred_repos:list, session: Session):
    scanpath = Path(args.scanpath[0])

    if args.debug:
        logger.debug(f'Scan path: {scanpath} unified_data: {len(unified_data)} starred_repos: {len(starred_repos)}')

    # Pass the already-fetched starred_repos to populate_repo_data instead of letting it fetch again
    stats = await populate_repo_data(session, args, starred_repos=starred_repos)
    if args.debug:
        logger.debug(f'populate_repo_data done stats: {len(stats)}')
    print(json.dumps(stats,indent=4))

    if args.global_limit > 0:
        logger.warning(f'Global limit set to {args.global_limit}, this will limit the number of repositories processed.')
        git_repos = session.query(GitRepo).limit(args.global_limit).all()
    else:
        git_repos = session.query(GitRepo).all()

    if args.debug:
        logger.debug(f'Git Repos: {len(git_repos)}')

    if len(unified_data.get('lists_metadata', {})) > 0 or len(unified_data.get('lists_with_repos', {})) > 0 or len(unified_data.get('Unknown', {})) > 0:
        try:
            # urls = list(set(flatten([unified_data[k]['hrefs'] for k in unified_data])))
            urls = list(set(flatten([unified_data['lists_with_repos'][k]['hrefs'] for k in unified_data['lists_with_repos']])))
        except TypeError as e:
            logger.error(f"Error flattening URLs: {e}")
            if args.debug:
                logger.error(f"unified_data: {unified_data}")
            urls = []
    else:
        if args.debug:
            logger.warning(f"unified_data does not contain expected keys or is empty. unified_data: {unified_data}")
        urls = []

    localrepos = [k.github_repo_name for k in git_repos]
    notfoundrepos = []  # [k for k in [k for k in urls] if k.split('/')[-1] not in localrepos]
    foundrepos = [k for k in [k for k in urls] if k.split('/')[-1] in localrepos]
    print(f'unified_data: {len(unified_data)} Starred Repos: {len(starred_repos)} urls: {len(urls)} foundrepos: {len(foundrepos)} notfoundrepos: {len(notfoundrepos)}')

    # Process repos in parallel
    for i in range(0, len(notfoundrepos), args.batch_size):
        batch = notfoundrepos[i:i+args.batch_size]
        tasks = []

        for repo in batch:
            if '20142995/pocsuite3' in repo:
                logger.warning(f'problematic repo: {repo}')
            else:
                tasks.append(process_starred_repo(repo, session, args))

        await asyncio.gather(*tasks)
        session.commit()
        await asyncio.sleep(1)  # Small delay to avoid overwhelming the DB
    await link_existing_repos_to_stars(session, args, unified_data, starred_repos)

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
        seen_paths = set()
        for git_folder in git_folders:
            git_path = git_folder.parent
            if git_path in seen_paths:
                continue
            seen_paths.add(git_path)
            tasks.add(process_git_folder(git_path, session, args))

        if len(git_folders) != len(tasks):
            logger.warning(f"Found {len(git_folders)} git folders, but only {len(tasks)} unique paths. Duplicates ignored.")

        await asyncio.gather(*tasks)
        session.commit()
        print(f'Processed {len(git_folders)} git folders')
    else:
        logger.error(f'Scan path: {scanpath} is not a valid directory')

    # PRE-FETCH the repo-to-list mapping ONCE
    logger.info("Creating repo-to-list mapping...")
    repo_to_list_mapping = await create_repo_to_list_mapping(session, args, unified_data=unified_data)
    logger.info(f"Created mapping for {len(repo_to_list_mapping)} repositories")
