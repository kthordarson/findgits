#!/usr/bin/python3
import traceback
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict
from itertools import combinations
from typing import Dict, List
from loguru import logger
from sqlalchemy import text
from dbstuff import GitRepo, GitFolder, GitStar, GitList
from gitstars import get_lists_and_stars_unified, fetch_github_starred_repos
from repotools import check_update_dupes
from cacheutils import get_api_rate_limits

def dbcheck(session) -> dict:
    """
    run db checks:
        * todo check for missing folders
        * todo check for missing repos
    """
    repos = session.query(GitRepo).all()
    folders = session.query(GitFolder).all()
    result = {'repo_count': len(repos), 'folder_count': len(folders)}
    return result

def check_git_dates(session, create_heatmap=False):
    """
    Check and analyze git timestamp differences
    """
    df = pd.DataFrame(session.execute(text('select git_path.git_path, git_path.git_path_ctime, git_path.git_path_atime, git_path.git_path_mtime, gitrepo.created_at, gitrepo.updated_at, gitrepo.pushed_at from git_path inner join gitrepo on git_path.gitrepo_id=gitrepo.id ')).tuples())

    # Convert timestamp columns to datetime if not already
    timestamp_columns = ['git_path_ctime', 'git_path_atime', 'git_path_mtime', 'created_at', 'updated_at', 'pushed_at']
    for col in timestamp_columns:
        df[col] = pd.to_datetime(df[col])

    # Calculate time differences (in days)
    df['mtime_to_ctime'] = (df['git_path_ctime'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
    df['atime_to_mtime'] = (df['git_path_atime'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
    df['mtime_to_updated_at'] = (df['updated_at'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
    df['mtime_to_pushed_at'] = (df['pushed_at'] - df['git_path_mtime']).dt.total_seconds() / (60 * 60 * 24)
    df['created_at_to_pushed_at'] = (df['pushed_at'] - df['created_at']).dt.total_seconds() / (60 * 60 * 24)

    # Display the differences
    print(f'Differences in timestamps for {len(df)} git paths:')
    print(df[['git_path', 'mtime_to_ctime', 'atime_to_mtime', 'mtime_to_updated_at', 'mtime_to_pushed_at', 'created_at_to_pushed_at']].head())

    # Compute correlation matrix for timestamps
    correlation_matrix = df[timestamp_columns].corr()
    print(correlation_matrix)

    # Group by a simplified path (e.g., extract project name) and analyze
    df['project'] = df['git_path'].str.split('/').str[-1]
    print('Average timestamps grouped by project:')
    print(df.groupby('project')[timestamp_columns].mean())

    if create_heatmap:
        # Create a list of all unique pairs of timestamp columns
        pairs = list(combinations(timestamp_columns, 2))

        # Compute absolute time differences (in days) for each pair
        diff_data = []
        for index, row in df.iterrows():
            row_diffs = {}
            for col1, col2 in pairs:
                pair_name = f"{col1}_to_{col2}"
                diff = abs((row[col1] - row[col2]).total_seconds() / (60 * 60 * 24))  # Convert to days
                row_diffs[pair_name] = diff
            diff_data.append(row_diffs)

        # Create a DataFrame of differences, indexed by git_path
        diff_df = pd.DataFrame(diff_data, index=df['git_path'])

        # Create a heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(diff_df, annot=False, fmt=".1f", cmap="YlOrRd", cbar_kws={'label': 'Days'})
        plt.title('Heatmap of Absolute Time Differences Between Timestamps')
        plt.xlabel('Timestamp Pairs')
        plt.ylabel('Repository Path')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        # Save or display the heatmap
        plt.savefig('timestamp_differences_heatmap.png')
        plt.show()

async def get_starred_repos_by_list(session, args) -> Dict[str, List[dict]]:
    """
    Get starred repositories grouped by list name
    """
    try:
        unified_data = await get_lists_and_stars_unified(session, args)
        git_lists = unified_data.get('lists_with_repos', {})
        if 'lists_with_repos' in git_lists:
            git_lists = git_lists['lists_with_repos']

        if not git_lists:
            logger.warning("No GitHub lists found")
            return {}

        # Get starred repos as before
        starred_repos = await fetch_github_starred_repos(args, session)
        if not starred_repos:
            logger.warning("No starred repositories found")
            return {}

        repo_lookup = {repo['full_name']: repo for repo in starred_repos}
        grouped_repos = defaultdict(list)

        for list_name, list_data in git_lists.items():
            for href in list_data['hrefs']:
                full_name = href.strip('/').split('github.com/')[-1]
                if full_name in repo_lookup:
                    grouped_repos[list_name].append(repo_lookup[full_name])

        return dict(grouped_repos)

    except Exception as e:
        logger.error(f"Error getting starred repos by list: {e} {type(e)}")
        logger.error(f'traceback: {traceback.format_exc()}')
        return {}

def show_starred_repo_stats(session):
    """Pretty print starred repo count statistics by list"""
    query = text("""
        SELECT COALESCE(gl.list_name, 'not in a list') as list_name,
        COUNT(*) as repo_count
        FROM gitrepo gr
        LEFT JOIN gitstars gs ON gr.id = gs.gitrepo_id
        LEFT JOIN gitlists gl ON gs.gitlist_id = gl.id
        WHERE gr.is_starred = 1
        GROUP BY gl.list_name
        ORDER BY list_name;
    """)

    result = session.execute(query).fetchall()

    if not result:
        print("No starred repositories found.")
        return

    # Calculate totals
    total_repos = sum(row[1] for row in result)
    total_lists = len([row for row in result if row[0] != 'not in a list'])
    unlisted_count = next((row[1] for row in result if row[0] == 'not in a list'), 0)
    listed_count = total_repos - unlisted_count

    print("\n" + "=" * 60)
    print("📊 STARRED REPOSITORIES BY LIST")
    print("=" * 60)

    # Summary stats
    print("📈 Summary:")
    print(f"   Total starred repositories: {total_repos:,}")
    print(f"   Repositories in lists: {listed_count:,} ({(listed_count/total_repos*100):.1f}%)")
    print(f"   Repositories not in lists: {unlisted_count:,} ({(unlisted_count/total_repos*100):.1f}%)")
    print(f"   Total lists: {total_lists}")
    print()

    # Table header
    print(f"{'List Name':<20} {'Count':<8} {'Percentage':<12} {'Bar'}")
    print("-" * 60)

    # Sort results - put 'not in a list' at the end
    sorted_result = sorted(result, key=lambda x: (x[0] == 'not in a list', x[0]))

    for list_name, repo_count in sorted_result:
        percentage = (repo_count / total_repos) * 100

        # Create a visual bar
        bar_length = 20
        filled_length = int(bar_length * repo_count / max(row[1] for row in result))
        bar = '█' * filled_length + '░' * (bar_length - filled_length)

        # Different emoji for different categories
        if list_name == 'not in a list':
            emoji = "📂"
        elif repo_count >= 100:
            emoji = "🔥"
        elif repo_count >= 50:
            emoji = "⭐"
        elif repo_count >= 10:
            emoji = "📋"
        else:
            emoji = "📄"

        print(f"{emoji} {list_name:<18} {repo_count:<8,} {percentage:<11.1f}% {bar}")

    print("-" * 60)
    print(f"{'TOTAL':<20} {total_repos:<8,} {'100.0%':<12}")
    print("=" * 60)

    # Top lists
    top_lists = sorted([row for row in result if row[0] != 'not in a list'], key=lambda x: x[1], reverse=True)[:5]
    if top_lists:
        print("\n🏆 Top 5 Lists by Repository Count:")
        for i, (list_name, count) in enumerate(top_lists, 1):
            print(f"   {i}. {list_name}: {count:,} repos")

async def show_list_by_group(session, args):
    """Show starred repos grouped by list"""
    grouped_repos = await get_starred_repos_by_list(session, args)
    total_repos = sum(len(repos) for repos in grouped_repos.values())
    print(f"\nFound {len(grouped_repos)} lists with {total_repos} total repositories:\n")
    for list_name, repos in grouped_repos.items():
        print(f"\n{list_name} ({len(repos)} repos):")
        print("-" * (len(list_name) + 10))
        for repo in repos:
            stars = repo.get('stargazers_count', 0)
            if repo.get('language'):
                lang = repo.get('language', 'Unknown')[:15].ljust(15)  # Ensure fixed width
            else:
                lang = 'Unknown'.ljust(15)
            if repo.get('description'):
                desc = repo.get('description', 'No description')
            else:
                desc = 'No description'
            if len(desc) > 60:
                desc = desc[:57] + "..."
            print(f"⭐ {stars:7d} | {lang:15} | {repo['full_name']:40} | {desc}")

async def show_rate_limits(session, args):
    """Show GitHub API rate limit information"""
    rate_limits = await get_api_rate_limits(args)
    if rate_limits:
        print("\n🔍 GitHub API Rate Limits Status")
        print("=" * 50)

        if rate_limits.get('limit_hit'):
            print("⚠️  RATE LIMIT HIT!")
        else:
            print("✅ Rate limits OK")

        # Main rate limit info
        rate_info = rate_limits.get('rate_limits', {}).get('rate', {})
        if rate_info:
            limit = rate_info.get('limit', 0)
            used = rate_info.get('used', 0)
            remaining = rate_info.get('remaining', 0)
            reset_timestamp = rate_info.get('reset', 0)

            # Convert timestamp to readable time
            reset_time = datetime.fromtimestamp(reset_timestamp).strftime('%Y-%m-%d %H:%M:%S')

            print("\n📊 Overall Rate Limit:")
            print(f"   Limit:     {limit:,}")
            print(f"   Used:      {used:,}")
            print(f"   Remaining: {remaining:,}")
            print(f"   Resets at: {reset_time}")

            # Calculate percentage used
            if limit > 0:
                percentage_used = (used / limit) * 100
                print(f"   Usage:     {percentage_used:.1f}%")

                # Visual progress bar
                bar_length = 30
                filled_length = int(bar_length * used // limit)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                print(f"   Progress:  [{bar}]")

        # Resource-specific limits
        resources = rate_limits.get('rate_limits', {}).get('resources', {})
        if resources:
            print("\n📋 Resource-Specific Limits:")
            print("-" * 50)

            # Sort by usage percentage for better visibility
            resource_data = []
            for resource, data in resources.items():
                limit = data.get('limit', 0)
                used = data.get('used', 0)
                remaining = data.get('remaining', 0)
                if limit > 0:
                    usage_pct = (used / limit) * 100
                else:
                    usage_pct = 0
                resource_data.append((resource, limit, used, remaining, usage_pct))

            # Sort by usage percentage (highest first)
            resource_data.sort(key=lambda x: x[4], reverse=True)

            for resource, limit, used, remaining, usage_pct in resource_data:
                if used > 0 or limit < 5000:  # Show resources that are used or have lower limits
                    status = "⚠️ " if usage_pct > 80 else "🟡" if usage_pct > 50 else "🟢"
                    print(f"{status} {resource:25} {used:4d}/{limit:4d} ({usage_pct:5.1f}%) - {remaining:4d} remaining")
    else:
        print("❌ No API rate limits found or unable to fetch.")
