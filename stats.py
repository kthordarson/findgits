#!/usr/bin/python3
import traceback
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict
from itertools import combinations
from typing import Dict, List
from loguru import logger
from sqlalchemy import text
from dbstuff import GitRepo, GitFolder
from gitstars import get_lists_and_stars_unified, fetch_github_starred_repos
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

def check_git_dates(session, create_heatmap=False) -> None:
	"""
	Check and analyze git timestamp differences
	"""
	df = pd.DataFrame(session.execute(text('select git_path.git_path, git_path.git_path_ctime, git_path.git_path_atime, git_path.git_path_mtime, gitrepo.created_at, gitrepo.updated_at, gitrepo.pushed_at from git_path inner join gitrepo on git_path.gitrepo_id=gitrepo.id ')).tuples())

	if df.empty:
		print("No git repositories found with timestamp data.")
		return

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

	print("\n" + "=" * 80)
	print("📅 GIT REPOSITORY TIMESTAMP ANALYSIS")
	print("=" * 80)
	print(f"📊 Analyzing {len(df)} git repositories")
	print()

	# Pretty print the differences with better formatting
	print("🕐 TIMESTAMP DIFFERENCES (Top 10 repositories by path):")
	print("-" * 80)

	# Create a shortened path column for display
	df['short_path'] = df['git_path'].str.replace('/home/kth/', '~/', regex=False)
	df['short_path'] = df['short_path'].apply(lambda x: x[-60:] if len(x) > 60 else x)

	# Select columns for display
	display_df = df[['short_path', 'mtime_to_ctime', 'atime_to_mtime', 'mtime_to_updated_at', 'mtime_to_pushed_at', 'created_at_to_pushed_at']].head(10)

	# Format the display with proper column names and rounding
	display_df.columns = ['Repository Path', 'MTime→CTime', 'ATime→MTime', 'MTime→Updated', 'MTime→Pushed', 'Created→Pushed']

	# Round numeric columns to 1 decimal place
	numeric_cols = ['MTime→CTime', 'ATime→MTime', 'MTime→Updated', 'MTime→Pushed', 'Created→Pushed']
	for col in numeric_cols:
		display_df[col] = display_df[col].round(1)

	print(display_df.to_string(index=False, max_colwidth=60))
	print("-" * 80)

	# Summary statistics
	print("\n📈 SUMMARY STATISTICS (days):")
	print("-" * 50)

	summary_stats = df[['mtime_to_ctime', 'atime_to_mtime', 'mtime_to_updated_at', 'mtime_to_pushed_at', 'created_at_to_pushed_at']].describe()
	summary_stats.columns = ['MTime→CTime', 'ATime→MTime', 'MTime→Updated', 'MTime→Pushed', 'Created→Pushed']
	print(summary_stats.round(1).to_string())

	# Interesting findings
	print("\n🔍 INTERESTING FINDINGS:")
	print("-" * 50)

	# Repos with significant time differences
	old_repos = df[df['created_at_to_pushed_at'] > 1000].sort_values('created_at_to_pushed_at', ascending=False)
	if not old_repos.empty:
		print("📜 Oldest repositories (created > 1000 days before last push):")
		for _, repo in old_repos.head(5).iterrows():
			days = repo['created_at_to_pushed_at']
			years = days / 365.25
			print(f"   • {repo['short_path']:<50} {days:7.0f} days ({years:.1f} years)")

	# Recently accessed repos
	recent_access = df[df['atime_to_mtime'] < 7].sort_values('atime_to_mtime')
	if not recent_access.empty:
		print("\n📂 Recently accessed repositories (accessed within 7 days of modification):")
		for _, repo in recent_access.head(5).iterrows():
			days = repo['atime_to_mtime']
			print(f"   • {repo['short_path']:<50} {days:5.1f} days ago")

	# Repos with future timestamps (potential issues)
	future_timestamps = df[(df['mtime_to_updated_at'] < 0) | (df['mtime_to_pushed_at'] < 0)]
	if not future_timestamps.empty:
		print("\n⚠️  Repositories with timestamp inconsistencies:")
		for _, repo in future_timestamps.head(5).iterrows():
			issues = []
			if repo['mtime_to_updated_at'] < 0:
				issues.append(f"updated {abs(repo['mtime_to_updated_at']):.0f}d future")
			if repo['mtime_to_pushed_at'] < 0:
				issues.append(f"pushed {abs(repo['mtime_to_pushed_at']):.0f}d future")
			print(f"   • {repo['short_path']:<50} {', '.join(issues)}")

	# Compute correlation matrix for timestamps
	print("\n🔗 TIMESTAMP CORRELATIONS:")
	print("-" * 50)
	correlation_matrix = df[timestamp_columns].corr()

	# Pretty print correlation matrix with better formatting
	correlation_display = correlation_matrix.round(3)
	correlation_display.columns = ['CTime', 'ATime', 'MTime', 'Created', 'Updated', 'Pushed']
	correlation_display.index = pd.Index(['CTime', 'ATime', 'MTime', 'Created', 'Updated', 'Pushed'])
	print(correlation_display.to_string())

	# Group by project analysis
	print("\n📁 PROJECT-LEVEL ANALYSIS:")
	print("-" * 50)
	df['project'] = df['git_path'].str.split('/').str[-1]
	project_stats = df.groupby('project')[['atime_to_mtime', 'created_at_to_pushed_at']].agg({
		'atime_to_mtime': ['mean', 'std'],
		'created_at_to_pushed_at': ['mean', 'std']
	}).round(1)

	# Flatten column names
	project_stats.columns = ['Avg_Access_Days', 'Std_Access_Days', 'Avg_Age_Days', 'Std_Age_Days']

	# Show top projects by age and recent access
	print("Top 10 projects by average age (created to last push):")
	oldest_projects = project_stats.sort_values('Avg_Age_Days', ascending=False).head(10)
	for project, stats in oldest_projects.iterrows():
		print(f"   • {project:<30} {stats['Avg_Age_Days']:7.0f} days avg age")

	print("\nMost recently accessed projects:")
	recent_projects = project_stats[project_stats['Avg_Access_Days'] < 30].sort_values('Avg_Access_Days').head(10)
	for project, stats in recent_projects.iterrows():
		print(f"   • {project:<30} {stats['Avg_Access_Days']:5.1f} days since access")

	print("=" * 80)

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
		diff_df = pd.DataFrame(diff_data, index=df['short_path'])

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
		print("\n💾 Heatmap saved as 'timestamp_differences_heatmap.png'")
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

def show_starred_repo_stats(session) -> None:
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

async def show_list_by_group(session, args) -> None:
	"""Show starred repos grouped by list"""
	grouped_repos = await get_starred_repos_by_list(session, args)
	total_repos = sum(len(repos) for repos in grouped_repos.values())
	print(f"\nFound {len(grouped_repos)} lists with {total_repos} total repositories:\n")
	for list_name, repos in grouped_repos.items():
		print(f"\n{list_name} ({len(repos)} repos):")
		print("-" * (len(list_name) + 10))
		print(f"{'Stars':>9} | {'Language':<15} | {'Repository Name':<40} | {'Description'}")
		print(f"{'-' * 9} | {'-' * 15} | {'-' * 40} | {'-' * 11}")

		for repo in repos[:args.max_output]:
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
			print(f"{stars:9d} | {lang:15} | {repo['full_name']:40} | {desc}")

async def show_rate_limits(session, args) -> None:
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
