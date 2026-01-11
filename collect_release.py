import os, time, logging, bisect
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
HEADERS = {
    'Authorization': 'token ' + GITHUB_TOKEN,
    'Accept': 'application/vnd.github.v3+json'
}
HEADERS_STAR = {
    'Authorization': 'token ' + GITHUB_TOKEN,
    'Accept': 'application/vnd.github.v3.star+json'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReleaseCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.output_csv = 'data/release_raw_data.csv'
        
    def search_recent_repos(self, page, max_repos_per_page, days_back = 366) -> List[Dict]:
        repos = []
        since_date = (datetime.now() - timedelta(days=days_back)).isoformat()
        url = f"https://api.github.com/search/repositories"
        params = {
            'q': f'created:>{since_date} stars:100..5000',
            'sort': 'updated',
            'order': 'desc',
            'per_page': max_repos_per_page,
            'page': page
        }
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if 'items' not in data or not data['items']:
                return repos
            for repo in data['items']:
                if repo['size'] > 100:
                    repos.append({
                        'full_name': repo['full_name'],
                        'stargazers_count': repo['stargazers_count'],
                        'forks_count': repo['forks_count'],
                        'watchers_count': repo['watchers_count'],
                        'language': repo.get('language', 'Unknown'),
                        'created_at': repo['created_at'],
                        'updated_at': repo['updated_at'],
                        'topics': repo.get('topics', []),
                        'owner': repo.get('owner', {}).get('login') if repo.get('owner') else None
                    })
            logger.info(f"{len(repos)} repositories collected.")
            time.sleep(1)
            return repos
        except Exception as e:
            logger.error(f"Error: {e}")
            return repos
    
    def get_repo_releases(self, owner: str, repo: str) -> List[Dict]:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            releases = response.json()
            if not isinstance(releases, list):
                return []
            return releases
        except Exception as e:
            logger.error(f"{owner}/{repo} Error: {e}")
            return []
    
    def get_author_features(self, author_login: str) -> dict:
        if not author_login:
            return {
                'author_followers': 0,
                'author_public_repos': 0,
                'author_type': 'Unknown'
            }
        try:
            url = f"https://api.github.com/users/{author_login}"
            response = self.session.get(url, timeout=5)
            if response.status_code == 404:
                return {
                'author_followers': 0,
                'author_public_repos': 0,
                'author_type': 'Unknown'
            }
            response.raise_for_status()
            user_data = response.json()
            return {
                'author_followers': user_data['followers'],
                'author_public_repos': user_data['public_repos'],
                'author_type': user_data['type']
            }
        except Exception as e:
            logger.warning(f"Error getting {author_login}: {e}")
            return {
                'author_followers': 0,
                'author_public_repos': 0,
                'author_type': 'Unknown'
            }
    
    def get_first_week_stars(self, owner, repo, published_at_str):
        published_at = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
        cutoff_date = published_at + timedelta(days=7)
        total_stars = 0
        page = 1
        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/stargazers?per_page=100&page={page}"
            resp = requests.get(url, headers=HEADERS_STAR)
            if resp.status_code != 200:
                print(f"Star Count Failed: {resp.status_code}")
                break
            data = resp.json()
            if not data:
                break
            star_times = [datetime.strptime(star['starred_at'], "%Y-%m-%dT%H:%M:%SZ") for star in data]
            start_idx = bisect.bisect_left(star_times, published_at)
            end_idx = bisect.bisect_right(star_times, cutoff_date)
            page_count = end_idx - start_idx
            total_stars += page_count
            if end_idx == 0:
                break
            if star_times[-1] > cutoff_date and page_count == 0:
                break
            page += 1
            time.sleep(0.5)
        return total_stars

    def _process_release(self, repo, release, owner, repo_name):
        try:
            author_login = repo['owner']
            author = self.get_author_features(author_login)
            first_week_star = self.get_first_week_stars(owner, repo_name, release['published_at'])
            release_data = {
                'full_name': repo['full_name'] + '/' + release.get('tag_name', ''),
                'repo_stars': repo['stargazers_count'],
                'repo_forks': repo['forks_count'],
                'repo_watchers': repo['watchers_count'],
                'language': repo['language'],
                'repo_created_at': repo['created_at'],
                'repo_updated_at': repo['updated_at'],
                'topics': repo['topics'],
                'release_name': release.get('name', ''),
                'release_body': release.get('body', ''),
                'author_followers': author['author_followers'],
                'author_public_repos': author['author_public_repos'],
                'author_type': author['author_type'],
                'published_at': release.get('published_at', ''),
                'prerelease': release.get('prerelease', False),
                'draft': release.get('draft', False),
                'first_week_star': first_week_star
            }
            df = pd.DataFrame([release_data])
            df.to_csv(self.output_csv, mode='a', header=False, index=False, encoding='utf-8')
            return release_data
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
    
    def collect_release_data(self, max_repos_per_page = 50, start_page = 1, end_page = 30):
        logger.info("Start collecting...")
        page = start_page
        collected_repo_nums = 0
        collected_release_nums = 0
        while (page <= end_page):
            logger.info(f"Start page {page}...")
            repos = self.search_recent_repos(page, max_repos_per_page)
            all_release_data = []
            for i, repo in enumerate(repos):
                full_name = repo['full_name']
                owner, repo_name = full_name.split('/')
                logger.info(f"Collecting repo {i+1}/{len(repos[:max_repos_per_page])}: {full_name}")
                releases = self.get_repo_releases(owner, repo_name)
                for release in releases[:10]:
                    try:
                        release_info = self._process_release(repo, release, owner, repo_name)
                        if release_info:
                            all_release_data.append(release_info)
                            logger.info(f"  Release collected: {release.get('tag_name', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Error: {e}")
                        continue
                    time.sleep(0.5)
            collected_repo_nums += len(repos)
            collected_release_nums += len(all_release_data)
            logger.info(f"Page {page} done.")
            logger.info(f"{len(repos)} new repos collected.")
            logger.info(f"{len(all_release_data)} new releases collected.")
            logger.info(f"========================")
            page += 1
        logger.info(f"{collected_repo_nums} repos in total collected.")
        logger.info(f"{collected_release_nums} releases in total collected.")

if __name__ == "__main__":
    collector = ReleaseCollector()
    collector.collect_release_data()