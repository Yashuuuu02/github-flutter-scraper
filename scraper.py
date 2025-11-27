"""
Flutter GitHub Repository Scraper - Production Grade
====================================================
Downloads complete Flutter projects from GitHub for AI dataset creation.
Extracts useful Dart/YAML/JSON files, detects architecture, dependencies,
exports everything into a JSONL dataset, and deletes extracted folders.

Version: 3.0 (Stable & Clean)
"""

import os
import io
import json
import time
import yaml
import zipfile
import requests
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from dataclasses import dataclass, asdict
from enum import Enum

# tqdm optional
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


# ================================================================
# CONFIGURATION
# ================================================================

# GitHub token must be set in environment variable
# Example:
#   Windows: set GITHUB_TOKEN=your_token
#   Mac/Linux: export GITHUB_TOKEN=your_token
GITHUB_API_TOKEN = os.getenv("GITHUB_TOKEN")

CONFIG_FILE = Path("scraper_config.yaml")

DEFAULT_CONFIG = {
    "output_dir": "flutter_dataset",
    "jsonl_file": "orion_dataset.jsonl",
    "log_file": "scraper.log",
    "progress_file": "scraping_progress.json",
    "metadata_file": "dataset_metadata.json",

    "search_queries": [
        "flutter ecommerce app", "flutter login", "flutter chat app",
        "flutter dashboard", "flutter weather app", "flutter riverpod",
        "flutter bloc", "flutter mvvm", "flutter provider", "flutter ui kit",
        "flutter social media", "flutter finance app", "flutter onboarding",
        "flutter todo app", "flutter animation", "flutter booking app"
    ],

    "quality_filters": {
        "min_stars": 10,
        "max_repo_age_years": 3,
        "exclude_archived": True,
        "exclude_forks": False,
    },

    "file_filters": {
        "extensions": [".dart", ".yaml", ".json"],
        "important_files": ["main.dart", "pubspec.yaml"],
        "important_dirs": [
            "lib", "screens", "widgets", "models", "services",
            "providers", "bloc", "cubit", "views"
        ],
    },

    "scraping_limits": {
        "repos_per_query": 1,
        "max_total_repos": 3,
        "delay_between_downloads": 1,
        "max_parallel_downloads": 3,
    },

    "retry_config": {
        "max_retries": 3,
        "initial_backoff": 2,
        "max_backoff": 30,
        "backoff_multiplier": 2,
    },

    "deduplication": {
        "check_content_hash": True,
        "check_fork_relationship": True,
    },

    "dry_run": False,
}


# ================================================================
# JSONL WRITER
# ================================================================

def write_jsonl_record(output_dir: Path, jsonl_file: str, record: dict):
    """Append a record to a JSONL file (with duplicate check)."""
    dataset_path = output_dir / jsonl_file
    line = json.dumps(record, ensure_ascii=False)

    # Avoid writing duplicates
    if dataset_path.exists():
        with open(dataset_path, "r", encoding="utf-8") as f:
            for existing in f:
                if existing.strip() == line.strip():
                    return

    with open(dataset_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ================================================================
# DATA MODELS
# ================================================================

class Architecture(Enum):
    RIVERPOD = "Riverpod"
    BLOC = "BLoC"
    CUBIT = "Cubit"
    PROVIDER = "Provider"
    GETX = "GetX"
    MOBX = "MobX"
    REDUX = "Redux"
    CLEAN = "Clean Architecture"
    MVVM = "MVVM"
    UNKNOWN = "Unknown"


@dataclass
class RepoMetadata:
    full_name: str
    name: str
    owner: str
    stars: int
    forks: int
    description: str
    url: str
    language: str
    updated_at: str
    created_at: str
    size_kb: int
    is_fork: bool
    fork_source: Optional[str]
    topics: List[str]
    license: Optional[str]


@dataclass
class ExtractionResult:
    repo_full_name: str
    success: bool
    files_extracted: int
    total_size_bytes: int
    architecture: str
    dependencies: Dict[str, str]
    flutter_version: Optional[str]
    error_message: Optional[str] = None


# ================================================================
# CONFIGURATION LOADER
# ================================================================

def load_config():
    """Load user config if exists, otherwise create default."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                user_cfg = yaml.safe_load(f)
            cfg = DEFAULT_CONFIG.copy()
            cfg.update(user_cfg)
            return cfg
        except Exception:
            pass

    with open(CONFIG_FILE, "w") as f:
        yaml.dump(DEFAULT_CONFIG, f)

    return DEFAULT_CONFIG


# ================================================================
# LOGGING
# ================================================================

def setup_logging(log_file: str):
    """Initialize logging to file + console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf8"),
            logging.StreamHandler()
        ]
    )


# ================================================================
# STATS TRACKER
# ================================================================

class ScraperStats:
    def __init__(self):
        self.start = time.time()
        self.total_searched = 0
        self.total_downloaded = 0
        self.total_skipped = 0
        self.total_failed = 0
        self.total_files_extracted = 0
        self.total_size_bytes = 0
        self.repos_by_architecture = defaultdict(int)
        self.popular_dependencies = defaultdict(int)
        self.rate_limit_waits = 0
        self.retries_performed = 0
        self.errors = []

    def print_summary(self):
        elapsed = time.time() - self.start
        print("\n================ SUMMARY ================")
        print(f"Time: {elapsed/60:.1f} min")
        print(f"Searched: {self.total_searched}")
        print(f"Downloaded: {self.total_downloaded}")
        print(f"Skipped: {self.total_skipped}")
        print(f"Failed: {self.total_failed}")
        print(f"Extracted Files: {self.total_files_extracted}")
        print(f"Dataset Size: {self.total_size_bytes/1e6:.2f} MB")
        print("=========================================\n")


stats = ScraperStats()


# ================================================================
# PROGRESS MANAGER
# ================================================================

class ProgressManager:
    """Tracks downloaded repos, failed repos, and content hashes."""

    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.downloaded: Set[str] = set()
        self.failed: Set[str] = set()
        self.content_hashes: Dict[str, str] = {}
        self.load()

    def load(self):
        if not self.progress_file.exists():
            return
        try:
            with open(self.progress_file, "r") as f:
                data = json.load(f)
            self.downloaded = set(data.get("downloaded", []))
            self.failed = set(data.get("failed", []))
            self.content_hashes = data.get("content_hashes", {})
        except Exception:
            pass

    def save(self):
        with open(self.progress_file, "w") as f:
            json.dump({
                "downloaded": list(self.downloaded),
                "failed": list(self.failed),
                "content_hashes": self.content_hashes,
            }, f, indent=2)

    def is_downloaded(self, name):
        return name in self.downloaded

    def is_failed(self, name):
        return name in self.failed

    def add_downloaded(self, name, h=None):
        self.downloaded.add(name)
        if h:
            self.content_hashes[name] = h
        self.save()

    def add_failed(self, name):
        self.failed.add(name)
        self.save()

    def is_duplicate_hash(self, h):
        return h in self.content_hashes.values()


# ================================================================
# GITHUB API CLIENT
# ================================================================

class GitHubAPIClient:
    """Handles GitHub API interactions, retry logic, rate limits."""

    def __init__(self, token, retry_cfg):
        self.token = token
        self.base = "https://api.github.com"
        self.retry_cfg = retry_cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {token}" if token else "",
            "User-Agent": "Flutter-Scraper"
        })
    def check_rate(self):
        """Return GitHub rate limit information."""
        try:
            r = self.session.get(f"{self.base}/rate_limit", timeout=10)
            data = r.json()
            return data["resources"]["core"]
        except Exception:
            return None

    def _retry(self, fn, *args, **kwargs):
        """Generic retry wrapper for API calls."""
        max_retries = self.retry_cfg["max_retries"]
        base_backoff = self.retry_cfg["initial_backoff"]
        multiplier = self.retry_cfg["backoff_multiplier"]
        max_backoff = self.retry_cfg["max_backoff"]

        for attempt in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except Exception:
                if attempt == max_retries - 1:
                    raise
                backoff = min(base_backoff * (multiplier ** attempt), max_backoff)
                stats.retries_performed += 1
                time.sleep(backoff)

    def search_repositories(self, query, per_page=30, page=1):
        """Search GitHub Dart/Flutter repositories."""
        def fn():
            r = self.session.get(
                f"{self.base}/search/repositories",
                params={
                    "q": f"{query} language:dart",
                    "sort": "stars",
                    "order": "desc",
                    "page": page,
                    "per_page": per_page
                },
                timeout=20
            )

            if r.status_code == 403:  # rate limited
                rl = self.check_rate()
                if rl:
                    wait = rl["reset"] - int(time.time()) + 3
                    time.sleep(max(0, wait))
                return self.search_repositories(query, per_page, page)

            r.raise_for_status()
            return r.json()

        return self._retry(fn)

    def download_zipball(self, owner, repo):
        """Download repository zipball and return in-memory buffer."""
        def fn():
            r = self.session.get(
                f"{self.base}/repos/{owner}/{repo}/zipball",
                timeout=60,
                stream=True
            )

            if r.status_code == 403:
                rl = self.check_rate()
                if rl:
                    wait = rl["reset"] - int(time.time()) + 3
                    time.sleep(max(0, wait))
                return self.download_zipball(owner, repo)

            r.raise_for_status()

            buf = io.BytesIO()
            for chunk in r.iter_content(8192):
                buf.write(chunk)
            buf.seek(0)
            return buf

        return self._retry(fn)


# ================================================================
# REPOSITORY PROCESSOR
# ================================================================

class RepositoryProcessor:
    """Handles repo metadata parsing, extraction, architecture detection."""

    def __init__(self, config, api, progress):
        self.config = config
        self.api = api
        self.progress = progress
        self.output_dir = Path(config["output_dir"])
        self.jsonl_file = config["jsonl_file"]
        self.output_dir.mkdir(exist_ok=True)

    # -------------------------- META --------------------------

    def _parse_repo(self, item):
        """Convert GitHub API item → RepoMetadata object."""
        return RepoMetadata(
            full_name=item["full_name"],
            name=item["name"],
            owner=item["owner"]["login"],
            stars=item["stargazers_count"],
            forks=item["forks_count"],
            description=item.get("description", ""),
            url=item["html_url"],
            language=item["language"],
            updated_at=item["updated_at"],
            created_at=item["created_at"],
            size_kb=item["size"],
            is_fork=item["fork"],
            fork_source=item["parent"]["full_name"] if item["fork"] and "parent" in item else None,
            topics=item.get("topics", []),
            license=item.get("license", {}).get("spdx_id") if item.get("license") else None,
        )

    # ------------------------ FILTERS --------------------------

    def _passes_filters(self, m: RepoMetadata):
        """Apply star/age filters."""
        cfg = self.config["quality_filters"]

        if m.stars < cfg["min_stars"]:
            return False, "low stars"

        # Age check
        try:
            updated_dt = datetime.fromisoformat(m.updated_at.replace("Z", "+00:00"))
            years_old = (datetime.now(updated_dt.tzinfo) - updated_dt).days / 365
            if years_old > cfg["max_repo_age_years"]:
                return False, "too old"
        except Exception:
            pass

        return True, "OK"

    # ------------------ ARCHITECTURE DETECTION -----------------

    def _detect_architecture(self, paths, contents):
        """Detect state management / architecture style."""
        text = " ".join(paths).lower() + " " + " ".join(contents.values()).lower()

        if "riverpod" in text:
            return Architecture.RIVERPOD.value
        if "bloc" in text:
            return Architecture.BLOC.value
        if "cubit" in text:
            return Architecture.CUBIT.value
        if "provider" in text:
            return Architecture.PROVIDER.value
        if "getx" in text:
            return Architecture.GETX.value
        if "mobx" in text:
            return Architecture.MOBX.value
        if "redux" in text:
            return Architecture.REDUX.value
        if "domain/" in text and "presentation/" in text:
            return Architecture.CLEAN.value
        if "viewmodel" in text:
            return Architecture.MVVM.value

        return Architecture.UNKNOWN.value

    # ---------------------- DEPENDENCY PARSING -----------------

    def _extract_dependencies(self, pubspec):
        """Extract Flutter/Dart dependencies from pubspec.yaml."""
        try:
            spec = yaml.safe_load(pubspec)
            deps = spec.get("dependencies", {})
            parsed = {}

            for key, value in deps.items():
                if key == "flutter":
                    continue
                if isinstance(value, dict):
                    parsed[key] = value.get("version", "unknown")
                else:
                    parsed[key] = value

            flutter_version = spec.get("environment", {}).get("flutter")
            return parsed, flutter_version

        except Exception:
            return {}, None

    # ------------------------- FILE FILTERING ------------------

    def _is_relevant(self, path):
        """Determine if a file should be kept."""
        p = Path(path)
        cfg = self.config["file_filters"]

        if p.suffix not in cfg["extensions"]:
            return False

        if p.name in cfg["important_files"]:
            return True

        if "test" in p.parts:
            return False

        # check if any important dirs exist in path
        lowered_parts = [x.lower() for x in p.parts]
        for d in cfg["important_dirs"]:
            if d in lowered_parts:
                return True

        return False

    # ---------------------- JSONL + CLEANUP --------------------

    def _convert_to_jsonl_and_delete(self, repo_dir, meta, arch, deps, flutter_version):
        """Convert extracted project into JSONL record and delete folder."""
        record = {
            "repo": meta.full_name,
            "owner": meta.owner,
            "name": meta.name,
            "stars": meta.stars,
            "description": meta.description,
            "url": meta.url,
            "architecture": arch,
            "dependencies": deps,
            "flutter_version": flutter_version,
            "files": []
        }

        for root, _, files in os.walk(repo_dir):
            for f in files:
                fp = Path(root) / f
                try:
                    content = fp.read_text(errors="ignore")
                    record["files"].append({
                        "path": str(fp.relative_to(repo_dir)),
                        "content": content
                    })
                except Exception:
                    pass

        # write JSONL
        write_jsonl_record(self.output_dir, self.jsonl_file, record)

        # cleanup extraction folder
        for root, dirs, files in os.walk(repo_dir, topdown=False):
            for f in files:
                try:
                    os.remove(Path(root) / f)
                except Exception:
                    pass
            for d in dirs:
                try:
                    os.rmdir(Path(root) / d)
                except Exception:
                    pass

        try:
            os.rmdir(repo_dir)
        except Exception:
            pass

    # ------------------------ EXTRACTION ------------------------

    def extract_repository(self, meta, zip_data):
        """Extract only relevant Flutter files from downloaded zip."""
        owner, name = meta.owner, meta.name
        repo_dir = self.output_dir / f"{name}__{owner}"

        try:
            with zipfile.ZipFile(zip_data, "r") as z:
                files = z.namelist()

                # require pubspec
                if not any("pubspec.yaml" in f for f in files):
                    return ExtractionResult(
                        meta.full_name, False, 0, 0, "Unknown", {}, None, "no pubspec"
                    )

                root_prefix = files[0].split("/")[0] + "/"
                repo_dir.mkdir(exist_ok=True)

                extracted = []
                contents_small = {}
                pubspec = None
                size_total = 0
                count = 0
                for f in files:
                    rel = f[len(root_prefix):]

                    # skip directories
                    if not rel or f.endswith("/"):
                        continue

                    # skip irrelevant files
                    if not self._is_relevant(rel):
                        continue

                    try:
                        with z.open(f) as src:
                            data = src.read()

                        target = repo_dir / rel
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(data)

                        size_total += len(data)
                        count += 1
                        extracted.append(rel)

                        if rel == "pubspec.yaml":
                            pubspec = data.decode("utf8", errors="ignore")

                        # for architecture detection
                        if rel.endswith(".dart") and len(data) < 60000:
                            contents_small[rel] = data.decode("utf8", errors="ignore")

                    except Exception:
                        pass

                if count == 0:
                    return ExtractionResult(
                        meta.full_name, False, 0, 0, "", {}, None, "no files"
                    )

                # detect architecture
                arch = self._detect_architecture(extracted, contents_small)

                # extract dependencies
                deps, flutter_version = self._extract_dependencies(pubspec or "")

                # convert to JSONL and cleanup
                self._convert_to_jsonl_and_delete(repo_dir, meta, arch, deps, flutter_version)

                stats.repos_by_architecture[arch] += 1
                stats.total_files_extracted += count
                stats.total_size_bytes += size_total

                return ExtractionResult(
                    meta.full_name, True, count, size_total, arch, deps, flutter_version
                )

        except Exception as e:
            return ExtractionResult(
                meta.full_name, False, 0, 0, "Unknown", {}, None, str(e)
            )

    # ----------------------- PROCESS ONE REPO -----------------------

    def process_repository(self, meta):
        full_name = meta.full_name

        # skip downloaded repos
        if self.progress.is_downloaded(full_name):
            stats.total_skipped += 1
            return True

        # skip failed repos
        if self.progress.is_failed(full_name):
            stats.total_skipped += 1
            return False

        # apply filters
        ok, reason = self._passes_filters(meta)
        if not ok:
            stats.total_skipped += 1
            return False

        try:
            # download zip
            zip_data = self.api.download_zipball(meta.owner, meta.name)

            # dedup using md5 hash
            content_hash = hashlib.md5(zip_data.getvalue()).hexdigest()
            if self.config["deduplication"]["check_content_hash"]:
                if self.progress.is_duplicate_hash(content_hash):
                    stats.total_skipped += 1
                    return False

            # extract project
            result = self.extract_repository(meta, zip_data)

            if result.success:
                self.progress.add_downloaded(full_name, content_hash)
                stats.total_downloaded += 1
                return True
            else:
                self.progress.add_failed(full_name)
                stats.total_failed += 1
                return False

        except Exception:
            self.progress.add_failed(full_name)
            stats.total_failed += 1
            return False


# ================================================================
# MAIN SCRAPER
# ================================================================

class FlutterScraper:
    """Orchestrates searching, deduplication, downloading and saving."""

    def __init__(self, config):
        self.config = config
        self.api = GitHubAPIClient(GITHUB_API_TOKEN, config["retry_config"])
        self.progress = ProgressManager(Path(config["output_dir"]) / config["progress_file"])
        self.processor = RepositoryProcessor(config, self.api, self.progress)

    # ------------------------ SEARCHING -------------------------

    def search_all_queries(self):
        repos = []
        queries = self.config["search_queries"]
        max_per = self.config["scraping_limits"]["repos_per_query"]

        iterator = tqdm(queries, desc="Searching") if TQDM_AVAILABLE else queries

        for query in iterator:
            found = 0
            page = 1

            while found < max_per:
                result = self.api.search_repositories(query, 30, page)
                items = result.get("items", [])

                if not items:
                    break

                for item in items:
                    repos.append(self.processor._parse_repo(item))
                    found += 1
                    if found >= max_per:
                        break

                page += 1
                time.sleep(1)

            stats.total_searched += found
            logging.info(f"Query '{query}' → {found} repos")

        return repos

    # ---------------------- DEDUPLICATION -----------------------

    def deduplicate_repos(self, repos):
        seen = set()
        unique_repos = []

        for repo in repos:
            if repo.full_name in seen:
                continue

            if self.progress.is_downloaded(repo.full_name):
                continue

            # dedup forks
            if self.config["deduplication"]["check_fork_relationship"]:
                if repo.is_fork and repo.fork_source:
                    if repo.fork_source in seen or self.progress.is_downloaded(repo.fork_source):
                        continue

            seen.add(repo.full_name)
            unique_repos.append(repo)

        logging.info(f"Dedup: {len(repos)} → {len(unique_repos)}")
        return unique_repos

    # ------------------------ DOWNLOADING -----------------------

    def download_repos(self, repos):
        if not repos:
            return

        limit = self.config["scraping_limits"]["max_total_repos"]
        repos = repos[:limit]

        delay = self.config["scraping_limits"]["delay_between_downloads"]
        max_workers = self.config["scraping_limits"]["max_parallel_downloads"]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.processor.process_repository, r): r for r in repos}

            iterator = tqdm(as_completed(futures), total=len(futures), desc="Downloading") \
                if TQDM_AVAILABLE else as_completed(futures)

            for future in iterator:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in thread: {e}")
                time.sleep(delay)

    # ----------------------- METADATA SAVE ----------------------

    def save_metadata(self):
        meta_path = Path(self.config["output_dir"]) / self.config["metadata_file"]

        metadata = {
            "generated_at": datetime.now().isoformat(),
            "total_repos": stats.total_downloaded,
            "total_files": stats.total_files_extracted,
            "size_bytes": stats.total_size_bytes,
            "architectures": dict(stats.repos_by_architecture),
            "stats": {
                "searched": stats.total_searched,
                "downloaded": stats.total_downloaded,
                "skipped": stats.total_skipped,
                "failed": stats.total_failed,
            }
        }

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    # ---------------------------- RUN ---------------------------

    def run(self):
        logging.info("=== SCRAPER STARTING ===")

        repos = self.search_all_queries()
        repos = self.deduplicate_repos(repos)

        self.download_repos(repos)

        self.save_metadata()
        stats.print_summary()

        logging.info(f"Dataset stored in: {Path(self.config['output_dir']).absolute()}")


# ================================================================
# ENTRY POINT
# ================================================================

def main():
    if not GITHUB_API_TOKEN:
        print("❌ ERROR: Please set your GitHub token in the environment variable GITHUB_TOKEN")
        return

    config = load_config()
    setup_logging(config["log_file"])

    scraper = FlutterScraper(config)
    scraper.run()


if __name__ == "__main__":
    main()
