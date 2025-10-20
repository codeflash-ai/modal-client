# Copyright Modal Labs 2025
import asyncio
from typing import Optional

from modal.config import logger
from modal_proto import api_pb2


async def run_command_fallible(args: list[str]) -> Optional[str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout_bytes, _ = await process.communicate()

        if process.returncode != 0:
            logger.debug(f"Command {args} exited with code {process.returncode}")
            return None

        return stdout_bytes.decode("utf-8").strip()

    except Exception as e:
        logger.debug(f"Command {args} failed", exc_info=e)
        return None


def is_valid_commit_info(commit_info: api_pb2.CommitInfo) -> tuple[bool, str]:
    # returns (valid, error_message)
    vcs = commit_info.vcs
    commit_hash = commit_info.commit_hash
    branch = commit_info.branch
    repo_url = commit_info.repo_url
    author_name = commit_info.author_name
    author_email = commit_info.author_email

    if vcs != "git":
        return False, "Invalid VCS"
    if len(commit_hash) != 40:
        return False, "Invalid commit hash"
    if len(branch) > 255:
        # Git doesn't enforce a max length for branch names, but github does, so use their limit
        # https://stackoverflow.com/questions/24014361/max-length-of-git-branch-name
        return False, "Branch name too long"
    if len(repo_url) > 200:
        return False, "Repo URL too long"
    if len(author_name) > 200:
        return False, "Author name too long"
    if len(author_email) > 200:
        return False, "Author email too long"
    return True, ""


async def get_git_commit_info() -> Optional[api_pb2.CommitInfo]:
    """Collect git information about the current repository asynchronously."""

    # Avoid repeated attribute lookups for faster assignment (micro-optimization).
    git_info: api_pb2.CommitInfo = api_pb2.CommitInfo(vcs="git")

    # Define commands as immutable tuples for a minor overhead reduction.
    commands = (
        ("git", "log", "-1", "--format=%H%n%ct%n%an%n%ae", "HEAD"),
        ("git", "rev-parse", "--abbrev-ref", "HEAD"),
        ("git", "status", "--porcelain"),
        ("git", "remote", "get-url", "origin"),
    )

    # Launch all tasks concurrently but avoid generator overhead via list comprehension.
    tasks = [run_command_fallible(cmd) for cmd in commands]
    log_info, branch, status, origin_url = await asyncio.gather(*tasks)

    if not branch:
        return None

    git_info.branch = branch

    if not log_info:
        return None

    info_lines = log_info.split("\n")
    if len(info_lines) < 4:
        logger.debug(f"Log info returned only {len(info_lines)} lines")
        return None

    try:
        (
            git_info.commit_hash,
            commit_timestamp,
            git_info.author_name,
            git_info.author_email,
        ) = info_lines[:4]
        git_info.commit_timestamp = int(commit_timestamp)
    except (ValueError, IndexError):
        logger.debug(f"Failed to parse git log info: {log_info}")
        return None

    git_info.dirty = bool(status)

    if origin_url:
        git_info.repo_url = origin_url

    valid, error_message = is_valid_commit_info(git_info)
    if not valid:
        logger.warning(f"Invalid commit info: {error_message}")
        return None

    return git_info
