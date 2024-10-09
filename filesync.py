#!/usr/bin/env python3

import argparse
import hashlib
import json
import logging
import os
import queue
import stat
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import paramiko
from paramiko import SFTPClient
from tqdm import tqdm

# Type Aliases
Index = Dict[str, List['FileEntry']]
Connection = Tuple[paramiko.SSHClient, SFTPClient]


@dataclass
class FileEntry:
    """
    Represents a file's metadata.
    """
    name: str
    path: str
    relpath: str
    size: int
    last_modified: float


class SFTPConnectionPool:
    """
    A pool to manage SFTP connections.
    """

    def __init__(self, hostname: str, username: str, key_filename: str, pool_size: int) -> None:
        self.hostname = hostname
        self.username = username
        self.key_filename = key_filename
        self.pool_size = pool_size
        self.pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """
        Initialize the connection pool with SSH and SFTP clients.
        """
        for i in range(self.pool_size):
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect(
                    hostname=self.hostname,
                    username=self.username,
                    key_filename=self.key_filename,
                )
                sftp = ssh.open_sftp()
                self.pool.put((ssh, sftp))
                logging.debug(f"SFTP connection {i+1} added to pool.")
            except Exception as e:
                logging.error(f"Error connecting to {self.hostname}: {e}")
                self.close_all()
                sys.exit(1)

    def acquire(self) -> Connection:
        """
        Acquire an SFTP connection from the pool.
        """
        try:
            connection = self.pool.get(timeout=10)
            logging.debug("SFTP connection acquired from pool.")
            return connection
        except queue.Empty:
            logging.error("No available SFTP connections in the pool.")
            self.close_all()
            sys.exit(1)

    def release(self, connection: Connection) -> None:
        """
        Release an SFTP connection back to the pool.
        """
        self.pool.put(connection)
        logging.debug("SFTP connection released back to pool.")

    def close_all(self) -> None:
        """
        Close all SSH and SFTP connections in the pool.
        """
        while not self.pool.empty():
            ssh, sftp = self.pool.get()
            try:
                sftp.close()
                ssh.close()
                logging.debug("SFTP connection closed.")
            except Exception as e:
                logging.error(f"Error closing connection: {e}")


class FileIndexer:
    """
    Handles indexing of local and remote directories.
    """

    def __init__(
        self,
        local_dir: str,
        remote_dir: str,
        index_dir: str,
        update_interval: int,
        ignore_case: bool,
        allowed_extensions: Optional[Set[str]],
        sftp_pool: SFTPConnectionPool,
        max_workers: int,
    ) -> None:
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.index_dir = index_dir
        self.update_interval = update_interval
        self.ignore_case = ignore_case
        self.allowed_extensions = allowed_extensions
        self.sftp_pool = sftp_pool
        self.max_workers = max_workers

    def build_local_index(self) -> Index:
        """
        Build an index of local files.
        """
        logging.info("Building local index...")
        index_path = os.path.join(self.index_dir, 'local_index.json')
        local_index: Index = {}
        last_update = 0

        if os.path.exists(index_path):
            last_update = os.path.getmtime(index_path)
            try:
                with open(index_path, 'r') as file:
                    data = json.load(file)
                local_index = {
                    key: [FileEntry(**entry) for entry in entries]
                    for key, entries in data.items()
                }
                logging.debug("Loaded existing local index.")
            except Exception as e:
                logging.error(f"Error loading local index: {e}")

        current_time = time.time()
        if current_time - last_update < self.update_interval:
            logging.info("Local index is up-to-date.")
            return {
                key: [entry.__dict__ for entry in entries]
                for key, entries in local_index.items()
            }

        # Rebuild the index
        logging.info("Scanning local directory...")
        file_count = 0
        for root, _, files in os.walk(self.local_dir):
            for name in files:
                ext = os.path.splitext(name)[1][1:].lower()
                if self.allowed_extensions and ext not in self.allowed_extensions:
                    continue  # Skip disallowed extensions

                key_name = name.lower() if self.ignore_case else name
                filepath = os.path.join(root, name)
                relpath = os.path.relpath(filepath, self.local_dir)
                try:
                    size = os.path.getsize(filepath)
                    mtime = os.path.getmtime(filepath)
                except Exception as e:
                    logging.error(f"Error accessing file '{filepath}': {e}")
                    continue

                entry = FileEntry(
                    name=name,
                    path=filepath,
                    relpath=relpath,
                    size=size,
                    last_modified=mtime,
                )
                local_index.setdefault(key_name, []).append(entry)
                file_count += 1

                if file_count % 100 == 0:
                    logging.debug(f"Indexed {file_count} local files...")

        logging.info(f"Total local files indexed: {file_count}")

        try:
            os.makedirs(self.index_dir, exist_ok=True)
            with open(index_path, 'w') as file:
                # Convert FileEntry objects to dictionaries for JSON serialization
                serializable_index = {
                    key: [entry.__dict__ for entry in entries]
                    for key, entries in local_index.items()
                }
                json.dump(serializable_index, file, indent=4)
            logging.info("Local index saved.")
        except Exception as e:
            logging.error(f"Error saving local index: {e}")

        return serializable_index

    def build_remote_index(self) -> Index:
        """
        Build an index of remote files using a queue-based multithreading approach.
        """
        logging.info("Building remote index...")
        index_path = os.path.join(self.index_dir, 'remote_index.json')
        remote_index: Index = {}
        last_update = 0

        if os.path.exists(index_path):
            last_update = os.path.getmtime(index_path)
            try:
                with open(index_path, 'r') as file:
                    data = json.load(file)
                remote_index = {
                    key: [FileEntry(**entry) for entry in entries]
                    for key, entries in data.items()
                }
                logging.debug("Loaded existing remote index.")
            except Exception as e:
                logging.error(f"Error loading remote index: {e}")

        current_time = time.time()
        if current_time - last_update < self.update_interval:
            logging.info("Remote index is up-to-date.")
            return {
                key: [entry.__dict__ for entry in entries]
                for key, entries in remote_index.items()
            }

        remote_index = {}
        file_count = 0
        lock = threading.Lock()

        directory_queue: queue.Queue = queue.Queue()
        directory_queue.put((self.remote_dir, ''))

        def worker(thread_id: int) -> None:
            nonlocal file_count
            while True:
                try:
                    current_remote_dir, rel_path = directory_queue.get(timeout=5)
                except queue.Empty:
                    logging.debug(f"Worker {thread_id} exiting: No more directories to process.")
                    return

                connection = self.sftp_pool.acquire()
                ssh, sftp = connection
                try:
                    logging.debug(f"Worker {thread_id} scanning directory: {current_remote_dir}")
                    for item in sftp.listdir_attr(current_remote_dir):
                        item_path = os.path.join(current_remote_dir, item.filename)
                        item_rel_path = os.path.join(rel_path, item.filename)
                        mode = item.st_mode

                        if stat.S_ISDIR(mode):
                            directory_queue.put((item_path, item_rel_path))
                        elif stat.S_ISREG(mode):
                            ext = os.path.splitext(item.filename)[1][1:].lower()
                            if self.allowed_extensions and ext not in self.allowed_extensions:
                                continue  # Skip disallowed extensions

                            key_name = item.filename.lower() if self.ignore_case else item.filename
                            entry = FileEntry(
                                name=item.filename,
                                path=item_path,
                                relpath=item_rel_path,
                                size=item.st_size,
                                last_modified=item.st_mtime,
                            )

                            with lock:
                                remote_index.setdefault(key_name, []).append(entry)
                                file_count += 1
                                if file_count % 100 == 0:
                                    logging.debug(f"Indexed {file_count} remote files...")
                except Exception as e:
                    logging.error(f"Error scanning remote path '{current_remote_dir}': {e}")
                finally:
                    self.sftp_pool.release(connection)
                    directory_queue.task_done()

        logging.info("Scanning remote directory with multithreading...")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Start worker threads
            futures = [executor.submit(worker, thread_id=i+1) for i in range(self.max_workers)]
            # Wait for all directories to be processed
            directory_queue.join()
            # Wait for all workers to finish
            for future in as_completed(futures):
                if future.exception():
                    logging.error(f"Worker thread encountered an error: {future.exception()}")

        logging.info(f"Total remote files indexed: {file_count}")

        try:
            os.makedirs(self.index_dir, exist_ok=True)
            with open(index_path, 'w') as file:
                # Convert FileEntry objects to dictionaries for JSON serialization
                serializable_index = {
                    key: [entry.__dict__ for entry in entries]
                    for key, entries in remote_index.items()
                }
                json.dump(serializable_index, file, indent=4)
            logging.info("Remote index saved.")
        except Exception as e:
            logging.error(f"Error saving remote index: {e}")

        return serializable_index


class FileSynchronizer:
    """
    Handles synchronization between local and remote files.
    """

    def __init__(
        self,
        args: argparse.Namespace,
        local_index: Index,
        remote_index: Index,
        sftp_pool: SFTPConnectionPool,
    ) -> None:
        self.args = args
        self.local_index = self._convert_index(local_index)
        self.remote_index = self._convert_index(remote_index)
        self.sftp_pool = sftp_pool

    @staticmethod
    def _convert_index(index: Index) -> Dict[str, List[FileEntry]]:
        """
        Convert index with dictionaries to index with FileEntry objects.
        """
        converted_index: Dict[str, List[FileEntry]] = {}
        for key, entries in index.items():
            converted_entries = [FileEntry(**entry) for entry in entries]
            converted_index[key] = converted_entries
        return converted_index

    def compute_file_hash(self, filepath: str) -> Optional[str]:
        """
        Compute SHA256 hash of a local file.
        """
        hash_sha256 = hashlib.sha256()
        try:
            with open(filepath, 'rb') as file:
                for chunk in iter(lambda: file.read(8192), b''):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logging.error(f"Error computing hash for {filepath}: {e}")
            return None

    def compute_remote_file_hash(self, ssh: paramiko.SSHClient, remote_path: str) -> Optional[str]:
        """
        Compute SHA256 hash of a remote file via SSH.
        """
        try:
            command = f"sha256sum '{remote_path}'"
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode().strip()
            if output:
                return output.split()[0]
            else:
                logging.error(f"No hash output for remote file {remote_path}")
                return None
        except Exception as e:
            logging.error(f"Error computing remote hash for {remote_path}: {e}")
            return None

    def progress_callback_factory(self, tqdm_bar: tqdm) -> Any:
        """
        Factory to create a progress callback function for tqdm.
        """

        def callback(transferred: int, total: int) -> None:
            tqdm_bar.update(transferred - tqdm_bar.n)

        return callback

    def process_file(self, key_name: str) -> None:
        """
        Process a single file: compare local and remote versions and download if necessary.
        """
        local_entries = self.local_index.get(key_name, [])
        remote_entries = self.remote_index.get(key_name, [])

        if not local_entries or not remote_entries:
            # File doesn't exist in both directories
            return

        # Apply --match filter
        if self.args.match:
            match_found = False
            for entry in local_entries + remote_entries:
                filename = entry.name
                if self.args.ignore_case:
                    if self.args.match.lower() in filename.lower():
                        match_found = True
                        break
                else:
                    if self.args.match in filename:
                        match_found = True
                        break
            if not match_found:
                logging.debug(f"No match for '{self.args.match}' in '{key_name}'")
                return

        # Compare each local file with remote files
        for local_entry in local_entries:
            local_size = local_entry.size
            local_mtime = local_entry.last_modified

            # Find matching remote file
            for remote_entry in remote_entries:
                remote_size = remote_entry.size
                remote_mtime = remote_entry.last_modified

                need_download = False

                if local_size != remote_size:
                    need_download = True
                elif self.args.compute_hash:
                    # Compute local file hash
                    local_hash = self.compute_file_hash(local_entry.path)
                    if local_hash is None:
                        continue  # Skip if hash computation failed

                    # Compute remote file hash
                    connection = self.sftp_pool.acquire()
                    ssh, sftp = connection
                    remote_hash = self.compute_remote_file_hash(ssh, remote_entry.path)
                    self.sftp_pool.release(connection)

                    if remote_hash is None or local_hash != remote_hash:
                        need_download = True

                if need_download:
                    if self.args.interactive:
                        prompt_message = "\n" + "=" * 80 + "\n"
                        prompt_message += f"File '{local_entry.relpath}' differs from remote.\n"
                        prompt_message += f"Local size: {local_size}, Remote size: {remote_size}\n"
                        prompt_message += "Do you want to download and overwrite the local file? (y/n): "
                        sys.stdout.flush()
                        try:
                            approval = input(prompt_message).lower()
                        except EOFError:
                            approval = 'n'  # Default to 'no' if input fails
                        if approval != 'y':
                            logging.info(f"Skipped downloading '{local_entry.relpath}'.")
                            continue

                    try:
                        logging.info(f"Downloading '{remote_entry.relpath}' to '{local_entry.relpath}'")
                        os.makedirs(os.path.dirname(local_entry.path), exist_ok=True)

                        connection = self.sftp_pool.acquire()
                        ssh, sftp = connection

                        with tqdm(
                            total=remote_size,
                            unit='B',
                            unit_scale=True,
                            desc=local_entry.relpath,
                            leave=False,
                        ) as pbar:
                            callback = self.progress_callback_factory(pbar)
                            sftp.get(remote_entry.path, local_entry.path, callback=callback)

                        self.sftp_pool.release(connection)

                        # Verify file size after download
                        new_local_size = os.path.getsize(local_entry.path)
                        if new_local_size == remote_size:
                            logging.info(f"Download successful for '{local_entry.relpath}'.")
                            # Update local index entry
                            local_entry.size = new_local_size
                            local_entry.last_modified = os.path.getmtime(local_entry.path)
                        else:
                            logging.error(f"File size mismatch after download for '{local_entry.relpath}'.")

                    except Exception as e:
                        logging.error(f"Error updating '{local_entry.relpath}': {e}")

                else:
                    logging.debug(f"File '{local_entry.relpath}' is up-to-date.")

                break  # Move to next local file


class FileSyncApp:
    """
    Main application controller for file synchronization.
    """

    def __init__(self) -> None:
        self.args = self.parse_arguments()
        self.configure_logging()
        self.allowed_extensions = self.parse_allowed_extensions()
        self.sftp_pool = SFTPConnectionPool(
            hostname=self.args.hostname,
            username=self.args.username,
            key_filename=self.args.key_filename,
            pool_size=self.args.max_workers,
        )
        self.file_indexer = FileIndexer(
            local_dir=self.args.local_dir,
            remote_dir=self.args.remote_dir,
            index_dir=self.args.index_dir,
            update_interval=self.args.update_interval,
            ignore_case=self.args.ignore_case,
            allowed_extensions=self.allowed_extensions,
            sftp_pool=self.sftp_pool,
            max_workers=self.args.max_workers,
        )
        self.file_synchronizer: Optional[FileSynchronizer] = None

    def parse_arguments(self) -> argparse.Namespace:
        """
        Parse command-line arguments.
        """
        parser = argparse.ArgumentParser(description='Synchronize files between local and remote directories.')
        parser.add_argument('--local-dir', required=True, help='Path to the local directory.')
        parser.add_argument('--remote-dir', required=True, help='Path to the remote directory.')
        parser.add_argument('--hostname', required=True, help='Remote host name or IP address.')
        parser.add_argument('--username', required=True, help='Username for SSH authentication.')
        parser.add_argument('--key-filename', required=True, help='Path to the SSH private key file.')
        parser.add_argument('--index-dir', required=True, help='Directory to store index files.')
        parser.add_argument('--update-interval', type=int, default=3600, help='Index update interval in seconds.')
        parser.add_argument('--max-workers', type=int, default=8, help='Maximum number of worker threads.')
        parser.add_argument('--interactive', action='store_true', help='Require approval before downloading files.')
        parser.add_argument('--match', help='Only process files containing the specified substring in the filename.')
        parser.add_argument('--ignore-case', action='store_true', help='Perform case-insensitive filename matching.')
        parser.add_argument('--type', help='Comma-separated list of file extensions to include (e.g., mkv,mp4,avi).')
        parser.add_argument(
            '--log-level',
            default='INFO',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            help='Set the logging level (default: INFO).',
        )
        parser.add_argument(
            '--compute-hash',
            action='store_true',
            help='Compute and compare SHA256 hashes for files.',
        )
        return parser.parse_args()

    def configure_logging(self) -> None:
        """
        Configure logging based on command-line arguments.
        """
        numeric_level = getattr(logging, self.args.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f'Invalid log level: {self.args.log_level}')
        logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')

        # Adjust logging level in interactive mode to reduce clutter
        if self.args.interactive and numeric_level <= logging.DEBUG:
            logging.getLogger().setLevel(logging.INFO)

    def parse_allowed_extensions(self) -> Optional[Set[str]]:
        """
        Parse allowed file extensions from command-line arguments.
        """
        if self.args.type:
            allowed_extensions = set(ext.strip().lower() for ext in self.args.type.split(','))
            logging.info(f"Filtering files with extensions: {', '.join(allowed_extensions)}")
            return allowed_extensions
        else:
            return None  # No filtering on extensions

    def run(self) -> None:
        """
        Execute the synchronization process.
        """
        try:
            # Build local index
            local_index = self.file_indexer.build_local_index()

            # Build remote index
            remote_index = self.file_indexer.build_remote_index()

            # Initialize synchronizer
            self.file_synchronizer = FileSynchronizer(
                args=self.args,
                local_index=local_index,
                remote_index=remote_index,
                sftp_pool=self.sftp_pool,
            )

            # Find common filenames (keys)
            common_keys = set(self.file_synchronizer.local_index.keys()) & set(self.file_synchronizer.remote_index.keys())
            logging.info(f"Total common filenames: {len(common_keys)}")

            if not common_keys:
                logging.info("No common files to process.")
                return

            # Collect keys to process
            matching_keys = list(common_keys)

            # Process files
            logging.info("Starting file synchronization...")
            if self.args.interactive:
                # Process files sequentially in interactive mode
                for key_name in matching_keys:
                    self.file_synchronizer.process_file(key_name)
            else:
                # Use ThreadPoolExecutor for multithreading
                with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
                    futures = [
                        executor.submit(self.file_synchronizer.process_file, key_name)
                        for key_name in matching_keys
                    ]
                    for future in as_completed(futures):
                        if future.exception():
                            logging.error(f"Error processing file: {future.exception()}")

            # Save updated local index with FileEntry objects
            self.save_updated_local_index(self.file_synchronizer.local_index)

        finally:
            # Close all SFTP connections
            self.sftp_pool.close_all()
            logging.info("File synchronization complete.")

    def save_updated_local_index(self, local_index: Index) -> None:
        """
        Save the updated local index to disk.
        """
        local_index_path = os.path.join(self.args.index_dir, 'local_index.json')
        try:
            with open(local_index_path, 'w') as file:
                # Convert FileEntry objects to dictionaries for JSON serialization
                serializable_index = {
                    key: [entry.__dict__ for entry in entries]
                    for key, entries in local_index.items()
                }
                json.dump(serializable_index, file, indent=4)
            logging.info("Local index updated.")
        except Exception as e:
            logging.error(f"Error saving updated local index: {e}")


def main() -> None:
    """
    Entry point of the script.
    """
    app = FileSyncApp()
    app.run()


if __name__ == '__main__':
    main()
