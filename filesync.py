#!/usr/bin/env python3

import os
import json
import paramiko
from concurrent.futures import ThreadPoolExecutor
import threading
import hashlib
import time
import argparse
import stat
import sys
import logging

# Import for progress bar
import functools

# Thread-local storage for SSH connections
thread_local = threading.local()

def get_sftp_client(hostname, username, key_filename):
    if not hasattr(thread_local, 'sftp'):
        logging.debug("Establishing SSH connection...")
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(hostname, username=username, key_filename=key_filename)
            thread_local.ssh = ssh
            thread_local.sftp = ssh.open_sftp()
            logging.debug("SSH connection established.")
        except Exception as e:
            logging.error(f"Error connecting to {hostname}: {e}")
            sys.exit(1)
    return thread_local.sftp

def compute_file_hash(filepath):
    """Compute SHA256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def build_local_index(local_dir, index_dir, update_interval, ignore_case, allowed_extensions):
    logging.info("Building local index...")
    index_path = os.path.join(index_dir, 'local_index.json')
    local_index = {}
    last_update = 0

    if os.path.exists(index_path):
        last_update = os.path.getmtime(index_path)
        with open(index_path, 'r') as f:
            local_index = json.load(f)
        logging.debug("Loaded existing local index.")

    current_time = time.time()
    if current_time - last_update < update_interval:
        logging.info("Local index is up-to-date.")
        return local_index

    # Rebuild the index
    logging.info("Scanning local directory...")
    file_count = 0
    for root, dirs, files in os.walk(local_dir):
        for name in files:
            # Check if file extension is in allowed_extensions
            ext = os.path.splitext(name)[1][1:].lower()  # Get extension without dot
            if allowed_extensions and ext not in allowed_extensions:
                continue  # Skip files with disallowed extensions

            key_name = name.lower() if ignore_case else name
            filepath = os.path.join(root, name)
            relpath = os.path.relpath(filepath, local_dir)
            size = os.path.getsize(filepath)
            mtime = os.path.getmtime(filepath)
            entry = {
                'name': name,  # Original filename
                'path': filepath,
                'relpath': relpath,
                'size': size,
                'last_modified': mtime,
            }
            local_index.setdefault(key_name, []).append(entry)
            file_count += 1
            if file_count % 100 == 0:
                logging.debug(f"Indexed {file_count} local files...")
    logging.info(f"Total local files indexed: {file_count}")

    with open(index_path, 'w') as f:
        json.dump(local_index, f)
    logging.info("Local index saved.")

    return local_index

def build_remote_index(sftp, remote_dir, index_dir, update_interval, ignore_case, allowed_extensions):
    logging.info("Building remote index...")
    index_path = os.path.join(index_dir, 'remote_index.json')
    remote_index = {}
    last_update = 0

    if os.path.exists(index_path):
        last_update = os.path.getmtime(index_path)
        with open(index_path, 'r') as f:
            remote_index = json.load(f)
        logging.debug("Loaded existing remote index.")

    current_time = time.time()
    if current_time - last_update < update_interval:
        logging.info("Remote index is up-to-date.")
        return remote_index

    remote_index = {}
    file_count = 0

    def recursive_scan(remote_path, rel_path=''):
        nonlocal file_count
        try:
            for item in sftp.listdir_attr(remote_path):
                item_path = os.path.join(remote_path, item.filename)
                item_rel_path = os.path.join(rel_path, item.filename)
                mode = item.st_mode
                if stat.S_ISDIR(mode):
                    recursive_scan(item_path, item_rel_path)
                elif stat.S_ISREG(mode):
                    # Check if file extension is in allowed_extensions
                    ext = os.path.splitext(item.filename)[1][1:].lower()  # Get extension without dot
                    if allowed_extensions and ext not in allowed_extensions:
                        continue  # Skip files with disallowed extensions

                    key_name = item.filename.lower() if ignore_case else item.filename
                    size = item.st_size
                    mtime = item.st_mtime
                    entry = {
                        'name': item.filename,  # Original filename
                        'path': item_path,
                        'relpath': item_rel_path,
                        'size': size,
                        'last_modified': mtime,
                    }
                    remote_index.setdefault(key_name, []).append(entry)
                    file_count += 1
                    if file_count % 100 == 0:
                        logging.debug(f"Indexed {file_count} remote files...")
        except Exception as e:
            logging.error(f"Error scanning remote path {remote_path}: {e}")

    logging.info("Scanning remote directory...")
    recursive_scan(remote_dir)
    logging.info(f"Total remote files indexed: {file_count}")

    with open(index_path, 'w') as f:
        json.dump(remote_index, f)
    logging.info("Remote index saved.")

    return remote_index

def progress_callback(transferred, total):
    progress = transferred / total * 100
    sys.stdout.write(f"\rDownloading... {progress:.2f}%")
    sys.stdout.flush()
    if transferred == total:
        print()  # Move to the next line after completion

def process_file(args, key_name):
    sftp = get_sftp_client(args.hostname, args.username, args.key_filename)
    local_entries = args.local_index.get(key_name, [])
    remote_entries = args.remote_index.get(key_name, [])

    if not local_entries or not remote_entries:
        # File doesn't exist in both directories
        return

    # Apply --match filter
    if args.match:
        # Use the original filenames for matching
        match_found = False
        for entry in local_entries + remote_entries:
            filename = entry['name']
            if args.ignore_case:
                if args.match.lower() in filename.lower():
                    match_found = True
                    break
            else:
                if args.match in filename:
                    match_found = True
                    break
        if not match_found:
            logging.debug(f"No match for '{args.match}' in '{filename}'")
            return

    # Compare each local file with remote files
    for local_entry in local_entries:
        local_size = local_entry['size']
        local_mtime = local_entry['last_modified']

        # Find matching remote file
        for remote_entry in remote_entries:
            remote_size = remote_entry['size']
            remote_mtime = remote_entry['last_modified']

            # Compare sizes
            if local_size != remote_size:
                local_path = local_entry['path']
                remote_path = remote_entry['path']
                if args.interactive:
                    # Prompt for approval
                    prompt_message = "\n" + "="*80 + "\n"
                    prompt_message += f"File '{local_entry['relpath']}' differs from remote.\n"
                    prompt_message += f"Local size: {local_size}, Remote size: {remote_size}\n"
                    prompt_message += "Do you want to download and overwrite the local file? (y/n): "
                    sys.stdout.flush()
                    approval = input(prompt_message).lower()
                    if approval != 'y':
                        logging.info("Skipped.")
                        continue
                try:
                    logging.info(f"Downloading '{remote_entry['relpath']}' to '{local_entry['relpath']}'")
                    # Ensure the local directory exists
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)

                    # Use callback for progress indication
                    sftp.get(remote_path, local_path, callback=progress_callback)

                    # Verify file size after download
                    new_local_size = os.path.getsize(local_path)
                    if new_local_size == remote_size:
                        logging.info(f"Download successful. File sizes match.")
                        # Update local index entry
                        local_entry['size'] = new_local_size
                        local_entry['last_modified'] = os.path.getmtime(local_path)
                    else:
                        logging.error(f"File size mismatch after download for '{local_entry['relpath']}'")
                        # Optionally, implement retry logic here
                except Exception as e:
                    logging.error(f"Error updating '{local_entry['relpath']}': {e}")
                break  # Move to next local file
            else:
                # Files match, no action needed
                logging.debug(f"File '{local_entry['relpath']}' is up-to-date.")
                break

def main():
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
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set the logging level (default: INFO).')
    args = parser.parse_args()

    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {args.log_level}')
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')

    # Adjust logging level in interactive mode to reduce clutter
    if args.interactive and numeric_level <= logging.DEBUG:
        logging.getLogger().setLevel(logging.INFO)

    # Parse allowed extensions
    if args.type:
        allowed_extensions = set(ext.strip().lower() for ext in args.type.split(','))
        logging.info(f"Filtering files with extensions: {', '.join(allowed_extensions)}")
    else:
        allowed_extensions = None  # No filtering on extensions

    # Build indexes
    args.local_index = build_local_index(args.local_dir, args.index_dir, args.update_interval, args.ignore_case, allowed_extensions)
    sftp = get_sftp_client(args.hostname, args.username, args.key_filename)
    args.remote_index = build_remote_index(sftp, args.remote_dir, args.index_dir, args.update_interval, args.ignore_case, allowed_extensions)

    # Find common filenames (keys)
    common_keys = set(args.local_index.keys()) & set(args.remote_index.keys())
    logging.info(f"Total common filenames: {len(common_keys)}")

    if not common_keys:
        logging.info("No common files to process.")
        return

    # Collect keys to process
    matching_keys = list(common_keys)

    # Process files
    logging.info("Starting file synchronization...")
    if args.interactive:
        # Process files sequentially in interactive mode
        for key_name in matching_keys:
            process_file(args, key_name)
    else:
        # Use ThreadPoolExecutor for multithreading
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            executor.map(lambda key_name: process_file(args, key_name), matching_keys)

    # Save updated local index
    index_path = os.path.join(args.index_dir, 'local_index.json')
    with open(index_path, 'w') as f:
        json.dump(args.local_index, f)
    logging.info("Local index updated.")

    # Close SSH connections
    if hasattr(thread_local, 'sftp'):
        thread_local.sftp.close()
        thread_local.ssh.close()
    logging.info("File synchronization complete.")

if __name__ == '__main__':
    main()
