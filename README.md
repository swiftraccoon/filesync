# FileSync 📁🚀

**Effortlessly synchronize your local and remote directories with style!**

---

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Synchronization](#basic-synchronization)
  - [Interactive Mode](#interactive-mode)
  - [Filtering Files](#filtering-files)
  - [Advanced Options](#advanced-options)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

---

## Introduction

Welcome to **FileSyncer**—your friendly neighborhood script for synchronizing files between local and remote directories over SFTP. Whether you're dealing with large media files or keeping codebases in sync, FileSyncer has got your back.

Say goodbye to manual transfers and hello to automated synchronization with a touch of elegance and a sprinkle of fun! 🌟

---

## Features

- **Two-Way Synchronization**: Compare and sync files between local and remote directories.
- **File Matching**: Use patterns to match specific files.
- **Case-Insensitive Matching**: Ignore case differences in filenames.
- **Extension Filtering**: Sync only the file types you care about (e.g., `.mkv`, `.mp4`).
- **Interactive Mode**: Get prompts before overwriting files.
- **Progress Indicators**: Enjoy real-time progress bars during file transfers.
- **Logging**: Keep track of what's happening with adjustable log levels.
- **Multi-Threaded Performance**: Utilize multiple threads for faster synchronization when not in interactive mode.

---

## Installation

First, ensure you have **Python 3.6+** installed. Then, clone the repository and install the required packages:

```bash
git clone https://github.com/yourusername/filesyncer.git
cd filesyncer
pip install -r requirements.txt
```

---

## Usage

Run the script using Python:

```bash
python filesync.py [options]
```

### Basic Synchronization

Synchronize files between your local and remote directories:

```bash
python filesync.py \
  --local-dir "/path/to/local/dir" \
  --remote-dir "/path/to/remote/dir" \
  --hostname "your.remote.host" \
  --username "your_username" \
  --key-filename "/path/to/ssh/key" \
  --index-dir "/path/to/index/dir"
```

### Interactive Mode

Enable interactive mode to get prompts before overwriting files:

```bash
--interactive
```

### Filtering Files

Match files containing a specific substring:

```bash
--match "Episode.S02E05"
```

Ignore case differences in filenames:

```bash
--ignore-case
```

Filter files by extensions:

```bash
--type mkv,mp4,avi
```

### Advanced Options

Adjust the maximum number of worker threads:

```bash
--max-workers 16
```

Set the logging level:

```bash
--log-level DEBUG
```

Force index rebuild by setting update interval to zero:

```bash
--update-interval 0
```

---

## Examples

**Example 1: Basic Sync**

```bash
python filesync.py \
  --local-dir "/home/user/videos" \
  --remote-dir "/remote/videos" \
  --hostname "remote.host.com" \
  --username "user" \
  --key-filename "/home/user/.ssh/id_rsa" \
  --index-dir "/home/user/.filesyncer"
```

**Example 2: Interactive Sync with File Matching**

```bash
python filesync.py \
  --local-dir "/home/user/videos" \
  --remote-dir "/remote/videos" \
  --hostname "remote.host.com" \
  --username "user" \
  --key-filename "/home/user/.ssh/id_rsa" \
  --index-dir "/home/user/.filesyncer" \
  --interactive \
  --match "Finale" \
  --ignore-case \
  --type mkv,mp4 \
  --log-level INFO
```

**Example 3: High-Performance Sync**

```bash
python filesync.py \
  --local-dir "/data/local" \
  --remote-dir "/data/remote" \
  --hostname "remote.host.com" \
  --username "user" \
  --key-filename "/home/user/.ssh/id_rsa" \
  --index-dir "/home/user/.filesyncer" \
  --max-workers 32 \
  --log-level WARNING
```

---

## Contributing

Contributions are welcome! Feel free to submit a pull request or open an issue.

1. Fork the repository.
2. Create your feature branch: `git checkout -b feature/awesome-feature`
3. Commit your changes: `git commit -am 'Add an awesome feature'`
4. Push to the branch: `git push origin feature/awesome-feature`
5. Open a pull request.

---

Give a ⭐ if this project helped you! Let's make file synchronization fun and easy together! 🎉

---

*Happy Syncing!*
