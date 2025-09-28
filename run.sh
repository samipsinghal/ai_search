#!/usr/bin/env bash
# run.sh
# Convenience script to run the crawler with sensible defaults.
# Edits you should make:
#   - Update UA to include your name/email.
#   - Point --seeds to the file you want to use (seeds.txt or seeds_run2.txt).

set -euo pipefail

SEEDS_FILE="${1:-seeds.txt}"
LOG_PATH="${2:-logs/run1.tsv}"
UA='NYU-CS6913-HW1/1.0 (Your Name; your_email@nyu.edu)'

# Ensure the logs directory exists
mkdir -p "$(dirname "$LOG_PATH")"

python3 main.py \
  --seeds "$SEEDS_FILE" \
  --user-agent "$UA" \
  --threads 32 \
  --max-pages 5000 \
  --max-depth 10 \
  --timeout 5.0 \
  --log "$LOG_PATH"
