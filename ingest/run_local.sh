#!/usr/bin/env sh
set -eu

if [ -f ../.env ]; then
  # Load environment variables from project root .env for local execution.
  # shellcheck disable=SC1091
  . ../.env
fi

python main.py
