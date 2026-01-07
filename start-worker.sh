#!/bin/bash
set -e  # Exit on error

echo "=========================================="
echo "Running database migration (worker)..."
echo "=========================================="

# Run comprehensive migration (handles both fresh and existing databases)
python -m database.migrations.run_all_migrations

echo ""
echo "=========================================="
echo "Migration complete. Starting worker..."
echo "=========================================="
echo ""

# Start worker
exec python worker.py
