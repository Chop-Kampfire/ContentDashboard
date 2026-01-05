#!/bin/bash
set -e  # Exit on error

echo "=========================================="
echo "Running database migration (worker)..."
echo "=========================================="

# Run the comprehensive migration from database/migrations
python -m database.migrations.v002_multiplatform

echo ""
echo "=========================================="
echo "Migration complete. Starting worker..."
echo "=========================================="
echo ""

# Start worker
exec python worker.py
