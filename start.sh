#!/bin/bash
set -e  # Exit on error

echo "=========================================="
echo "Running database migration..."
echo "=========================================="

# Run comprehensive migration (handles both fresh and existing databases)
python -m database.migrations.run_all_migrations

echo ""
echo "=========================================="
echo "Migration complete. Starting application..."
echo "=========================================="
echo ""

# Start Streamlit
exec streamlit run app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
