#!/bin/bash
set -e  # Exit on error

echo "=========================================="
echo "Running database migration..."
echo "=========================================="

# Run migration
python migrate_v002.py

echo ""
echo "=========================================="
echo "Migration complete. Starting application..."
echo "=========================================="
echo ""

# Start Streamlit
exec streamlit run app.py --server.port $PORT --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
