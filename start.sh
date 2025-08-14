#!/bin/bash

echo "🚀 Starting DataHub Metadata Manager..."
echo

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Please run setup.py first:"
    echo "   python setup.py"
    exit 1
fi

# Activate virtual environment and start application
source venv/bin/activate
echo "✅ Virtual environment activated"
echo

echo "🌐 Starting web server..."
echo "📍 Application will be available at: http://localhost:5000"
echo "🛑 Press Ctrl+C to stop the server"
echo

python run.py