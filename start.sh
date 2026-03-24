#!/bin/bash
# Market Breadth Engine - Quick Start Script
echo "🚀 Starting Market Breadth Engine..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.9+"
    exit 1
fi

# Install dependencies if needed
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "📦 Installing backend dependencies..."
    pip install -r backend/requirements.txt
fi

# Start backend in background
echo "⚡ Starting backend on http://localhost:8001..."
cd backend && python main.py &
BACKEND_PID=$!
cd ..

# Wait for backend to start
sleep 2

# Open frontend
echo "🌐 Opening dashboard..."
if command -v open &> /dev/null; then
    open frontend/index.html
elif command -v xdg-open &> /dev/null; then
    xdg-open frontend/index.html
else
    echo "👉 Open frontend/index.html in your browser"
fi

echo ""
echo "✅ Market Breadth Engine running!"
echo "   Backend: http://localhost:8001"
echo "   Frontend: frontend/index.html"
echo ""
echo "Press Ctrl+C to stop."

wait $BACKEND_PID
