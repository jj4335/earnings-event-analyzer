#!/bin/bash

PROJECT_DIR="/Users/jonghyun/Downloads/earnings-event-analyzer"

echo "Starting Earnings Event Analyzer..."

# API 서버 (백그라운드)
source "$PROJECT_DIR/.venv/bin/activate"
cd "$PROJECT_DIR"
export PYTHONPATH=.
uvicorn backend.api_server:app --port 8080 &
API_PID=$!
echo "API server started (PID: $API_PID)"

# 잠깐 대기
sleep 2

# React 대시보드
cd "$PROJECT_DIR/frontend/dashboard"
npm start &
REACT_PID=$!
echo "React dashboard started (PID: $REACT_PID)"

echo ""
echo "Dashboard: http://localhost:3000"
echo "API:       http://localhost:8080"
echo ""
echo "Press Ctrl+C to stop both servers"

# 두 프로세스 종료 대기
trap "kill $API_PID $REACT_PID 2>/dev/null; echo 'Stopped.'" EXIT
wait
