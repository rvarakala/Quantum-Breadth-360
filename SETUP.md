# Quantum Breadth 360 — Setup Guide

## First Time Setup (Run Once)

### Step 1 — Clone the repo
```bash
git clone https://github.com/rvarakala/Quantum-Breadth-360.git C:\QUANTUM_BREADTH_360
cd C:\QUANTUM_BREADTH_360
```

### Step 2 — Create virtual environment
```bash
cd C:\QUANTUM_BREADTH_360\backend
python -m venv venv
```

### Step 3 — Activate venv
```bash
# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate
```

### Step 4 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 5 — Start the backend
```bash
python main.py
```

### Step 6 — Open browser
```
http://localhost:8001
```

---

## Daily Use (After First Setup)

```bash
cd C:\QUANTUM_BREADTH_360\backend
venv\Scripts\activate
python main.py
```

Or just double-click: `START_BREADTH_ENGINE.bat`

---

## Getting Updates

```bash
cd C:\QUANTUM_BREADTH_360
git pull
# Then restart backend
```

---

## Troubleshooting

**Port already in use:**
```bash
# Kill whatever is on port 8001
netstat -ano | findstr :8001
taskkill /PID <PID> /F
```

**Missing packages after git pull:**
```bash
pip install -r requirements.txt
```

**DB not found — first run:**
The SQLite DB (`backend/breadth_data.db`) is auto-created on first start.
Go to Data Import tab → run "NSE Data Sync" to populate data.
