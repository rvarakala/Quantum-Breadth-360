"""
Breadth Engine DB Tool — C:\breadth-app\db_tool.py
Usage: python db_tool.py <command> [args]
Commands: import-nifty, import-sectors, import-csv, status
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'status'

    from data_store import init_db
    init_db()

    if cmd == 'import-nifty':
        from data_store import import_nifty500_csv
        path = sys.argv[2] if len(sys.argv) > 2 else r'backend\data\nifty500_clean.csv'
        n = import_nifty500_csv(path)
        print(f"✅ Imported {n} NIFTY 500 tickers into SQLite")

    elif cmd == 'import-sectors':
        from data_store import import_sectors_csv
        path = sys.argv[2] if len(sys.argv) > 2 else 'sectors.csv'
        n = import_sectors_csv(path)
        print(f"✅ Imported {n} sector mappings into SQLite")

    elif cmd == 'import-csv':
        path = sys.argv[2] if len(sys.argv) > 2 else None
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))
        import import_local
        if path:
            import_local.import_files([path])
        else:
            import_local.main()

    elif cmd == 'status':
        from data_store import db_stats, load_ticker_universe, load_sector_counts
        s = db_stats()
        u = load_ticker_universe('India')
        try:
            sc = load_sector_counts()
            sector_count = len(sc)
        except:
            sector_count = 0
        print(f"  DB rows:    {s.get('total_rows', 0):,}")
        print(f"  Tickers:    {s.get('total_tickers', 0)}")
        print(f"  Date range: {s.get('oldest_date','?')} → {s.get('newest_date','?')}")
        print(f"  Universe:   {len(u)} tickers (NIFTY 500 list)")
        print(f"  Sectors:    {sector_count}")

    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python db_tool.py [import-nifty|import-sectors|import-csv|status]")

if __name__ == '__main__':
    main()
