"""
Real-time Replication & Lag Monitor between Oracle 23ai Sources and PostgreSQL Data Lake.
Continuously queries Oracle source tables and PostgreSQL Data Lake sink tables.
"""

import argparse
import os
import sys
import time
from datetime import datetime

import oracledb
import psycopg

ORACLE_CONFIGS = [
    {
        "app": "Wallet",
        "oracle_dsn": os.getenv("WALLET_DSN", "localhost:11521/FREEPDB1"),
        "oracle_user": "WALLET_APP",
        "oracle_pwd": "WalletApp123!",
        "oracle_query": "SELECT count(*) FROM TRANSACTIONS",
        "pg_table": "wallet_cdc_wallet_app_transactions",
    },
    {
        "app": "Ecommerce",
        "oracle_dsn": os.getenv("ECOMMERCE_DSN", "localhost:1521/FREEPDB1"),
        "oracle_user": "ECOMMERCE_APP",
        "oracle_pwd": "EcomApp123!",
        "oracle_query": "SELECT count(*) FROM ORDERS",
        "pg_table": "ecommerce_cdc_ecommerce_app_orders",
    },
    {
        "app": "CRM",
        "oracle_dsn": os.getenv("CRM_DSN", "localhost:21521/FREEPDB1"),
        "oracle_user": "CRM_APP",
        "oracle_pwd": "CrmApp123!",
        "oracle_query": "SELECT count(*) FROM LEADS",
        "pg_table": "crm_cdc_crm_app_leads",
    },
    {
        "app": "Audit",
        "oracle_dsn": os.getenv("AUDIT_DSN", "localhost:31521/FREEPDB1"),
        "oracle_user": "AUDIT_APP",
        "oracle_pwd": "AuditApp123!",
        "oracle_query": "SELECT count(*) FROM SYSTEM_LOGS",
        "pg_table": "audit_cdc_audit_app_system_logs",
    },
]

PG_DSN = os.getenv("PG_DSN", "postgresql://datalake_admin:DataLakePwd123!@localhost:5432/datalake_db")


def get_oracle_count(cfg):
    try:
        with oracledb.connect(user=cfg["oracle_user"], password=cfg["oracle_pwd"], dsn=cfg["oracle_dsn"]) as conn:
            with conn.cursor() as cur:
                cur.execute(cfg["oracle_query"])
                return cur.fetchone()[0]
    except Exception as e:
        return f"ERR: {e}"


def get_pg_counts(pg_conn, table_names):
    counts = {}
    with pg_conn.cursor() as cur:
        for tbl in table_names:
            try:
                cur.execute(f"SELECT count(*) FROM {tbl}")
                counts[tbl] = cur.fetchone()[0]
            except Exception:
                pg_conn.rollback()
                counts[tbl] = 0
    return counts


def main():
    parser = argparse.ArgumentParser(description="Replication Monitor")
    parser.add_argument("--once", action="store_true", help="Print replication status once and exit")
    parser.add_argument("--interval", type=int, default=3, help="Refresh interval in seconds (default: 3)")
    args = parser.parse_args()

    print("Connecting to PostgreSQL Data Lake...")
    try:
        pg_conn = psycopg.connect(PG_DSN, autocommit=True)
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

    table_names = [cfg["pg_table"] for cfg in ORACLE_CONFIGS]

    try:
        while True:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pg_counts = get_pg_counts(pg_conn, table_names)
            
            print(f"\n--- Replication Status [{timestamp}] ---")
            header = f"{'Application':<12} | {'Oracle (Source)':<15} | {'Postgres (Lake)':<15} | {'Synced %':<10} | {'Lag (Rows)':<12}"
            print(header)
            print("-" * len(header))
            
            all_synced = True
            for cfg in ORACLE_CONFIGS:
                app = cfg["app"]
                o_count = get_oracle_count(cfg)
                p_count = pg_counts.get(cfg["pg_table"], 0)
                
                if isinstance(o_count, int):
                    lag = max(0, o_count - p_count)
                    pct = (p_count / o_count * 100) if o_count > 0 else 100.0
                    synced_str = f"{pct:6.2f}%"
                    lag_str = f"{lag:,}"
                    if lag > 0:
                        all_synced = False
                    print(f"{app:<12} | {o_count:>15,} | {p_count:>15,} | {synced_str:>10} | {lag_str:>12}")
                else:
                    print(f"{app:<12} | {str(o_count):>15} | {p_count:>15,} | {'N/A':>10} | {'N/A':>12}")

            if all_synced:
                print("\n[OK] All data lake tables are 100% in sync with Oracle!")
            
            if args.once:
                break
            time.sleep(args.interval)


    except KeyboardInterrupt:
        print("\nMonitor stopped.")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
