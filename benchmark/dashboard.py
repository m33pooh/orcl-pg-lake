"""
Live Web UI Dashboard for Oracle 23ai -> PostgreSQL Data Lake CDC Replication
Runs a lightweight local web server on http://localhost:8088
"""

import http.server
import json
import os
import socketserver
import sys
import threading
import time
from urllib.request import urlopen

import oracledb
import psycopg

PORT = int(os.getenv("DASHBOARD_PORT", "8088"))
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
RECEIVER_URL = os.getenv("RECEIVER_URL", "http://localhost:8000")
PG_DSN = os.getenv("PG_DSN", "postgresql://datalake_admin:DataLakePwd123!@localhost:5432/datalake_db")

ORACLE_CONFIGS = [
    {
        "id": "wallet",
        "app": "Wallet App",
        "oracle_dsn": os.getenv("WALLET_DSN", "localhost:11521/FREEPDB1"),
        "oracle_user": "WALLET_APP",
        "oracle_pwd": "WalletApp123!",
        "oracle_query": "SELECT count(*) FROM TRANSACTIONS",
        "pg_table": "wallet_cdc_wallet_app_transactions",
        "desc": "Financial Transactions & Balances",
    },
    {
        "id": "ecommerce",
        "app": "E-Commerce App",
        "oracle_dsn": os.getenv("ECOMMERCE_DSN", "localhost:1521/FREEPDB1"),
        "oracle_user": "ECOMMERCE_APP",
        "oracle_pwd": "EcomApp123!",
        "oracle_query": "SELECT count(*) FROM ORDERS",
        "pg_table": "ecommerce_cdc_ecommerce_app_orders",
        "desc": "Customer Orders & Payments",
    },
    {
        "id": "crm",
        "app": "CRM App",
        "oracle_dsn": os.getenv("CRM_DSN", "localhost:21521/FREEPDB1"),
        "oracle_user": "CRM_APP",
        "oracle_pwd": "CrmApp123!",
        "oracle_query": "SELECT count(*) FROM LEADS",
        "pg_table": "crm_cdc_crm_app_leads",
        "desc": "Sales Leads & Deals",
    },
    {
        "id": "audit",
        "app": "Audit App",
        "oracle_dsn": os.getenv("AUDIT_DSN", "localhost:31521/FREEPDB1"),
        "oracle_user": "AUDIT_APP",
        "oracle_pwd": "AuditApp123!",
        "oracle_query": "SELECT count(*) FROM SYSTEM_LOGS",
        "pg_table": "audit_cdc_audit_app_system_logs",
        "desc": "Subsystem Security & Action Logs",
    },
]

# Cached metrics updated by background worker
latest_metrics = {
    "timestamp": "",
    "total_oracle": 0,
    "total_pg": 0,
    "total_lag": 0,
    "overall_sync_pct": 100.0,
    "throughput": 0.0,
    "receiver_connected": False,
    "apps": [],
    "pipelines": [],
}


def fetch_receiver_status():
    try:
        req = urlopen(f"{RECEIVER_URL}/stats", timeout=2)
        if req.status == 200:
            return json.loads(req.read().decode())
    except Exception:
        pass
    return None


def fetch_pipeline_status(receiver_data):
    pipelines = [
        {
            "name": "debezium-server-wallet",
            "source": "FREEPDB1.WALLET_APP.TRANSACTIONS",
            "sink": "wallet_cdc_wallet_app_transactions",
            "mode": "Standalone Runtime",
            "state": "STREAMING",
            "key": "TRANSACTIONS",
        },
        {
            "name": "debezium-server-ecommerce",
            "source": "FREEPDB1.ECOMMERCE_APP.ORDERS",
            "sink": "ecommerce_cdc_ecommerce_app_orders",
            "mode": "Standalone Runtime",
            "state": "STREAMING",
            "key": "ORDERS",
        },
        {
            "name": "debezium-server-crm",
            "source": "FREEPDB1.CRM_APP.LEADS",
            "sink": "crm_cdc_crm_app_leads",
            "mode": "Standalone Runtime",
            "state": "STREAMING",
            "key": "LEADS",
        },
        {
            "name": "debezium-server-audit",
            "source": "FREEPDB1.AUDIT_APP.SYSTEM_LOGS",
            "sink": "audit_cdc_audit_app_system_logs",
            "mode": "Standalone Runtime",
            "state": "STREAMING",
            "key": "SYSTEM_LOGS",
        },
    ]
    by_table = receiver_data.get("by_table", {}) if receiver_data else {}
    for p in pipelines:
        tbl_stat = by_table.get(p["key"], {})
        p["received"] = tbl_stat.get("received", 0)
        p["upserted"] = tbl_stat.get("upserted", 0)
    return pipelines


def metrics_collector_thread():
    while True:
        try:
            table_names = [c["pg_table"] for c in ORACLE_CONFIGS]
            pg_counts = {}
            try:
                with psycopg.connect(PG_DSN, autocommit=True, connect_timeout=3) as pg_conn:
                    with pg_conn.cursor() as cur:
                        for tbl in table_names:
                            try:
                                cur.execute(f"SELECT count(*) FROM {tbl}")
                                pg_counts[tbl] = cur.fetchone()[0]
                            except Exception:
                                pg_conn.rollback()
                                pg_counts[tbl] = 0
            except Exception:
                pass

            apps_data = []
            total_oracle = 0
            total_pg = 0

            for cfg in ORACLE_CONFIGS:
                o_count = 0
                try:
                    with oracledb.connect(user=cfg["oracle_user"], password=cfg["oracle_pwd"], dsn=cfg["oracle_dsn"]) as o_conn:
                        with o_conn.cursor() as o_cur:
                            o_cur.execute(cfg["oracle_query"])
                            o_count = o_cur.fetchone()[0]
                except Exception:
                    o_count = 0

                p_count = pg_counts.get(cfg["pg_table"], 0)
                lag = max(0, o_count - p_count)
                pct = (p_count / o_count * 100) if o_count > 0 else 100.0

                total_oracle += o_count
                total_pg += p_count

                apps_data.append({
                    "id": cfg["id"],
                    "app": cfg["app"],
                    "desc": cfg["desc"],
                    "table": cfg["pg_table"],
                    "oracle_count": o_count,
                    "pg_count": p_count,
                    "lag": lag,
                    "sync_pct": round(pct, 2),
                })

            total_lag = max(0, total_oracle - total_pg)
            overall_pct = (total_pg / total_oracle * 100) if total_oracle > 0 else 100.0

            receiver_data = fetch_receiver_status()
            pipelines = fetch_pipeline_status(receiver_data)
            throughput = receiver_data.get("throughput_req_per_sec", 0.0) if receiver_data else 0.0

            latest_metrics.update({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_oracle": total_oracle,
                "total_pg": total_pg,
                "total_lag": total_lag,
                "overall_sync_pct": round(overall_pct, 2),
                "throughput": throughput,
                "receiver_connected": receiver_data is not None,
                "apps": apps_data,
                "pipelines": pipelines,
            })
        except Exception as e:
            print(f"Error in metrics collector: {e}", file=sys.stderr)

        time.sleep(2)


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Data Lake CDC Live Monitor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: rgba(22, 29, 49, 0.7);
      --card-border: rgba(255, 255, 255, 0.08);
      --text: #f3f4f6;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --purple: #a855f7;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: 'Inter', sans-serif;
      background: radial-gradient(circle at 50% 0%, #1e1b4b 0%, #0b0f19 70%);
      color: var(--text);
      min-height: 100vh;
      padding: 24px;
    }

    .container {
      max-width: 1300px;
      margin: 0 auto;
    }

    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 28px;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--card-border);
    }

    .logo-area h1 {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: -0.5px;
      background: linear-gradient(135deg, #38bdf8, #818cf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .logo-area p {
      color: var(--text-muted);
      font-size: 13px;
      margin-top: 4px;
    }

    .header-links {
      display: flex;
      gap: 12px;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      text-decoration: none;
      transition: all 0.2s ease;
      cursor: pointer;
      border: 1px solid var(--card-border);
    }

    .btn-kafka {
      background: rgba(56, 189, 248, 0.1);
      color: #38bdf8;
      border-color: rgba(56, 189, 248, 0.3);
    }
    .btn-kafka:hover {
      background: rgba(56, 189, 248, 0.2);
    }

    .btn-connect {
      background: rgba(168, 85, 247, 0.1);
      color: #c084fc;
      border-color: rgba(168, 85, 247, 0.3);
    }
    .btn-connect:hover {
      background: rgba(168, 85, 247, 0.2);
    }

    /* Summary KPI Grid */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 18px;
      margin-bottom: 28px;
    }

    .kpi-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 20px;
      backdrop-filter: blur(12px);
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    }

    .kpi-label {
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-muted);
      margin-bottom: 8px;
    }

    .kpi-value {
      font-size: 32px;
      font-weight: 700;
      font-family: 'JetBrains Mono', monospace;
    }

    .kpi-sub {
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 6px;
    }

    /* App Cards Grid */
    .section-title {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 16px;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .app-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 20px;
      margin-bottom: 32px;
    }

    .app-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 22px;
      backdrop-filter: blur(12px);
      transition: transform 0.2s, border-color 0.2s;
    }
    .app-card:hover {
      border-color: rgba(56, 189, 248, 0.4);
      transform: translateY(-2px);
    }

    .app-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }

    .app-title {
      font-size: 16px;
      font-weight: 600;
    }

    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.3px;
    }
    .badge-sync {
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .badge-lag {
      background: rgba(245, 158, 11, 0.15);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.3);
    }

    .app-stats {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-bottom: 16px;
    }

    .stat-box {
      background: rgba(0, 0, 0, 0.25);
      padding: 10px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.04);
    }

    .stat-label {
      font-size: 11px;
      color: var(--text-muted);
      margin-bottom: 4px;
    }

    .stat-val {
      font-size: 18px;
      font-weight: 600;
      font-family: 'JetBrains Mono', monospace;
    }

    .progress-bar-bg {
      width: 100%;
      height: 8px;
      background: rgba(255, 255, 255, 0.08);
      border-radius: 4px;
      overflow: hidden;
      margin-bottom: 6px;
    }

    .progress-bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #38bdf8, #10b981);
      transition: width 0.4s ease;
    }

    .progress-desc {
      display: flex;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-muted);
    }

    /* Connectors Panel */
    .connectors-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 20px;
    }

    .connector-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    .connector-table th {
      text-align: left;
      padding: 12px;
      color: var(--text-muted);
      border-bottom: 1px solid var(--card-border);
      font-weight: 600;
      font-size: 12px;
      text-transform: uppercase;
    }

    .connector-table td {
      padding: 12px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      font-family: 'JetBrains Mono', monospace;
    }

    .badge-running {
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-area">
        <h1>Oracle 23ai to PostgreSQL Data Lake</h1>
        <p>Debezium Server Standalone (Zero-Kafka High-Throughput CDC Engine)</p>
      </div>
      <div class="header-links">
        <a href="http://localhost:8000/stats" target="_blank" class="btn btn-connect" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3);">
          Lake Receiver API (Port 8000)
        </a>
        <a href="http://localhost:8000/health" target="_blank" class="btn btn-kafka" style="background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3);">
          System Health (Port 8000)
        </a>
      </div>
    </header>

    <!-- Overall KPIs -->
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Oracle Sources Total</div>
        <div class="kpi-value" id="kpi-oracle" style="color: #38bdf8;">--</div>
        <div class="kpi-sub">Total records across 4 OLTPs</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">PostgreSQL Lake Total</div>
        <div class="kpi-value" id="kpi-pg" style="color: #10b981;">--</div>
        <div class="kpi-sub">Total records stored in Lake</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Replication Lag</div>
        <div class="kpi-value" id="kpi-lag" style="color: #fbbf24;">--</div>
        <div class="kpi-sub">Pending stream records</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Ingestion Throughput</div>
        <div class="kpi-value" id="kpi-throughput" style="color: #38bdf8;">--</div>
        <div class="kpi-sub">Lake bulk upsert req/sec</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Sync Health</div>
        <div class="kpi-value" id="kpi-sync" style="color: #a855f7;">--%</div>
        <div class="kpi-sub" id="last-updated">Updated: --</div>
      </div>
    </div>

    <!-- Application Databases Status -->
    <div class="section-title">Application Domains (Oracle 23ai Sources)</div>
    <div class="app-grid" id="apps-container"></div>

    <!-- Standalone Debezium Server Pipelines -->
    <div class="section-title">Standalone Debezium Server Pipelines (Zero-Kafka Engine)</div>
    <div class="connectors-card">
      <table class="connector-table">
        <thead>
          <tr>
            <th>Instance Name</th>
            <th>Source Table</th>
            <th>Lake Target Table</th>
            <th>Runtime Engine</th>
            <th>Upserted Records</th>
            <th>State</th>
          </tr>
        </thead>
        <tbody id="pipelines-tbody"></tbody>
      </table>
    </div>
  </div>

  <script>
    async function updateDashboard() {
      try {
        const res = await fetch('/api/metrics');
        const data = await res.json();

        // Update KPIs
        document.getElementById('kpi-oracle').innerText = data.total_oracle.toLocaleString();
        document.getElementById('kpi-pg').innerText = data.total_pg.toLocaleString();
        document.getElementById('kpi-lag').innerText = data.total_lag.toLocaleString();
        document.getElementById('kpi-throughput').innerText = (data.throughput ? data.throughput.toLocaleString() : '0') + ' req/s';
        document.getElementById('kpi-sync').innerText = data.overall_sync_pct + '%';
        document.getElementById('last-updated').innerText = 'Updated: ' + data.timestamp;

        // Update Apps
        const appsContainer = document.getElementById('apps-container');
        appsContainer.innerHTML = data.apps.map(app => {
          const isSync = app.lag === 0;
          const badgeClass = isSync ? 'badge-sync' : 'badge-lag';
          const badgeText = isSync ? 'IN SYNC' : `LAG: ${app.lag.toLocaleString()}`;

          return `
            <div class="app-card">
              <div class="app-header">
                <div>
                  <div class="app-title">${app.app}</div>
                  <div style="font-size: 11px; color: var(--text-muted);">${app.desc}</div>
                </div>
                <span class="badge ${badgeClass}">${badgeText}</span>
              </div>
              <div class="app-stats">
                <div class="stat-box">
                  <div class="stat-label">Oracle Source</div>
                  <div class="stat-val" style="color: #38bdf8;">${app.oracle_count.toLocaleString()}</div>
                </div>
                <div class="stat-box">
                  <div class="stat-label">Postgres Lake</div>
                  <div class="stat-val" style="color: #10b981;">${app.pg_count.toLocaleString()}</div>
                </div>
              </div>
              <div class="progress-bar-bg">
                <div class="progress-bar-fill" style="width: ${app.sync_pct}%;"></div>
              </div>
              <div class="progress-desc">
                <span>Table: ${app.table}</span>
                <span>${app.sync_pct}%</span>
              </div>
            </div>
          `;
        }).join('');

        // Update Pipelines
        const pipeTbody = document.getElementById('pipelines-tbody');
        if (data.pipelines && data.pipelines.length > 0) {
          pipeTbody.innerHTML = data.pipelines.map(p => `
            <tr>
              <td style="color: #f3f4f6; font-weight: 500;">${p.name}</td>
              <td style="color: #94a3b8;">${p.source}</td>
              <td style="color: #38bdf8;">${p.sink}</td>
              <td><span style="color: #a855f7; text-transform: uppercase;">${p.mode}</span></td>
              <td style="color: #10b981; font-weight: 600;">${(p.upserted || 0).toLocaleString()}</td>
              <td><span class="badge-running">${p.state}</span></td>
            </tr>
          `).join('');
        }

      } catch (err) {
        console.error('Failed to update metrics:', err);
      }
    }

    updateDashboard();
    setInterval(updateDashboard, 2000);
  </script>
</body>
</html>
"""


class DashboardHTTPHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif self.path == "/api/metrics":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(latest_metrics).encode("utf-8"))
        else:
            self.send_error(404, "Not Found")

    def log_message(self, format, *args):
        # Suppress routine request logs to keep terminal clean
        return


def run_server():
    # Start background metric collector
    collector = threading.Thread(target=metrics_collector_thread, daemon=True)
    collector.start()

    with socketserver.ThreadingTCPServer(("", PORT), DashboardHTTPHandler) as httpd:
        print(f"Data Lake CDC Live Web Dashboard running at: http://localhost:{PORT}")
        print("Press Ctrl+C to terminate.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nDashboard server terminated.")


if __name__ == "__main__":
    run_server()
