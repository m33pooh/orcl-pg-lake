# Multi-App High-Performance Load Generator & Benchmark

ชุดเครื่องมือสร้างปริมาณข้อมูลและทดสอบโหลด (Load Generator & Benchmark) สำหรับยิงธุรกรรมจำลองระดับสูง (เช่น 1,000,000 Requests ต่อแอปพลิเคชัน หรือ 4,000,000 Requests รวมทุกแอป) เข้าสู่ Oracle 23ai Free ทั้ง 4 Instance พร้อมระบบตรวจสอบการ Replicate ข้อมูลแบบ Real-time ไปยัง PostgreSQL Data Lake

---

## 1. แอปพลิเคชันและตารางเป้าหมาย

| Application | Target Table | Primary Key | ฟิลด์ข้อมูลจำลอง | พอร์ต Localhost |
|---|---|---|---|---|
| **Wallet** | `TRANSACTIONS` | `TX_ID` | `WALLET_ID`, `AMOUNT`, `TX_TYPE`, `CREATED_AT` | `11521` |
| **Ecommerce** | `ORDERS` | `ORDER_ID` | `CUSTOMER_ID`, `TOTAL_AMOUNT`, `STATUS`, `ORDER_DATE` | `1521` |
| **CRM** | `LEADS` | `LEAD_ID` | `CONTACT_NAME`, `COMPANY`, `STATUS`, `ESTIMATED_VALUE` | `21521` |
| **Audit** | `SYSTEM_LOGS` | `LOG_ID` | `SUBSYSTEM`, `ACTION`, `PERFORMED_BY`, `SEVERITY`, `LOGGED_AT` | `31521` |

---

## 2. วิธีใช้งานผ่าน Python Load Generator (แนะนำ)

เครื่องมือ `benchmark/load_generator.py` พัฒนาด้วย Python รองรับการยิงข้อมูลแบบ Multi-Process และ Array Binding Batching (`cursor.executemany`) ทำให้ได้ Throughput สูงกว่า 20,000 - 30,000 req/s

ไม่ต้องติดตั้ง Python dependencies ลงเครื่องโดยตรง สามารถสั่งรันผ่าน `uv` ได้ทันที:

### 2.1 ทดสอบยิง 1,000,000 Requests พร้อมกันทุกแอป (Total 4,000,000 Requests)
```bash
uv run --with oracledb python benchmark/load_generator.py --app all --count 1000000 --batch-size 10000
```

### 2.2 ทดสอบยิงเฉพาะแอปพลิเคชันที่ต้องการ
```bash
# Wallet App (1,000,000 Transactions)
uv run --with oracledb python benchmark/load_generator.py --app wallet --count 1000000 --batch-size 10000

# E-Commerce App (1,000,000 Orders)
uv run --with oracledb python benchmark/load_generator.py --app ecommerce --count 1000000 --batch-size 10000

# CRM App (1,000,000 Leads)
uv run --with oracledb python benchmark/load_generator.py --app crm --count 1000000 --batch-size 10000

# Audit App (1,000,000 System Logs)
uv run --with oracledb python benchmark/load_generator.py --app audit --count 1000000 --batch-size 10000
```

### 2.3 พารามิเตอร์ที่ปรับแต่งได้
- `--app`: เลือกระบุ `wallet`, `ecommerce`, `crm`, `audit`, หรือ `all` (default: `all`)
- `--count`: จำนวนแถว/รีเควสต์ต่อแอปพลิเคชัน (default: `1000000`)
- `--batch-size`: ขนาดของ Array Binding และรอบ Commit (แนะนำ `5000` - `10000` เพื่อประสิทธิภาพสูงสุด)

---

## 3. วิธีใช้งานผ่าน In-Database PL/SQL Scripts (ความเร็วสูงสุด)

หากต้องการรันคำสั่งโดยตรงภายใน Oracle Container โดยไม่มี Network Overhead สามารถใช้สคริปต์ PL/SQL ในโฟลเดอร์ `benchmark/sql/`:

```bash
# 1. Wallet: ยิง 1,000,000 transactions (batch size 10,000)
docker compose cp benchmark/sql/generate_wallet_data.sql oracle-wallet:/tmp/
docker compose exec oracle-wallet bash -c "export ORACLE_SID=FREE; sqlplus -s / as sysdba @/tmp/generate_wallet_data.sql 1000000 10000"

# 2. Ecommerce: ยิง 1,000,000 orders
docker compose cp benchmark/sql/generate_ecommerce_data.sql oracle-ecommerce:/tmp/
docker compose exec oracle-ecommerce bash -c "export ORACLE_SID=FREE; sqlplus -s / as sysdba @/tmp/generate_ecommerce_data.sql 1000000 10000"

# 3. CRM: ยิง 1,000,000 leads
docker compose cp benchmark/sql/generate_crm_data.sql oracle-crm:/tmp/
docker compose exec oracle-crm bash -c "export ORACLE_SID=FREE; sqlplus -s / as sysdba @/tmp/generate_crm_data.sql 1000000 10000"

# 4. Audit: ยิง 1,000,000 audit logs
docker compose cp benchmark/sql/generate_audit_data.sql oracle-audit:/tmp/
docker compose exec oracle-audit bash -c "export ORACLE_SID=FREE; sqlplus -s / as sysdba @/tmp/generate_audit_data.sql 1000000 10000"
```

---

## 4. UI Monitor สำหรับตรวจสอบการทำงาน

ระบบมี Web UI Monitor ให้เลือกใช้งาน 2 ช่องทาง:

### 4.1 Real-Time Replication Web Dashboard (Port 8088)
เปิดดูภาพรวมการซิงค์ข้อมูลระหว่าง Oracle ทั้ง 4 ตัว และ PostgreSQL Data Lake แบบเรียลไทม์ พร้อมกราฟแสดง % การซิงค์และสถานะของ Connectors:
```bash
uv run --with oracledb --with "psycopg[binary]" python benchmark/dashboard.py
```
เปิดเบราว์เซอร์ไปที่: **http://localhost:8088**

### 4.2 Kafka & Debezium Web UI (Port 8080)
ใช้ดู Topic สตรีมมิ่ง, ข้อความ JSON Payload, Partition Offsets, และสถานะ Debezium Connector:
เปิดเบราว์เซอร์ไปที่: **http://localhost:8080**

---

## 5. ตรวจสอบ Real-time Replication Lag ผ่าน Terminal (CLI)

ตัวมอนิเตอร์จะแสดงตารางเปรียบเทียบจำนวนแถวและ Replication Lag แบบเรียลไทม์ทุก 3 วินาที:

```
--- Replication Status [2026-10-20 01:21:34] ---
Application  | Oracle (Source) | Postgres (Lake) | Synced %   | Lag (Rows)  
----------------------------------------------------------------------------
Wallet       |       1,000,004 |       1,000,004 |    100.00% |            0
Ecommerce    |       1,000,002 |       1,000,002 |    100.00% |            0
CRM          |       1,000,002 |       1,000,002 |    100.00% |            0
Audit        |       1,000,002 |       1,000,002 |    100.00% |            0

[OK] All data lake tables are 100% in sync with Oracle!
```

หรือตรวจสอบสถานะครั้งเดียวแล้วออกทันที:
```bash
uv run --with oracledb --with "psycopg[binary]" python benchmark/monitor_replication.py --once
```

---

## 5. ข้อแนะนำสำหรับ Oracle 23ai Free เมื่อรันระดับ 1 ล้านแถว

1. **ขนาดพื้นที่จัดเก็บ (Storage Quota)**:
   - Oracle 23ai Free จำกัดพื้นที่ข้อมูลตารางผู้ใช้ที่ 12 GB ต่อฐานข้อมูล
   - ข้อมูล 1 ล้านแถวในโครงสร้างตารางข้างต้นจะใช้พื้นที่ประมาณ 120 - 250 MB ต่อแอปพลิเคชัน จึงปลอดภัยจากข้อจำกัด 12 GB
2. **ขนาด Redo Log**:
   - การ Commit ในขนาด Batch ละ 5,000 - 10,000 แถว ช่วยป้องกันปัญหา Redo Log Switch บ่อยเกินไป และทำให้ Debezium LogMiner สตรีมข้อมูลได้อย่างต่อเนื่อง
3. **RAM และ CPU**:
   - Oracle Free จำกัดที่ 2 CPU Cores และ 2 GB RAM ต่อ Instance การรันด้วย Array Binding จะช่วยลด Context Switching ของ CPU ได้อย่างมาก
