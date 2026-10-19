# Data Lake Architecture (Oracle 23ai Free + PostgreSQL + n8n)

โปรเจกต์นี้เป็นการจำลองสถาปัตยกรรมสำหรับระบบที่ใช้ **Oracle 23ai Free** เป็นฐานข้อมูลหลักสำหรับระบบที่ต้องการความเร็ว (OLTP) และใช้ **PostgreSQL** เป็น Data Lake (OLAP) สำหรับเก็บข้อมูลประวัติศาสตร์ขนาดใหญ่ โดยมี **n8n** ทำหน้าที่เป็นระบบอัตโนมัติ (ETL) ในการดูดข้อมูลข้ามฐานข้อมูลตามรอบเวลา (Batch Scheduling)

---

## ตัวอย่าง 4 แอปพลิเคชันที่ใช้งานสถาปัตยกรรมนี้

เพื่อให้เห็นภาพการจัดการข้อมูลภายใต้ข้อจำกัดพื้นที่ 12 GB ของ Oracle นี่คือตัวอย่าง 4 ระบบที่เชื่อมต่อเข้ากับสถาปัตยกรรมนี้:

### 1. ระบบร้านค้าออนไลน์ (E-Commerce Web App)
- **ข้อมูลที่เกิด:** ข้อมูลลูกค้า, ตะกร้าสินค้า, คำสั่งซื้อ (Orders), สถานะจัดส่ง
- **Oracle (Hot Data):** เก็บข้อมูลโปรไฟล์ลูกค้าทั้งหมด และคำสั่งซื้อที่กำลังดำเนินการอยู่หรือสั่งซื้อภายใน 1 ปีล่าสุด
- **PostgreSQL (Cold Data):** เก็บประวัติคำสั่งซื้อที่จัดส่งสำเร็จแล้วและมีอายุเกิน 1 ปี เพื่อนำไปทำ Data Analytics โดยไม่ต้องเปลืองพื้นที่ Oracle

### 2. ระบบกระเป๋าเงินดิจิทัล (Payment & Wallet API)
- **ข้อมูลที่เกิด:** ประวัติการเติมเงิน, โอนเงิน, ตัดบัตรเครดิต (Transaction Logs)
- **Oracle (Hot Data):** เก็บข้อมูลยอดเงินคงเหลือ (Balance) และประวัติการทำธุรกรรมย้อนหลัง 3-6 เดือน
- **PostgreSQL (Cold Data):** ดูดข้อมูล Transaction รายวันไปเก็บสะสมไว้ เพื่อใช้ในการตรวจสอบบัญชี (Reconciliation) สิ้นเดือน หรือส่ง Report ให้ทีมบัญชีผ่าน BI Tools

### 3. ระบบดูแลลูกค้า (CRM & Helpdesk Dashboard)
- **ข้อมูลที่เกิด:** ประวัติการแชท, การเปิด Ticket แจ้งปัญหา, ความพึงพอใจลูกค้า
- **Oracle (Hot Data):** เก็บสถานะ Ticket ที่กำลังเปิดอยู่ (Open/Pending) ข้อมูล Master Data ของลูกค้า
- **PostgreSQL (Cold Data):** เมื่อ Ticket ถูกปิด (Resolved) เกิน 30 วัน n8n จะดูดข้อมูลข้อความแชทและรายละเอียดปัญหาไปเก็บที่ Data Lake เพื่อนำไปเทรน AI หรือวิเคราะห์ปัญหาที่เกิดซ้ำ

### 4. ระบบเก็บประวัติความปลอดภัย (Audit & Security Logging)
- **ข้อมูลที่เกิด:** ประวัติการ Login, IP Address, API Request Payload (App Logs)
- **Oracle (Hot Data):** เก็บ Log การใช้งานระบบเฉพาะ 7 ถึง 30 วันล่าสุดเท่านั้น
- **PostgreSQL (Cold Data):** n8n ทำการกวาด Log ของเมื่อวาน (Daily Batch) ไปใส่ใน PostgreSQL ทุกคืน เพื่อปฏิบัติตาม พ.ร.บ. คอมพิวเตอร์ และสั่ง Delete Log เก่าออกจาก Oracle เพื่อคืนพื้นที่

---

## ความต้องการของระบบ (System Requirements)

- RAM: แนะนำอย่างน้อย 16 GB (Oracle 4 ตัว + PostgreSQL + n8n)
- Disk Space: อย่างน้อย 25 GB ว่าง
- Docker & Docker Compose v2 ขึ้นไป

---

## วิธีการติดตั้งและรันระบบ

1. คัดลอกและตั้งค่า `.env` (หากยังไม่มี):
   ```bash
   cp .env.example .env
   ```
2. รันคำสั่ง Start services:
   ```bash
   docker compose up -d
   ```
3. รอจนกระทั่ง Database ทั้งหมดพร้อมทำงาน (ตรวจสอบด้วย `docker compose ps` จนขึ้น healthy ทั้งหมด)
4. เปิดเบราว์เซอร์เข้าสู่ n8n Web Console:
   ```
   http://localhost:5678
   ```

---

## โครงสร้างการเชื่อมต่อ Database ใน n8n

เมื่อเปิด n8n ให้สร้าง Credential สำหรับเชื่อมต่อไปยังแต่ละฐานข้อมูลผ่าน Network ภายใน Docker:

| Service | Hostname | Port | Database / Service Name | User / Schema |
|---|---|---|---|---|
| E-Commerce DB | `oracle-ecommerce` | 1521 | `FREEPDB1` | sys as sysdba / app user |
| Wallet DB | `oracle-wallet` | 1521 | `FREEPDB1` | sys as sysdba / app user |
| CRM DB | `oracle-crm` | 1521 | `FREEPDB1` | sys as sysdba / app user |
| Audit DB | `oracle-audit` | 1521 | `FREEPDB1` | sys as sysdba / app user |
| Postgres Data Lake | `postgres-datalake` | 5432 | `datalake_db` | `datalake_admin` |

*(หมายเหตุ: พอร์ต 11521, 21521, 31521 เป็นพอร์ตสำหรับเชื่อมต่อจากภายนอก Host เครื่องคุณ แต่การเชื่อมต่อระหว่าง Container ภายใน Docker Network ให้ใช้ Port 1521 เสมอ)*

---

## ขั้นตอนการสร้าง Workflow ETL ตัวอย่างใน n8n

1. **Trigger Node:** เพิ่ม `Schedule Trigger` กำหนดเวลาทำงาน เช่น ทุกเที่ยงคืน (`0 0 * * *`)
2. **Read Hot Data Node:** ใช้ Oracle หรือ Execute Command Node ดึง Record เก่าที่ต้องการ Archive:
   ```sql
   SELECT * FROM wallet_transactions WHERE created_at < SYSDATE - 90;
   ```
3. **Write Cold Data Node:** ใช้ PostgreSQL Node แทรกข้อมูลลงในตาราง Data Lake:
   - Target Table: `datalake.wallet_transactions_archive`
   - Operation: Insert
4. **Purge Hot Data Node:** สั่งลบข้อมูลออกจาก Oracle เพื่อคืนโควตา 12 GB:
   ```sql
   DELETE FROM wallet_transactions WHERE created_at < SYSDATE - 90;
   COMMIT;
   ```