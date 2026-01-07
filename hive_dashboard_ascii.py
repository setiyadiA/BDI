import subprocess
import sys

DATABASE = "log_db"   # GANTI JIKA BUKAN default
TABLE = "datalog_akses_raw"

query = """
SELECT col9 AS status, COUNT(*) AS total
FROM {db}.{table}
GROUP BY col9
ORDER BY status
""".format(db=DATABASE, table=TABLE)

cmd = [
    "beeline",
    "-u", "jdbc:hive2://localhost:10000/" + DATABASE,
    "--outputformat=csv2",
    "-e", query
]

p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = p.communicate()

if err and "Error" in err:
    print("HIVE ERROR:")
    print(err)
    sys.exit(1)

lines = out.strip().split("\n")

if len(lines) <= 1:
    print("DATA KOSONG / TABEL TIDAK DITEMUKAN")
    sys.exit(1)

data = []
for line in lines[1:]:
    status, total = line.split(",")
    data.append((status, int(total)))

print("\n=== DASHBOARD AKSES WEB (HIVE) ===\n")

max_val = max(t for _, t in data)

for status, total in data:
    bar = "#" * int((total * 40) / max_val)
    print("Status {:>3} | {:>6} | {}".format(status, total, bar))
