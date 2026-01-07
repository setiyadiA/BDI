import subprocess

query = """
SELECT col9 AS status, COUNT(*) AS total
FROM datalog_akses_raw
GROUP BY col9
ORDER BY status
"""

cmd = [
    "beeline",
    "-u", "jdbc:hive2://localhost:10000/default",
    "--outputformat=csv2",
    "-e", query
]

p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
output = p.communicate()[0]

lines = output.strip().split("\n")

# Skip header
data = []
for line in lines[1:]:
    status, total = line.split(",")
    data.append((status, int(total)))

print("\n=== DASHBOARD AKSES WEB (HIVE) ===\n")

max_val = max(t for _, t in data)

for status, total in data:
    bar = "#" * int((total * 40) / max_val)
    print("Status {:>3} | {:>6} | {}".format(status, total, bar))
