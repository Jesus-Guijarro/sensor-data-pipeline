def extract(conn):
    return conn.execute("SELECT * FROM sensor_temperatures").fetchall()

def transform(rows):
    out = []
    for r in rows:
        # regla de negocio
        r["temp"] = None if r["temp"] > 150 else r["temp"]
        out.append(r)
    return out

def load(clean_rows, conn):
    conn.executemany(
        "INSERT INTO sensor_temperatures_clean VALUES (:sensor_id, :ts, :temp)",
        clean_rows
    )

def run(conn_src, conn_dst):
    rows = extract(conn_src)
    clean = transform(rows)
    load(clean, conn_dst)