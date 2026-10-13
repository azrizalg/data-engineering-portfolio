from psycopg import sql

def check_not_null(conn, table_name, column_name):
    query = sql.SQL("""
        SELECT COUNT(*)
        FROM {}
        WHERE {} IS NULL
    """).format(
        sql.Identifier(table_name),
        sql.Identifier(column_name)
    )

    with conn.cursor() as cur:
        cur.execute(query)
        failed_rows = cur.fetchone()[0]

    return {
        "check_name": f"{table_name}.{column_name}.not_null",
        "passed": failed_rows == 0,
        "failed_rows": failed_rows,
    }
    



def check_unique(conn, table_name, column_name):
    query = sql.SQL("""
        SELECT COUNT(*) - COUNT(DISTINCT {})
        FROM {}
    """).format(
        sql.Identifier(column_name),
        sql.Identifier(table_name)
    )

    with conn.cursor() as cur:
        cur.execute(query)
        failed_rows = cur.fetchone()[0]

    return {
        "check_name": f"{table_name}.{column_name}.unique",
        "passed": failed_rows == 0,
        "failed_rows": failed_rows,
    }
    


def run_dq_checks(conn):
    checks = [
        ("customers", "customer_id"),
        ("customers", "name"),
        ("customers", "city"),
    ]

    results = []

    for table_name, column_name in checks:
        result = check_not_null(conn, table_name, column_name)
        results.append(result)

    return results

def dq_passed(results):
    return all(result["passed"] for result in results)