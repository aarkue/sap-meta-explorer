from copy import copy

class Shared:
    domnames_dictio = {}


def apply(con, min_count=1000):
    df = con.execute_read_sql("SELECT DOMNAME, Count(*) FROM "+con.table_prefix+"DD03VV GROUP BY DOMNAME ORDER BY Count(*) DESC", ["DOMNAME", "COUNT"])
    df = df[df["DOMNAME"] != " "]
    df = df[df["COUNT"] >= 1000]
    return df


def apply_static(con, min_count=1000):
    if not Shared.domnames_dictio:
        Shared.domnames_dictio = apply(con, min_count=min_count)
    return copy(Shared.domnames_dictio)
