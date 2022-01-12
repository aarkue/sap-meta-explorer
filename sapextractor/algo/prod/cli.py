from sapextractor.algo.prod import obj_centr_log


def cli(con):
    print("\n\nProd extraction\n")
    print("available extraction types:")
    print("1) Prod object-centric log")
    print()
    ext_type = input("insert your choice (default: 1): ")
    if not ext_type:
        ext_type = "1"
    if ext_type == "1":
        return obj_centr_log.cli(con)
