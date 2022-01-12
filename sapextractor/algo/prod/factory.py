from sapextractor.algo.prod import obj_centr_log


def apply(con, ext_type, ext_arg):
    print("ext_type")
    print(ext_type)
    if ext_type == "prod_1d_log_extractor":
        pass
        # return o2c_1d_log_extractor.apply(con, **ext_arg)
    elif ext_type == "prod_1d_dataframe_extractor":
        pass
        # return o2c_1d_dataframe_extractor.apply(con, **ext_arg)
    elif ext_type == "prod_ocel":
        return obj_centr_log.apply(con, **ext_arg)
