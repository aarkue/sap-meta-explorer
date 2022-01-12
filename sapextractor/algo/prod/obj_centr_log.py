import pandas as pd
from dateutil import parser
from pm4pymdl.objects.mdl.exporter import exporter as mdl_exporter
from pm4pymdl.objects.ocel.exporter import exporter as ocel_exporter
from sapextractor.utils.dates import timestamp_column_from_dt_tm
from pandas.core.frame import DataFrame
from sapextractor.database_connection.interface import DatabaseConnection

def apply(con, keep_first=True, min_extr_date="2020-01-01 00:00:00", gjahr="2020", enable_changes=True,
          enable_payments=True, allowed_act_doc_types=None, allowed_act_changes=None, mandt="800"):
    print("WIP: production, 'apply' function")

    print("######################################################")
    print("Create Production Order")
    print("######################################################")
    afko_res = con.prepare_and_execute_query("AFKO", ["AUFNR","PLNBEZ", "GAMNG", "GMEIN"], additional_query_part=" WHERE MANDT = '"+mandt+"'");
    # Remove deleted ones
    aufk_res = con.prepare_and_execute_query("AUFK", ["AUFNR"], additional_query_part=" WHERE LOEKZ != 'X' AND MANDT = '"+mandt+"'");
    afko_res = afko_res.merge(aufk_res,left_on="AUFNR",right_on="AUFNR",how="inner");
    print(afko_res)
    afko_res['event_PRODORD'] = "OR" + afko_res["AUFNR"] #afko_res.apply(lambda row: "OR"+row.AUFNR)
    # removed:  CDTCODE='CO01' AND
    jcds_res = con.prepare_and_execute_query("JCDS", ["OBJNR", "CHGNR", "UDATE","UTIME","CDTCODE"], additional_query_part=" WHERE MANDT = '"+mandt+"'");
    print(jcds_res)
    dataframe = afko_res.merge(jcds_res,left_on="event_PRODORD",right_on="OBJNR",how="inner")
    print(dataframe)
    s026 = con.prepare_and_execute_query("S026", ["MATNR","MCOMB", "AUFNR"], additional_query_part=" WHERE MANDT = '"+mandt+"'").drop_duplicates();
    # s026.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)
    
    print(s026)
    dataframe['event_REQMAT'] = "";

    timestamp_column_from_dt_tm.apply(dataframe, "UDATE", "UTIME", "event_timestamp")
    min_extr_date = parser.parse(min_extr_date)
    dataframe = dataframe[dataframe["event_timestamp"] >= min_extr_date]
    dataframe = dataframe.sort_values("event_timestamp")
    dataframe = dataframe.drop_duplicates(subset=["event_PRODORD"],keep="first")
    


    # # Define relevant Production orders
    # relevant_production_orders_series = pd.Series(data=["OR000000822321"]);
    
    relevant_production_orders = dataframe["event_PRODORD"];
    print("relevant prod orders:", relevant_production_orders);
    # # Filter data
    # dataframe = dataframe.merge(relevant_production_orders,left_on="event_PRODORD",right_on="event_PRODORD",how="inner")
    # print("#### MERGED DATAFRAME: ")
    # print(dataframe)

    dataframe["DOCTYPE_RequiredMaterial"] = "";
    dataframe["DOCTYPE_RequiredMaterial"]  =  dataframe["DOCTYPE_RequiredMaterial"].astype(object)

    dataframe["event_REQMAT"] = "";
    # dataframe["event_REQMAT"]  =  dataframe["event_REQMAT"].astype(object)

    relevant_materials_series = pd.Series();
    dataframe: pd.DataFrame;
    for index, row in dataframe.iterrows():
        filtered = s026[(("OR"+s026['AUFNR']) == row['event_PRODORD'])];
        if not filtered.empty :
            print("Conncted Material:");
            if row['PLNBEZ'] == " ":
                dataframe.loc[index,'PLNBEZ'] = filtered['MATNR'].tolist()[0]
            relevant_materials_series = relevant_materials_series.append(filtered["MCOMB"]);
            connected_mat_list = filtered["MCOMB"].tolist()
            
            dataframe.at[index,'DOCTYPE_RequiredMaterial'] = connected_mat_list
            # for i, mat in enumerate(connected_mat_list):
            # dataframe.at[index,"event_REQMAT"+counter] = filtered["MCOMB"].toList(;
            dataframe.at[index,'event_REQMAT'] = str(connected_mat_list);

    relevant_materials = pd.DataFrame({'event_REQMAT': relevant_materials_series}).drop_duplicates(subset=["event_REQMAT"]);

    print(dataframe)

    dataframe.rename(columns={'PLNBEZ': 'event_MATNR'},inplace=True)
    dataframe.rename(columns={'GAMNG': 'event_MNG'},inplace=True)
    dataframe = dataframe.assign(DOCTYPE_ProdOrd = lambda x: x['event_PRODORD'])
    dataframe = dataframe.assign(DOCTYPE_Material = lambda x: x['event_MATNR'])
    # dataframe = dataframe.assign(DOCTYPE_RequiredMaterial = lambda x: str(x['event_REQMAT'].tolist()))
    # dataframe = dataframe.reset_index()
    # dataframe["event_id"] = dataframe.index.astype(str)
    dataframe["event_activity"] = "Create Production Order"
    dataframe = dataframe.drop(["AUFNR","OBJNR","CHGNR","UDATE","UTIME","CDTCODE","GMEIN"],axis=1)
    print(dataframe)

    # eban_cols = con.get_columns("EBAN")
    # print(eban_cols)
    # eban_test = con.prepare_and_execute_query("EBAN", eban_cols, additional_query_part=" WHERE BANFN = '0010044618' AND MANDT = '"+mandt+"'");
    # # resb = con.execute_read_sql("SELECT * FROM RESB WHERE AUFNR = '000000822321' AND MANDT = '"+mandt+"'",["RSNUM","RSPOS", "MATNR", "BDMNG","MEINS", "AUFNR", "BAUGR","BANFN","BNFPO"]);
    # print(eban_test);
    # eban_test.to_csv ('exported_eban.csv', index = True, header=True)

    print("######################################################")
    print("Plan Material// Create Purchase Requsition")
    print("######################################################")
    # TODO
    # print("THIS STEP IS IN PROGRESS")
    # Deletion Indicator: LOEKZ = X
    eban = con.prepare_and_execute_query("EBAN", ["BANFN","MATNR", "MENGE", "MEINS","BADAT"], additional_query_part=" WHERE LOEKZ != 'X' AND MANDT = '"+mandt+"'");
    # eban.rename(columns={'BANFN': 'event_PURCHREQ'},inplace=True)
    eban['event_PURCHREQ'] = "PR" + eban["BANFN"];
    eban.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)

    # Filter Materials
    eban = eban.merge(relevant_materials,left_on="event_REQMAT",right_on="event_REQMAT",how="inner")

    # Save relevant Purchase Requisitions
    relevant_purchase_requisitions_series = pd.Series();
    relevant_purchase_requisitions_series = relevant_purchase_requisitions_series.append(eban['event_PURCHREQ']);
    relevant_purchase_requisitions = pd.DataFrame({'event_PURCHREQ': relevant_purchase_requisitions_series}).drop_duplicates(subset=["event_PURCHREQ"]);
    print("Relevant Purchase Requisitions:")
    print(relevant_purchase_requisitions)

    eban.rename(columns={'MENGE': 'event_MNG'},inplace=True)
    eban.rename(columns={'MEINS': 'event_EIN'},inplace=True)
    eban["event_activity"] = "Create Purchase Requisition"
    eban['DOCTYPE_PurchReq'] = eban['event_PURCHREQ']
    eban['DOCTYPE_RequiredMaterial'] = eban['event_REQMAT']
    
    eban['TIME'] = "235959"; # TODO: Made up timestamp
    # timestamp_column_from_dt_tm.apply(eban, "BADAT", "TIME", "event_timestamp") # filter only based on day
    # # TODO: TEMP REMOVED, in favor of having only release event (with TIME!) in log
    # # eban = eban[eban["event_timestamp"] >= min_extr_date]
    # eban['TIME'] = "000000";  # but set time to start of day after that
    # timestamp_column_from_dt_tm.apply(eban, "BADAT", "TIME", "event_timestamp") 
    eban = eban.drop(["BADAT","TIME","BANFN"],axis=1)
    print(eban)
    
    print("######################################################")
    print("Release Purchase Requisition")
    print("######################################################")
    # TODO: Include  TCODE and CHNGIND: update?
    cdpos_res = con.prepare_and_execute_query("CDPOS", ["OBJECTID","CHANGENR", "FNAME","VALUE_NEW"], additional_query_part=" WHERE (VALUE_NEW = 'X' OR VALUE_NEW='XX') AND CHNGIND='U' AND FNAME='FRGZU' AND MANDANT = '"+mandt+"'");
    print(cdpos_res)
    #Removed: TCODE = 'ME54' AND 
    cdhdr_res = con.prepare_and_execute_query("CDHDR", ["OBJECTID","CHANGENR", "UDATE", "UTIME"], additional_query_part=" WHERE MANDANT = '"+mandt+"'");
    print(cdhdr_res)
    release_purch_req_data = cdpos_res.merge(cdhdr_res,left_on=["OBJECTID","CHANGENR"],right_on=["OBJECTID","CHANGENR"],how="inner") #.drop_duplicates(subset=["OBJECTID","CHANGENR","UDATE","UTIME"])
    print(release_purch_req_data)
    timestamp_column_from_dt_tm.apply(release_purch_req_data, "UDATE", "UTIME", "event_timestamp")
    release_purch_req_data = release_purch_req_data[release_purch_req_data["event_timestamp"] >= min_extr_date]
    release_purch_req_data = release_purch_req_data.sort_values("event_timestamp")
    # release_purch_req_data["event_id"] = release_purch_req_data.index.astype(str)
    release_purch_req_data["event_activity"] = ""
    release_purch_req_data["event_activity"] = release_purch_req_data.apply(lambda x: 'Release Purchase Requisition (1)'  if x['VALUE_NEW'] == 'X' else ('Release Purchase Requisition (2)' if x['VALUE_NEW'] == 'XX' else 'Release PurReq: ERR_UNKNOWN_CHANGE'), axis=1)
    release_purch_req_data.rename(columns={'PLNBEZ': 'event_REQMAT'},inplace=True)
    release_purch_req_data['event_PURCHREQ'] = "PR" + release_purch_req_data["OBJECTID"]
    release_purch_req_data.rename(columns={'VALUE_NEW': 'event_NEW-FRGZU'},inplace=True)
    release_purch_req_data = release_purch_req_data.assign(DOCTYPE_PurchReq = lambda x: x['event_PURCHREQ'])
    release_purch_req_data["event_REQMAT"] = ""
    if not release_purch_req_data.empty:
        release_purch_req_data["event_REQMAT"] = release_purch_req_data.apply(lambda x:eban.loc[eban['event_PURCHREQ'] == x['event_PURCHREQ']]['event_REQMAT'].values[0] if eban.loc[eban['event_PURCHREQ'] == x['event_PURCHREQ']]['event_REQMAT'].values.size > 0 else '', axis=1)
    # Filter only relevant Purchase Requisitions
    release_purch_req_data = release_purch_req_data.merge(relevant_purchase_requisitions,left_on="event_PURCHREQ",right_on="event_PURCHREQ",how="inner")
    release_purch_req_data = release_purch_req_data.assign(DOCTYPE_RequiredMaterial = lambda x: x['event_REQMAT'])
    # release_purch_req_data['DOCTYPE_RequiredMaterial'] = release_purch_req_data['event_REQMAT']
    print(release_purch_req_data)
    release_purch_req_data = release_purch_req_data.drop(["OBJECTID","CHANGENR","FNAME","UDATE","UTIME"],axis=1)
    print(release_purch_req_data)
    

    print("######################################################")
    print("Convert PR to Purchase Order")
    print("######################################################")
    ekpo_res = con.prepare_and_execute_query("EKPO",["EBELN","MATNR","MENGE", "MEINS", "BANFN"],additional_query_part=" WHERE BANFN != ' ' AND MANDT = '"+mandt+"'");
    print(ekpo_res)
    # cdpos_res_2 = con.prepare_and_execute_query("CDPOS",["OBJECTID","CHANGENR"],additional_query_part=" WHERE OBJECTCLAS = 'EINKBELEG' AND CHNGIND = 'I' AND MANDANT = '"+mandt+"'");
    # print(cdpos_res_2)
    cdhdr_res_2 = con.prepare_and_execute_query("CDHDR", ["OBJECTID","CHANGENR", "UDATE", "UTIME"], additional_query_part=" WHERE OBJECTCLAS = 'EINKBELEG' AND CHANGE_IND = 'I' AND MANDANT = '"+mandt+"'");
    print(cdhdr_res_2)
    convert_to_purch_order = ekpo_res.merge(cdhdr_res_2,left_on=["EBELN"],right_on=["OBJECTID"],how="inner") #.drop_duplicates(subset=["OBJECTID","CHANGENR","UDATE","UTIME"])
    print(convert_to_purch_order)
    timestamp_column_from_dt_tm.apply(convert_to_purch_order, "UDATE", "UTIME", "event_timestamp")
    convert_to_purch_order = convert_to_purch_order[convert_to_purch_order["event_timestamp"] >= min_extr_date]
    convert_to_purch_order = convert_to_purch_order.sort_values("event_timestamp")
    convert_to_purch_order.rename(columns={'PLNBEZ': 'event_REQMAT'},inplace=True)
    convert_to_purch_order['event_PURCHORD'] = "PUOR" + convert_to_purch_order["EBELN"]
    convert_to_purch_order['event_PURCHREQ'] = "PR" + convert_to_purch_order["BANFN"] # Can be empty
    # convert_to_purch_order['event_PURCHREQ'] =  convert_to_purch_order.apply(lambda x: "" if (x["BANFN"] == "" or x["BANFN"] == " ") else ("PR"+x["BANFN"]), axis=1)  
    convert_to_purch_order.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)
    if not convert_to_purch_order.empty:
        convert_to_purch_order = convert_to_purch_order.merge(relevant_materials,left_on="event_REQMAT",right_on="event_REQMAT",how="inner")

    print(convert_to_purch_order);

    # Filter only relevant Purchase Requisitions
    convert_to_purch_order = convert_to_purch_order.merge(relevant_purchase_requisitions,left_on="event_PURCHREQ",right_on="event_PURCHREQ",how="inner")

    # Save relevant Purchase Orders
    relevant_purchase_orders_series = pd.Series();
    relevant_purchase_orders_series = relevant_purchase_orders_series.append(convert_to_purch_order['event_PURCHORD']);
    relevant_purchase_orders = pd.DataFrame({'event_PURCHORD': relevant_purchase_orders_series}).drop_duplicates(subset=["event_PURCHORD"]);


    convert_to_purch_order.rename(columns={'MENGE': 'event_MNG'},inplace=True)
    convert_to_purch_order.rename(columns={'MEINS': 'event_EIN'},inplace=True)
    convert_to_purch_order = convert_to_purch_order.assign(DOCTYPE_PurchOrd = lambda x: x['event_PURCHORD'])
    convert_to_purch_order['DOCTYPE_PurchReq'] = "";
    convert_to_purch_order['DOCTYPE_PurchReq'] = convert_to_purch_order.apply(lambda x: None if x['event_PURCHREQ'] == '' else x['event_PURCHREQ'], axis=1)


    convert_to_purch_order = convert_to_purch_order.assign(DOCTYPE_RequiredMaterial = lambda x: x['event_REQMAT'])
    convert_to_purch_order["event_activity"] = "Convert to Purchase Order"
    print(convert_to_purch_order)
    convert_to_purch_order = convert_to_purch_order.drop(["OBJECTID","CHANGENR","UDATE","UTIME","EBELN","BANFN"],axis=1)
    print(convert_to_purch_order)
    
    print("######################################################")
    print("Release Purchase Order")
    print("######################################################") # AND FNAME='FRGZU' ? (VALUE_NEW = 'X') AND
    release_purchase_order_cdpos = con.prepare_and_execute_query("CDPOS", ["OBJECTID","CHANGENR", "FNAME","VALUE_NEW"], additional_query_part=" WHERE CHNGIND='U' AND VALUE_NEW = 'X' AND FNAME='FRGZU' AND OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");   
    print(release_purchase_order_cdpos)
    release_purchase_order_cdhdr = con.prepare_and_execute_query("CDHDR", ["OBJECTID","CHANGENR", "UDATE", "UTIME"], additional_query_part=" WHERE OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");
    print(release_purchase_order_cdhdr)
    release_purch_order = release_purchase_order_cdpos.merge(release_purchase_order_cdhdr,left_on=["OBJECTID","CHANGENR"],right_on=["OBJECTID","CHANGENR"],how="inner") #.drop_duplicates(subset=["OBJECTID","CHANGENR","UDATE","UTIME"])
    print(release_purch_order)
    timestamp_column_from_dt_tm.apply(release_purch_order, "UDATE", "UTIME", "event_timestamp")
    release_purch_order = release_purch_order[release_purch_order["event_timestamp"] >= min_extr_date]
    release_purch_order = release_purch_order.sort_values("event_timestamp")
    # # release_purch_req_data["event_id"] = release_purch_req_data.index.astype(str)
    # # release_purch_req_data["event_activity"] = "Release Purchase Requisition"
    release_purch_order["event_activity"] = ""
    if not release_purch_order.empty:
        release_purch_order["event_activity"] = release_purch_order.apply(lambda x: 'Release Purchase Order (Normal)'  if x['VALUE_NEW'] == 'X' else 'Release Purchase Order (Special)', axis=1)
    release_purch_order['event_PURCHORD'] = "PUOR" + release_purch_order["OBJECTID"]

    # Filter only relevant Purchase Requisitions
    release_purch_order = release_purch_order.merge(relevant_purchase_orders,left_on="event_PURCHORD",right_on="event_PURCHORD",how="inner")


    release_purch_order.rename(columns={'VALUE_NEW': 'event_NEW-FRGZU'},inplace=True)
    release_purch_order = release_purch_order.assign(DOCTYPE_PurchOrd = lambda x: x['event_PURCHORD'])
    print(release_purch_order)
    release_purch_order = release_purch_order.drop(["OBJECTID","CHANGENR","FNAME","UDATE","UTIME"],axis=1)
    print(release_purch_order)


    print("######################################################")
    print("Reject Purchase Order")
    print("######################################################") # (VALUE_NEW = '08') AND
    reject_purchase_order_cdpos = con.prepare_and_execute_query("CDPOS", ["OBJECTID","CHANGENR", "FNAME","VALUE_NEW"], additional_query_part=" WHERE (VALUE_NEW = 'B') AND CHNGIND='U' AND FNAME='FRGKE' AND OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");
    print(reject_purchase_order_cdpos)
    reject_purchase_order_cdhdr = con.prepare_and_execute_query("CDHDR", ["OBJECTID","CHANGENR", "UDATE", "UTIME"], additional_query_part=" WHERE OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");
    print(reject_purchase_order_cdhdr)
    reject_purch_order = reject_purchase_order_cdpos.merge(reject_purchase_order_cdhdr,left_on=["OBJECTID","CHANGENR"],right_on=["OBJECTID","CHANGENR"],how="inner") #.drop_duplicates(subset=["OBJECTID","CHANGENR","UDATE","UTIME"])
    reject_purch_order = reject_purch_order.merge(ekpo_res.drop(["MENGE", "MEINS", "BANFN"],axis=1),left_on=["OBJECTID"],right_on=["EBELN"],how="inner")
    reject_purch_order.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)
    if not reject_purch_order.empty:
        reject_purch_order = reject_purch_order.merge(relevant_materials,left_on="event_REQMAT",right_on="event_REQMAT",how="inner")
    print(reject_purch_order)
    timestamp_column_from_dt_tm.apply(reject_purch_order, "UDATE", "UTIME", "event_timestamp")
    reject_purch_order = reject_purch_order[reject_purch_order["event_timestamp"] >= min_extr_date]
    reject_purch_order = reject_purch_order.sort_values("event_timestamp")
    reject_purch_order["event_activity"] = ""
    if not reject_purch_order.empty:
        reject_purch_order["event_activity"] = reject_purch_order.apply(lambda x: 'Reject Purchase Order'  if x['VALUE_NEW'] == 'B' else 'Reject PurOrd: ERR_UNKNOWN_CHANGE', axis=1)

    reject_purch_order['event_PURCHORD'] = "PUOR" + reject_purch_order["OBJECTID"]

    # Filter only relevant Purchase Requisitions
    reject_purch_order = reject_purch_order.merge(relevant_purchase_orders,left_on="event_PURCHORD",right_on="event_PURCHORD",how="inner")

    reject_purch_order.rename(columns={'VALUE_NEW': 'event_NEW-FRGZU'},inplace=True)
    reject_purch_order = reject_purch_order.assign(DOCTYPE_PurchOrd = lambda x: x['event_PURCHORD'])
    reject_purch_order = reject_purch_order.assign(DOCTYPE_RequiredMaterial = lambda x: x['event_REQMAT'])
    print(reject_purch_order)
    reject_purch_order = reject_purch_order.drop(["OBJECTID","CHANGENR","FNAME","UDATE","UTIME","EBELN"],axis=1)
    print(reject_purch_order)


    print("######################################################")
    print("Reconsider Purchase Order")
    print("######################################################") # (VALUE_NEW = '08') AND
    reconsider_purchase_order_cdpos = con.prepare_and_execute_query("CDPOS", ["OBJECTID","CHANGENR", "FNAME","VALUE_NEW"], additional_query_part=" WHERE (VALUE_NEW = 'A') AND CHNGIND='U' AND FNAME='FRGKE' AND OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");   
    print(reconsider_purchase_order_cdpos)
    reconsider_purchase_order_cdhdr = con.prepare_and_execute_query("CDHDR", ["OBJECTID","CHANGENR", "UDATE", "UTIME"], additional_query_part=" WHERE OBJECTCLAS = 'EINKBELEG' AND MANDANT = '"+mandt+"'");
    print(reconsider_purchase_order_cdhdr)
    reconsider_purch_order = reconsider_purchase_order_cdpos.merge(reconsider_purchase_order_cdhdr,left_on=["OBJECTID","CHANGENR"],right_on=["OBJECTID","CHANGENR"],how="inner") #.drop_duplicates(subset=["OBJECTID","CHANGENR","UDATE","UTIME"])
    print(reconsider_purch_order)
    timestamp_column_from_dt_tm.apply(reconsider_purch_order, "UDATE", "UTIME", "event_timestamp")
    reconsider_purch_order = reconsider_purch_order[reconsider_purch_order["event_timestamp"] >= min_extr_date]
    reconsider_purch_order = reconsider_purch_order.sort_values("event_timestamp")
    reconsider_purch_order["event_activity"] = ""
    if not reconsider_purch_order.empty:
        reconsider_purch_order["event_activity"] = reconsider_purch_order.apply(lambda x: 'Reconsider Purchase Order'  if x['VALUE_NEW'] == 'A' else 'Reconsider PurOrd: ERR_UNKNOWN_CHANGE', axis=1)
    reconsider_purch_order['event_PURCHORD'] = "PUOR" + reconsider_purch_order["OBJECTID"]

    # Filter only relevant Purchase Requisitions
    reconsider_purch_order = reconsider_purch_order.merge(relevant_purchase_orders,left_on="event_PURCHORD",right_on="event_PURCHORD",how="inner")
    reconsider_purch_order.rename(columns={'VALUE_NEW': 'event_NEW-FRGKE'},inplace=True)
    reconsider_purch_order = reconsider_purch_order.assign(DOCTYPE_PurchOrd = lambda x: x['event_PURCHORD'])
    print(reconsider_purch_order)
    reconsider_purch_order = reconsider_purch_order.drop(["OBJECTID","CHANGENR","FNAME","UDATE","UTIME"],axis=1)
    print(reconsider_purch_order)




    print("######################################################")
    print("Goods Receipt for Purchase Order")
    print("######################################################")
    goods_receipt_for_purchase_order = con.prepare_and_execute_query("EKBE",["EBELN","CPUDT","CPUTM","BELNR","MATNR", "MENGE"], additional_query_part=" WHERE (NOT CPUDT = '00000000') AND MANDT = '"+mandt+"'")
    print(goods_receipt_for_purchase_order)
    timestamp_column_from_dt_tm.apply(goods_receipt_for_purchase_order, "CPUDT", "CPUTM", "event_timestamp")
    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order[goods_receipt_for_purchase_order["event_timestamp"] >= min_extr_date]
    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.sort_values("event_timestamp")
    if not goods_receipt_for_purchase_order.empty:
        goods_receipt_for_purchase_order["event_activity"] = goods_receipt_for_purchase_order.apply(lambda x: 'Goods Receipt for Purchase Order', axis=1)
    goods_receipt_for_purchase_order['event_PURCHORD'] = "PUOR" + goods_receipt_for_purchase_order["EBELN"]
    goods_receipt_for_purchase_order.rename(columns={'MENGE': 'event_MENGE'},inplace=True)
    # Filter only relevant Purchase Orders
    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.merge(relevant_purchase_orders,left_on="event_PURCHORD",right_on="event_PURCHORD",how="inner")


    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.assign(DOCTYPE_PurchOrd = lambda x: x['event_PURCHORD'])
    goods_receipt_for_purchase_order['event_MATDOC'] = "MATDOC" + goods_receipt_for_purchase_order["BELNR"]
    #?##goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.assign(DOCTYPE_MatDoc = lambda x: x['event_MATDOC'])
    goods_receipt_for_purchase_order.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)
    if not goods_receipt_for_purchase_order.empty:
        goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.merge(relevant_materials,left_on="event_REQMAT",right_on="event_REQMAT",how="inner")

    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.assign(DOCTYPE_RequiredMaterial = lambda x: x['event_REQMAT'])
    goods_receipt_for_purchase_order = goods_receipt_for_purchase_order.drop(["EBELN","CPUDT","CPUTM","BELNR"],axis=1)
    print(goods_receipt_for_purchase_order)

    print("######################################################")
    print("Goods Issue for Production Order")
    print("######################################################")
    goods_issue_for_production_order : DataFrame = con.prepare_and_execute_query("MSEG",["MBLNR","MATNR","MENGE","MEINS","AUFNR","CPUDT_MKPF","CPUTM_MKPF","BWART"], additional_query_part=" WHERE AUFNR > '000000000000' AND MANDT = '"+mandt+"' AND (NOT CPUDT_MKPF = '00000000')")
    print(goods_issue_for_production_order)
    timestamp_column_from_dt_tm.apply(goods_issue_for_production_order, "CPUDT_MKPF", "CPUTM_MKPF", "event_timestamp")
    goods_issue_for_production_order = goods_issue_for_production_order[goods_issue_for_production_order["event_timestamp"] >= min_extr_date]
    goods_issue_for_production_order = goods_issue_for_production_order.sort_values("event_timestamp")
    goods_issue_for_production_order['event_PRODORD'] = ""
    if not goods_issue_for_production_order.empty:
        goods_issue_for_production_order["event_activity"] = goods_issue_for_production_order.apply(lambda x: 'Goods Issue for Production Order', axis=1)
        goods_issue_for_production_order['event_PRODORD'] =  goods_issue_for_production_order.apply(lambda x: "" if (x["AUFNR"] == "") else ("OR"+x["AUFNR"]), axis=1) 

    # Filter
    goods_issue_for_production_order = goods_issue_for_production_order.merge(relevant_production_orders,left_on="event_PRODORD",right_on="event_PRODORD",how="inner")
    goods_issue_for_production_order.rename(columns={'MATNR': 'event_REQMAT'},inplace=True)
    goods_issue_for_production_order.rename(columns={'BWART': 'event_BWART'},inplace=True)
    
    if not goods_issue_for_production_order.empty:
        goods_issue_for_production_order = goods_issue_for_production_order.merge(relevant_materials,left_on="event_REQMAT",right_on="event_REQMAT",how="inner")


    gi_grouped = goods_issue_for_production_order.groupby(['AUFNR','CPUDT_MKPF','CPUTM_MKPF'])
    
    goods_issue_for_production_order_processed = pd.DataFrame()
    goods_issue_for_production_order_processed['MBLNR'] = ""
    for name,group in gi_grouped:
        toAdd = group.copy();
        toAdd["DOCTYPE_RequiredMaterial"] = "";
        toAdd['DOCTYPE_RequiredMaterial']=toAdd['DOCTYPE_RequiredMaterial'].astype('object')
        if(group.shape[0] > 1):
            related_req_mats = toAdd["event_REQMAT"].tolist()
            toAdd:DataFrame = toAdd.drop_duplicates(subset=['AUFNR','CPUDT_MKPF','CPUTM_MKPF'])
            # toAdd["DOCTYPE_RequiredMaterial"] = "";
            # toAdd["DOCTYPE_RequiredMaterial"]  =  toAdd["DOCTYPE_RequiredMaterial"].astype(object)
            for index, row in toAdd.iterrows():
                toAdd.at[index,'DOCTYPE_RequiredMaterial'] = related_req_mats
            toAdd['event_REQMAT'] = str(related_req_mats);
        else:
            toAdd['DOCTYPE_RequiredMaterial'] = group["event_REQMAT"]
            # toAdd['event_REQMAT'] = group["event_REQMAT"]
        goods_issue_for_production_order_processed = goods_issue_for_production_order_processed.append(toAdd)

    goods_issue_for_production_order_processed['event_MATDOC'] = "MATDOC" + goods_issue_for_production_order_processed["MBLNR"]
  
    goods_issue_for_production_order_processed.rename(columns={'MENGE': 'event_MNG'},inplace=True)
    goods_issue_for_production_order_processed.rename(columns={'MEINS': 'event_EIN'},inplace=True)
    if not goods_issue_for_production_order.empty:
        goods_issue_for_production_order_processed = goods_issue_for_production_order_processed.assign(DOCTYPE_ProdOrd = lambda x: x['event_PRODORD']) 
        goods_issue_for_production_order_processed = goods_issue_for_production_order_processed.drop(["MBLNR","AUFNR","CPUDT_MKPF","CPUTM_MKPF"],axis=1)
    else:
        goods_issue_for_production_order_processed['DOCTYPE_ProdOrd'] = ''

    #?##goods_issue_for_production_order_processed = goods_issue_for_production_order_processed.assign(DOCTYPE_MatDoc = lambda x: x['event_MATDOC'])
    # goods_issue_for_production_order_processed = goods_issue_for_production_order_processed.assign(DOCTYPE_RequiredMaterial = lambda x: x['event_REQMAT'])
    print(goods_issue_for_production_order_processed)


    print("######################################################")
    print("Production Order Confirmation")
    print("######################################################")

    production_order_confirmation = con.prepare_and_execute_query("AFRU",["AUFNR","LMNGA","XMNGA","GMEIN","ERSDA","ERZET"],additional_query_part=" WHERE MANDT = '"+mandt+"'")
    print(production_order_confirmation)
    production_order_confirmation_mat = con.prepare_and_execute_query("AUFM",["MATNR","MBLNR","AUFNR"],additional_query_part=" WHERE ELIKZ = 'X' AND MANDT = '"+mandt+"'")
    print(production_order_confirmation_mat)

    production_order_confirmation = production_order_confirmation.merge(production_order_confirmation_mat,left_on="AUFNR",right_on="AUFNR",how="inner").drop_duplicates(subset=["AUFNR"])
    print(production_order_confirmation);
    timestamp_column_from_dt_tm.apply(production_order_confirmation, "ERSDA", "ERZET", "event_timestamp")
    production_order_confirmation = production_order_confirmation[production_order_confirmation["event_timestamp"] >= min_extr_date]
    production_order_confirmation = production_order_confirmation.sort_values("event_timestamp")
    production_order_confirmation['event_PRODORD'] = ""
    if not production_order_confirmation.empty:
        production_order_confirmation['event_PRODORD'] =  production_order_confirmation.apply(lambda x: "" if (x["AUFNR"] == "") else ("OR"+x["AUFNR"]), axis=1) 

    print(production_order_confirmation)
    # Filter
    production_order_confirmation = production_order_confirmation.merge(relevant_production_orders,left_on="event_PRODORD",right_on="event_PRODORD",how="inner")

    production_order_confirmation.rename(columns={'LMNGA': 'event_MNG'},inplace=True)
    production_order_confirmation.rename(columns={'XMNGA': 'event_SCRAP_MNG'},inplace=True)
    production_order_confirmation.rename(columns={'GMEIN': 'event_EIN'},inplace=True)
    production_order_confirmation.rename(columns={'MATNR': 'event_MATNR'},inplace=True)
    production_order_confirmation['event_MATDOC'] = "MATDOC" + production_order_confirmation["MBLNR"]
    #?##production_order_confirmation = production_order_confirmation.assign(DOCTYPE_MatDoc = lambda x: x['event_MATDOC'])
    production_order_confirmation = production_order_confirmation.assign(DOCTYPE_Material = lambda x: x['event_MATNR'])
    if not production_order_confirmation.empty:
        production_order_confirmation["event_activity"] = production_order_confirmation.apply(lambda x: 'Confirmation of Production Order', axis=1)
    production_order_confirmation = production_order_confirmation.assign(DOCTYPE_ProdOrd = lambda x: x['event_PRODORD']) 
    production_order_confirmation = production_order_confirmation.drop(["AUFNR","ERSDA","ERZET","MBLNR"],axis=1);
    print(production_order_confirmation);
    
    union = pd.concat([dataframe,release_purch_req_data,convert_to_purch_order,release_purch_order,reject_purch_order,reconsider_purch_order,goods_receipt_for_purchase_order,goods_issue_for_production_order_processed,production_order_confirmation]).sort_values("event_timestamp").reset_index()
    union["event_id"] = union.index.astype(str)
    return union


def cli(con):
    print("\n\nProd Object-Centric Log Extractor\n\n")
    min_extr_date = input("Insert the minimum extraction date (default: 2020-01-01 00:00:00): ")
    if not min_extr_date:
        min_extr_date = "2020-01-01 00:00:00"
    gjahr = input("Insert the fiscal year (default: 2020):")
    if not gjahr:
        gjahr = "2020"
    dataframe = apply(con, min_extr_date=min_extr_date, gjahr=gjahr)
    path = input("Insert the path where the log should be saved (default: prod.xmlocel): ")
    if not path:
        path = "prod.xmlocel"
    if path.endswith("mdl"):
        mdl_exporter.apply(dataframe, path)
    elif path.endswith("jsonocel") or path.endswith("xmlocel"):
        ocel_exporter.apply(dataframe, path)
