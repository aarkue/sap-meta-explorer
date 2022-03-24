# SAP Meta-Explorer
## Web-tool to explore SAP meta-tables
This web tool is based on [SAP Extractor](https://github.com/Javert899/sap-extractor) and provides a few additional pages to explore meta-information of the ERP system.
For that, the corresponding meta-tables are first extracted from the SAP ERP system into an SQLite DB.
The tables `DD02T`, `DD03L` and `DD03M` are of interest and should be dumped for the intended language (e.g., `WHERE DDLANGUAGE = 'EN'`).
Additionally, a table or view of `DD03L` and `DD03M` called `DD03L_DD03M` should be created, which adds information on fields related tables (an example for that can be found below).

Using the SQLite Dump allows continued usage, also offline. But with small modifications, a direct connection to the SAP ERP systems DB is, of course, also possible.


```
CREATE VIEW DD03M_DD03L
AS
  SELECT
   *
  FROM
    DD03L
   LEFT JOIN
    DD03M
   ON DD03M.TABNAME=DD03L.TABNAME AND DD03M.FIELDNAME=DD03L.FIELDNAME
```

# SAP Extractor
Below you can find the original info for the rest of the functionality, which was presented in https://arxiv.org/abs/2110.03467.
See the original repository https://github.com/Javert899/sap-extractor for additional and up-to-date information.
# Process Mining SAP ERP ECC extractor (Python)

This project implements some connectors for the mainstream processes in SAP ERP ECC.

### Install

The project is available as a Python package in Pypi.
It can be installed under a Python >=3.6 using:

**pip install -U sapextractor**

### Included resources

The root folder of the project includes a SQLIte dump of
a SAP IDES instance, which can be used to test the extraction.

### Basic usage (command line interface)

The project can be easily used from the command line:

**import sapextractor**

**sapextractor.cli()**

The command line interface asks to insert the connection parameters to the database,
and the details about the extraction.

### Supported processes

We support different processes, and modalities of extraction:

##### Order to Cash

For the Order-to-Cash process, we can extract a dataframe (that can be stored in CSV/Parquet files),
a XES log, and object-centric logs (in the MDL and JMD formats).

The two classic log modalities asks for a central document type (for each document of the type, a case is created
with all the operations on the connected documents).

##### Accounting

For the processes related to the accounting (such as the Accounts-Payable and the Accounts-Receivable processes),
we offer different extraction possibilities:
* Dataframe (Parquet/CSV) containing a case per document. Each case contains all the transactions that are executed on the document.
* XES log containing a case per document. Each case contains all the transactions that are executed on the document.
* **Document Flow**: given a central document type, provide as many cases as many documents of such type. Each case contains, as events,
the connected documents to the 'central' document of the case. It is possible to extract both a dataframe (Parquet/CSV) and a XES log.
* **Transactions for the documents in a Document Flow**: given a central document type, provide as many cases as many documents
of such type. Each case contains, as events, the transactions executed on the connected documents to the 'central' document of the case.
It is possible to extract both a dataframe (Parquet/CSV) and a XES log.
* Object-Centric event logs (in the MDL and JMD formats).

##### Procurement

For the procurement, we can extract a dataframe (that can be stored in CSV/Parquet files),
a XES log, and object-centric logs (in the MDL and JMD formats).

The two classic log modalities asks for a central document type (for each document of the type, a case is created
with all the operations on the connected documents).

### Supported databases

The extraction happens directly at the database level.
We provide support for the extraction from the following databases:
* SQLite (as we provide a SAP IDES database dump in that format).
* Oracle (throught the cx_Oracle package).

