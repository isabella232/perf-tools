# Upload TPCH Data in sql server
This script used to upload data generated from TPCH data generator into sql server using Bulk Copy Program Utility[(BCP)](https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15)

**Use following to generate tpch data :**

Clone [Hive Testbench Tool](https://github.com/hortonworks/hive-testbench) repository to the Compute Engine VM. Following steps need to be followed to generate the data :

* Run tpch-build.sh file in hive-testbench folder to generate a zip folder named tpch_kit inside hive-testbench/tpch_gen folder.
* Unzip the file to obtain a folder named tpch_2_16_0. Traverse to dbgen folder inside tpch_2_16_0 folder.
* Create a copy of makefile.suite file and name it as makefile. Edit this file to add following configurations :
```
cc = gcc
DATABASE = SQLSERVER
MACHINE = LINUX
WORKLOAD = TPCH
```
Save the file and run make which will generate the script named dbgen which can be used to generate the data.
To Generate 1TB TPCH data use following command:

    $ cd ~/hive-testbench/tpch-gen/tpch_2_16_0/dbgen
    $ ./dbgen -s 1000 -S 1 -C 100 -T L

To Generate 500GB TPCH data use following command:

    $ cd ~/hive-testbench/tpch-gen/tpch_2_16_0/dbgen
    $ ./dbgen -s 500 -S 1 -C 100 -T L

Copy these data files into some directory
before running python script.

**Python scripts operations :**

* Creates database and empty table with given database name and schema mentioned in constants.py file.
* Creates format file for particular table.
* Loads datafiles of TPC-H data using BCP command.

Data generate using tpch data generator and data is stored in some folder in home directory of VM instance.Provide this path in "create_table_pyodbc", "create_formatfile_bcp", "import_data_bcp" given functions to simply load data in sql server.


    Note : Change TPCH data path directory to the directory where your data is stored/generated.Also change the list of datafiles depending on the size of data.

Run Python code using:

    $python3 load_tpch_mssql.py
