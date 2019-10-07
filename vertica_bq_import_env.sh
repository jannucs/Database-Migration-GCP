#!/usr/bin/env bash

db=$1
process=$2
subprocess=$3

echo ${db} ${process} ${subprocess}
#log file path
export LOGFILEPATH=/logs/src_bq_import/$process/$subprocess

#******************************************************************************#
if [ $db = 'vertica' ]
then
#***********************VERTICA PARAMS*****************************************#

	export JDBC_CONNECTION_URL="jdbc:vertica://localhost:5433/DB"
	export TARGET_USERNAME=verticaETL
	export TARGET_PASSWORD_FILE=/user/password/prod/sqoop.vertica_prod

#***********************VERTICA PARAMS*****************************************#
	if [ $process = 'SRC_BQ_IMPORT' ]
	then
		if [ $subprocess = 'table1' ]
		then
			export HDFS_PATH=/table1
			export HDFS_AUDIT_PATH=/audit/table1
			export PROCESS_NAME='table1'
			export SCHEMA_NAME='table1'
			export TARGET_TABLE=TABLE_NAME
			export LASTVALUECOL=TimeStamp
			export SPLITBYCOL=ID
			export GSBUCKET=gs://GSBUCKET/table1
			export NUM_MAPPER=8
			export SQOOPJOBNAME=SqoopJob_table1
			export AUDIT_PATH=/audit/table1
			export SQOOP_BATCH_ID=SQOOP_BATCH_ID.txt
			export DAG_BATCH_ID=DAG_BATCH_ID.txt
			export COMPOSER_DAG="DAG_Name"
			export COMPOSER_ENV="/composer.ini"
			export ADOBE_DAG_CONFIG="table1.json"
		elif [ $subprocess = 'item_dim_t' ]
		then
			export HDFS_PATH=/item_dim_t
			export HDFS_AUDIT_PATH=/audit/table2
			export PROCESS_NAME='TABLE2'
			export SCHEMA_NAME='schema2'
			export TARGET_TABLE=TABLE2
			export LASTVALUECOL=TS
			export SPLITBYCOL=KEY
			export GSBUCKET=gs://GSBUCKET/TABLE2
			export NUM_MAPPER=16
			export SQOOPJOBNAME=SqoopJob_table2
			export AUDIT_PATH=/audit/table2
			export SQOOP_BATCH_ID=SQOOP_BATCH_ID.txt
			export DAG_BATCH_ID=DAG_BATCH_ID.txt
			export COMPOSER_DAG="DAG2_name"
			export COMPOSER_ENV="composer.ini"
			export ADOBE_DAG_CONFIG="table2.json"
#YOUR NEW TABLE Here as above - add a new table so that script ll pick this as well
		else
			echo " No valid subprocess name has been passed"
			exit 1
		fi
	else
			echo " No valid process name has been passed"
			exit 1
	fi
else
	echo " No valid Database name has been passed"
	exit 1
fi
