#!/usr/bin/env bash
################################PROCESS_START HERE################################
#nohup sh /vertica_bq_import.sh -p /vertica_bq_import_env.sh -d vertica -f src_bq_import -r table1

#Checking Number of parameter

echo "Number of parameters passed: $#"

if [[ $# -eq 2 || $# -eq 4 || $# -eq 6 || $# -eq 8 || $# -eq 10 ]]
        then
        echo ""
else
        echo "Invalid Number of Parameter passed"
        echo "Please Pass: -p <Param file> (or) -p <Param file> -s <start date> -e <end date> (or) -p <Param file> -s <start date> -e <end date> -d <database> (or)  -p <Param file> -s <start date> -e <end date> -d <database> -f <process>"
        exit 1;
fi

#Parsing of Command Line Arguments
OPTIND=1                                # Sets initial value
        while getopts ":p:s:e:d:f:r:" OPTNS; do
                case "${OPTNS}" in
                p) _paramFile=${OPTARG} ;;
                s) _sDate=${OPTARG} ;;
                e) _eDate=${OPTARG}  ;;
                d) _db=${OPTARG} ;;
                f) _process=${OPTARG} ;;
                                r) _subProcess=${OPTARG} ;;

                *) echo "invalid command line argument"; exit 1;;
                esac
        done
        #******************* SECTION TO INCLUDE THE UNIX PARAMETER FILE OF THE PROCESS *********************#

source ${_paramFile} ${_db} ${_process} ${_subProcess}

copy_from_vertica_to_hadoop()
{
        sqoop_job_cnt=`sqoop job --list | grep $SQOOPJOBNAME | wc -l`
        if [ $sqoop_job_cnt -eq 1 ]; then
                sqoop job --exec $SQOOPJOBNAME > ${AUDIT_PATH}/${SCHEMA_NAME}.${TARGET_TABLE}_sqoop.out 2>&1
                if [ $? = 0 ]
                then
                        echo "Sqoop job $SQOOPJOBNAME is executed" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        echo "Sqoop import is successful to ${HDFS_PATH}" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        REC_CNT=`grep -o "Retrieved.* records." ${AUDIT_PATH}/${SCHEMA_NAME}.${TARGET_TABLE}_sqoop.out | sed 's/[^0-9]*//g'`
                        echo "Number of records imported: $REC_CNT" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log

                else
                        echo "Sqoop job is failed" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        echo "1" > ${AUDIT_PATH}/SQOOP_$current_sqoop_batch_id.txt
            echo "$current_sqoop_batch_id" > ${AUDIT_PATH}/${SQOOP_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ${HDFS_AUDIT_PATH}/
            rm -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt
                        exit 1
                fi
        else
                sqoop job --create $SQOOPJOBNAME -- import --connect $JDBC_CONNECTION_URL --username $TARGET_USERNAME  --password-file $TARGET_PASSWORD_FILE --driver com.vertica.jdbc.Driver --m $NUM_MAPPER --query "SELECT * FROM ${SCHEMA_NAME}.${TARGET_TABLE} where \$CONDITIONS" --incremental append --check-column $LASTVALUECOL --split-by $SPLITBYCOL  --target-dir ${HDFS_PATH} --fields-terminated-by '\t' --lines-terminated-by '\n' --escaped-by \\ --null-string '' --null-non-string ''

                echo "No sqoop job name $SQOOPJOBNAME is found hence Sqoop job $SQOOPJOBNAME is created and executing" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                sqoop job --exec $SQOOPJOBNAME > ${AUDIT_PATH}/${SCHEMA_NAME}.${TARGET_TABLE}_sqoop.out 2>&1
                if [ $? = 0 ]
                then
                        echo "Sqoop job $SQOOPJOBNAME is executed" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        echo "Sqoop import is successful to ${HDFS_PATH}" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        REC_CNT=`grep -o "Retrieved.* records." ${AUDIT_PATH}/${SCHEMA_NAME}.${TARGET_TABLE}_sqoop.out | sed 's/[^0-9]*//g'`
                        echo "Number of records imported: $REC_CNT" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                else
                        echo "Sqoop job is failed" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        echo "1" > ${AUDIT_PATH}/SQOOP_$current_sqoop_batch_id.txt
                        echo "$current_sqoop_batch_id" > ${AUDIT_PATH}/${SQOOP_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ${HDFS_AUDIT_PATH}/
                        rm -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt
                        exit 1
                fi
    fi
}

copy_from_hadoop_to_gcp()
{
        if hdfs dfs -test -e "${HDFS_PATH}/*"
        then
                hdfs dfs -mkdir ${GSBUCKET}
                echo "Cleaning the target directory ${GSBUCKET}" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                hdfs dfs -rm ${GSBUCKET}/*
                echo "Moving data from Hadoop ${HDFS_PATH} to Google Cloud Storage Bucket ${GSBUCKET}" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                if hdfs dfs -cp "${HDFS_PATH}/*" ${GSBUCKET}
                then
                        echo "2" > ${AUDIT_PATH}/SQOOP_$current_sqoop_batch_id.txt
                    echo "$current_sqoop_batch_id" > ${AUDIT_PATH}/${SQOOP_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ${HDFS_AUDIT_PATH}/
                    rm -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt

                        hdfs dfs -rm -r "${HDFS_PATH}/*"
                else
                        echo "Unable to move part files to GCP. Exiting...! please refer log" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                        echo "1" > ${AUDIT_PATH}/SQOOP_$current_sqoop_batch_id.txt
                        echo "$current_sqoop_batch_id" > ${AUDIT_PATH}/${SQOOP_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ${HDFS_AUDIT_PATH}/
                        rm -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt
                        exit 1
                fi
        else
                echo "Unable to find part files from sqoop. Exiting...! please refer log" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
                exit 0
        fi
}

invoke_dag()
{
        echo "Invoking ${COMPOSER_DAG} DAG" | tee -a ${LOGFILEPATH}/${COMPOSER_DAG}_`date "+%Y%m%d"`.log
        echo "python /opt/infoworks/ext_scripts/exttrig_gcpcomp/main.py ${COMPOSER_ENV} ${COMPOSER_DAG} ${ADOBE_DAG_CONFIG}" | tee -a ${LOGFILEPATH}/${COMPOSER_DAG}_`date "+%Y%m%d"`.log
        python /opt/infoworks/ext_scripts/exttrig_gcpcomp/main.py ${COMPOSER_ENV} ${COMPOSER_DAG} ${ADOBE_DAG_CONFIG}
        if [ $? -ne 0 ]
        then
                echo "${COMPOSER_DAG} DAG is failed. Process aborted." | tee -a ${LOGFILEPATH}/${COMPOSER_DAG}_`date "+%Y%m%d"`.log
                echo "1" > ${AUDIT_PATH}/DAG_$current_dag_batch_id.txt
        echo "$current_dag_batch_id" > ${AUDIT_PATH}/${DAG_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${DAG_BATCH_ID} ${HDFS_AUDIT_PATH}/
        rm -f ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt
                exit 1
        else
                echo "${COMPOSER_DAG} DAG is completed successfully" | tee -a ${LOGFILEPATH}/${COMPOSER_DAG}_`date "+%Y%m%d"`.log
                echo "2" > ${AUDIT_PATH}/DAG_$current_dag_batch_id.txt
        echo "$current_dag_batch_id" > ${AUDIT_PATH}/${DAG_BATCH_ID} | hadoop fs -put -f ${AUDIT_PATH}/${DAG_BATCH_ID} ${HDFS_AUDIT_PATH}/
        rm -f ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt
                exit 0
        fi
}

#******************* CHECKING FOR LOG FILE DIRECTORY *********************#

if [ -d ${LOGFILEPATH} ]
then
        echo "Log directory ${LOGFILEPATH} already exists"
else
        mkdir -p ${LOGFILEPATH}
        echo "Log directory ${LOGFILEPATH} has been created"
fi
#******************* CHECKING FOR LOG FILE DIRECTORY ENDED *************************#

if [ -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ] && [ -f ${AUDIT_PATH}/${DAG_BATCH_ID} ]; then

max_sqoop_batch_id=`hdfs dfs -cat ${HDFS_AUDIT_PATH}/${SQOOP_BATCH_ID}`
max_dag_batch_id=`hdfs dfs -cat ${HDFS_AUDIT_PATH}/${DAG_BATCH_ID}`
echo "max_sqoop_batch_id is: $max_sqoop_batch_id, max_dag_batch_id is: $max_dag_batch_id"

if [ -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt ] && [ -f ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt ]; then
	SQOOP_STATUS=`cat ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt`
	DAG_STATUS=`cat ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt`

	echo "SQOOP status is: $SQOOP_STATUS, DAG status is: $DAG_STATUS"

	if [ $SQOOP_STATUS -eq 2 ] && [ $DAG_STATUS -eq 2 ]; then
		echo "SQOOP and DAG are success for the last run"
		break
	elif [ $SQOOP_STATUS -eq 2 ] && [ $DAG_STATUS -ne 2 ]; then
		echo "SQOOP is success and DAG is failed"
		current_dag_batch_id=$(expr $max_dag_batch_id + 1)

		invoke_dag
	elif [ $SQOOP_STATUS -ne 2 ]; then
		echo "SQOOP for the previous run is failed"
		current_sqoop_batch_id=$(expr $max_sqoop_batch_id + 1)
		current_dag_batch_id=$(expr $max_dag_batch_id + 1)

		copy_from_vertica_to_hadoop
		copy_from_hadoop_to_gcp
		invoke_dag
		exit 0
	fi

	current_dag_batch_id=$(expr $max_dag_batch_id + 1)
	current_sqoop_batch_id=$(expr $max_sqoop_batch_id + 1)

	copy_from_vertica_to_hadoop
	copy_from_hadoop_to_gcp
	invoke_dag
else
	echo "either ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt or ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt does not exist" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	
fi

else
	echo "Either ${AUDIT_PATH}/SQOOP_BATCH_ID or ${AUDIT_PATH}/DAG_BATCH_ID does not exist, hence creating both" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	mkdir ${AUDIT_PATH} | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	`hdfs dfs -mkdir ${HDFS_AUDIT_PATH}` | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	`hdfs dfs -mkdir ${HDFS_PATH}` | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	touch ${AUDIT_PATH}/${SQOOP_BATCH_ID}
	touch ${AUDIT_PATH}/${DAG_BATCH_ID}
	touch ${AUDIT_PATH}/DAG_999.txt | echo "2" >${AUDIT_PATH}/DAG_999.txt
	touch ${AUDIT_PATH}/SQOOP_999.txt | echo "2" >${AUDIT_PATH}/SQOOP_999.txt
	echo "999" >${AUDIT_PATH}/${SQOOP_BATCH_ID} | hdfs dfs -put -f ${AUDIT_PATH}/${SQOOP_BATCH_ID} ${HDFS_AUDIT_PATH}
	echo "999" >${AUDIT_PATH}/${DAG_BATCH_ID} | hdfs dfs -put -f ${AUDIT_PATH}/${DAG_BATCH_ID} ${HDFS_AUDIT_PATH}
	
	max_sqoop_batch_id=`hdfs dfs -cat ${HDFS_AUDIT_PATH}/${SQOOP_BATCH_ID}`
	max_dag_batch_id=`hdfs dfs -cat ${HDFS_AUDIT_PATH}/${DAG_BATCH_ID}`
	echo "max_sqoop_batch_id is: $max_sqoop_batch_id, max_dag_batch_id is: $max_dag_batch_id"

if [ -f ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt ] && [ -f ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt ]; then
	SQOOP_STATUS=`cat ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt`
	DAG_STATUS=`cat ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt`

	echo "SQOOP status is: $SQOOP_STATUS, DAG status is: $DAG_STATUS"

	if [ $SQOOP_STATUS -eq 2 ] && [ $DAG_STATUS -eq 2 ]; then
		echo "SQOOP and DAG are success for the last run"
		break
	elif [ $SQOOP_STATUS -eq 2 ] && [ $DAG_STATUS -ne 2 ]; then
		echo "SQOOP is success and DAG is failed"
		current_dag_batch_id=$(expr $max_dag_batch_id + 1)

		invoke_dag
	elif [ $SQOOP_STATUS -ne 2 ]; then
		echo "SQOOP for the previous run is failed"
		current_sqoop_batch_id=$(expr $max_sqoop_batch_id + 1)
		current_dag_batch_id=$(expr $max_dag_batch_id + 1)

		copy_from_vertica_to_hadoop
		copy_from_hadoop_to_gcp
		invoke_dag
		exit 0
	fi

	current_dag_batch_id=$(expr $max_dag_batch_id + 1)
	current_sqoop_batch_id=$(expr $max_sqoop_batch_id + 1)

	copy_from_vertica_to_hadoop
	copy_from_hadoop_to_gcp
	invoke_dag
else
	echo "either ${AUDIT_PATH}/SQOOP_$max_sqoop_batch_id.txt or ${AUDIT_PATH}/DAG_$max_dag_batch_id.txt does not exist" | tee -a ${LOGFILEPATH}/${PROCESS_NAME}_`date "+%Y%m%d"`.log
	
fi
fi