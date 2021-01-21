#!/bin/bash

#################################################################################################
#   PROJECT :	DATA UTILITIES																	#
#  	PURPOSE	:	This programs is an example of how ingest an excel file from a local folder     #
# 				to snowflake																	#
#	DE		:	Joao Feck																		#
#  	SME		:				                                             						#
#  	Version	: 	1.0																	    		#		
#################################################################################################

##############Initialize variables##########################
prj_dir="$(dirname "$dir")"
DTS="date +%Y-%m-%d@%T"
PRGM="ingest_excel_into_snowflake"
SUBPRGM="Processing actuals and goals for pnl"
py_file="ingest_excel_into_snowflake.py"
config_file="file_parameters.PROPERTIES"
cur_date=$(date +"%Y%m%d")
time_stamp=$(date +"%H%M%S")
script_folder="scripts"
config_folder="config"
log_folder="logs"

############################################################

#####Create log directory(if doesnt exist) and file#########
log_path=${prj_dir}/${log_folder}
echo
if [ ! -d ${log_path} ]
then 
	mkdir ${log_path}
fi
logfile=${log_path}/${PRGM}_${cur_date}_${time_stamp}.log
############################################################

############################Job start and email notification#######################################
echo "****start of ${PRGM} @ `$DTS`" | tee  -a ${logfile}
###################################################################################################

########################Job Execution#############################
pyfile=${prj_dir}/${script_folder}/${py_file}
configfile=${prj_dir}/${config_folder}/${config_file}
echo "${PRGM} - ${SUBPRGM} started @ `$DTS`"  | tee  -a ${logfile}
python \
${pyfile}  \
${configfile}  \
>> ${logfile}

##################################################################

############################################Error Handling and email notification##################################################################
if [ $? -eq 0 ]
then
	echo -e "\t${PRGM}-${SUBPRGM} completed @ `$DTS`" | tee  -a ${logfile}
else
	echo -e "\t${PRGM}-${SUBPRGM} failed @ `$DTS`" | tee  -a ${logfile}
	exit 1
fi
###################################################################################################################################################

#################################Job completion and email notification###########################################
echo "****${PRGM} completed @ `$DTS`" | tee  -a ${logfile} 
#################################################################################################################
    
