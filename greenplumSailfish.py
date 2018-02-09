#!/usr/bin/python
from __future__ import print_function
from pygresql import pg
import datetime
import argparse
import os
import sys
import re
from argparse import RawTextHelpFormatter
import ntpath

sys.dont_write_bytecode = True

#############################################################################
#                        Greenplum Sailfish                                 #
#############################################################################
# *** NOTE ***                                                              #
#                                                                           #
# This script is provided as a field written utility and NOT supported      #
# by Pivotal.  You are free to alter the script in any way you see fit      #
# but if you find bugs in the script or have recommendations to improve     #
# it please send an email to the author listed below. By contacting the     #
# author, your input for fixes and changes can be made to the script        #
# and shared with others.                                                   #
#                                                                           #
# Author Email: lmugnano@pivotal.io                                         #
#                                                                           #
#############################################################################

# ---------------------------------------------------------------------------
# Global Variables
# ---------------------------------------------------------------------------
cfg = ''
con = ''
start_time = ''
log = None
location = ''

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------
def init():
    global cfg
    global con
    global start_time
    global location
    global args
    global log

    start_time = datetime.datetime.now().strftime("%y%m%d%H%M%S%f")
    log = open("./sailfish_" + str(start_time) + ".log", "w")

    #########################################################################
    #            Defines arguments to be parsed in command line             #
    #########################################################################
    parser = argparse.ArgumentParser(description='Greenplum S3 File viewer\nAuthor: Louis Mugnano\nEmail: lmugnano@pivotal.io\nThis script is provided as a field written utility and NOT supported by Pivotal. You are free to alter the script in any way you see fit but if you find bugs in the script or have recommendations to improve it please send an email to the author listed below. By contacting the author, your input for fixes and changes can be made to the script and shared with others.', formatter_class=RawTextHelpFormatter)
    parser.add_argument("-cfg", required=True, help="Specifies configuration file. If an error saying 'no module name .py found', be sure to remove the py extension when specifying the file.")
    parser.add_argument("-host", help="The host of the master node of Greenplum database. If no -host if specified the script will use the PGHOST environment variable if it is set")
    parser.add_argument("-p", type=int, help="-port : The port for the master node of Greenplum database. If no -port is specified the script will use the PGPORT environment variable if it is set")
    parser.add_argument("-db", help="The database being used. If no db is specified the script will use the PGDATABASE environment variable if it is set")
    parser.add_argument("-usr", help="User to log into the database under")
    parser.add_argument("-tbl", help="Set to the name of the target table you want to load data into in the format of <table_name>")
    parser.add_argument("-action", help="Sets the action to be completed")
    parser.add_argument("-s3_bucket", help="Sets the source S3 bucket of what's being loaded.")
    parser.add_argument("-s3_key", help="Sets the source S3 object key you want to view and/or load.")
    parser.add_argument("-s3_config", help="Set to the full path of the S3 Connector config file.")
    parser.add_argument("-ext_tbl_prefix", help="Prefix to use for the generated external table (Default 'ext_' if both ext_tbl_prefix and ext_tbl_suffix are not specified")
    parser.add_argument("-ext_tbl_suffix", help="Suffix to use for the generated external table.")
    parser.add_argument("-view_prefix", help="Prefix to use for the generated view (Default 'vw_' if both ext_view_prefix and ext_view_suffix are not specified")
    parser.add_argument("-view_suffix", help="Suffix to use for the generated view.")
    parser.add_argument("-admin_role", help="Role to grant admin rights to for generated objects.")
    parser.add_argument("-viewer_role", help="Role to grant select rights to for generated objects.")

    args = parser.parse_args()
    path = os.path.splitext(args.cfg )[0]

    if path:
        cfg = __import__(path)
    else:
        raise argparse.ArgumentTypeError( "You must specify a configuration file using the -cfg command following the load script. Make sure that you don't include the .py extention when specifying it and ensure that the config file is in the same directory as the gpLoad script. For more information, enter 'python gpLoad.py -h into the command line." )

    if args.host:
        cfg.host = args.host
    if args.p:
        cfg.port = args.p
    if args.db:
        cfg.db = args.db
    if args.usr:
        cfg.user = args.usr
    if args.tbl:
        cfg.tbl = args.tbl
    if args.action:
        cfg.action = args.action
    if args.s3_bucket:
        cfg.s3_bucket = args.s3_bucket
    if args.s3_key:
        cfg.s3_key = args.s3_key
    if args.s3_config:
        cfg.s3_config = args.s3_config
    if args.ext_tbl_prefix:
        cfg.ext_tbl_prefix = args.ext_tbl_prefix
    if args.ext_tbl_suffix:
        cfg.ext_tbl_suffix = args.ext_tbl_suffix
    if args.view_prefix:
        cfg.view_prefix = args.view_prefix
    if args.view_suffix:
        cfg.view_suffix = args.view_suffix
    if args.admin_role:
        cfg.admin_role = args.admin_role
    if args.viewer_role:
        cfg.viewer_role = args.viewer_role

    print ("-------------  CONNECTION CONFIGURATION  -----------------", file = log)
    print ("Host: " + (cfg.host), file = log)
    print ("Port: " + str(cfg.port), file = log)
    print ("Database: " + (cfg.db), file = log)
    print ("----------------------------------------------------------", file = log)

    try :
        con = pg.connect(dbname=cfg.db, host=cfg.host, port=cfg.port, user=cfg.user)
    except pg.Error as error_message :
        raise Exception("Connection Error: " + str(error_message))

# ---------------------------------------------------------------------------
# Function to print function and string to stdout
# ---------------------------------------------------------------------------
def log_it(func,str):
	print (func + ":\n    " + " ".join(str.split()) + "\n", file = log)

# ---------------------------------------------------------------------------
# Validate input
# ---------------------------------------------------------------------------
def validate_and_set_defaults():
    if cfg.action in ['genS3externalTable']:
        if cfg.s3_bucket == '':
            raise Exception("Required parms missing: -s3_bucket for -action " + cfg.action)
        if cfg.s3_key == '':
            raise Exception("Required parms missing: -s3_key for -action " + cfg.action)
        if cfg.s3_config == '':
            raise Exception("Required parms missing: -s3_config for -action " + cfg.action)
        if cfg.tbl == '':
            sep = '.'
            cfg.tbl = ntpath.basename(cfg.s3_key).split(sep,1)[0].lower()
        if cfg.ext_tbl_prefix == '' and cfg.ext_tbl_suffix == '':
            cfg.ext_tbl_prefix = 'ext_'
        if cfg.view_prefix == '' and cfg.view_suffix == '':
            cfg.view_prefix = 'vw_'

# ---------------------------------------------------------------------------
# Determine what the columns are for the external table based on the
# header row contents in the S3 object
# ---------------------------------------------------------------------------
def determine_headings():
    # Defines external table name
    ext_tbl = "s3_sandbox.ext_" + cfg.tbl + "_head"

    location = "'s3://s3.amazonaws.com/" + cfg.s3_bucket + '/' + cfg.s3_key + \
               " config=" + cfg.s3_config + "'"

    # Drops the external table if it already exists
    sql_str = "drop external table if exists " + ext_tbl 
    log_it("determine_headings-1", sql_str)
    query = con.query(sql_str)
                    
    # Creates the external table for getting the header row
    sql_str = "create external table " + ext_tbl + \
              " ( header text )" \
              " location (" + location + ") " \
              " format 'text'"                

    log_it("determine_headings-2", sql_str)
    query = con.query(sql_str)
    
    # Get heading columns (first row)
    sql_str = "select replace(unnest(header),'\"','') from (" + \
               "select string_to_array(header,',') as header " + \
               "from " + ext_tbl + " limit 1) a"
    
    log_it("determine_headings-3", sql_str)
    query = con.query(sql_str).getresult()
    
    headings=[]
    dummy_ctr = 1
    for i in query:
        if i[0] != '':
            headings.append(i[0])
        else:
            headings.append("dummy_" + str(dummy_ctr))
            dummy_ctr+=1

    # Drop the external table
    sql_str = "drop external table if exists " + ext_tbl
    log_it("determine_headings-4", sql_str)
    query = con.query(sql_str)
        
    return headings

# ---------------------------------------------------------------------------
# Grant permissions on the object
# ---------------------------------------------------------------------------
def grant_to_role(obj, permission, role):
    if role != '':
       sql_str = "grant " + permission + " on " + obj + " to " + role
       log_it("grant_to_role", sql_str)
       query = con.query(sql_str)

# ---------------------------------------------------------------------------
# Generate the external table for the S3 object
# ---------------------------------------------------------------------------
def create_ext_table(headings):
    # Defines external table name
    ext_tbl = "s3_sandbox."
    if cfg.ext_tbl_prefix != '':
        ext_tbl += cfg.ext_tbl_prefix
    ext_tbl += cfg.tbl
    if cfg.ext_tbl_suffix != '':
        ext_tbl += cfg.ext_tbl_suffix

    location = "'s3://s3.amazonaws.com/" + cfg.s3_bucket + '/' + cfg.s3_key + \
               " config=" + cfg.s3_config + "'"

    # Drops the external table if it already exists
    sql_str = "drop external table if exists " + ext_tbl + " cascade"
    log_it("create_ext_table-1", sql_str)
    query = con.query(sql_str)
    
    cols = ''
    for col in headings:
        col = col.lower().strip()
        col = re.sub(r"[^\w\s]", '_', col)
        col = re.sub(r"\s+", '_', col)
        cols += " " + col + " text,"
    cols = cols[:-1]

    # Creates the external table for getting the data
    sql_str = "create external table " + ext_tbl + \
              " ( " + cols + " )" \
              " location (" + location + ") " \
              " format 'csv' (header)"                

    log_it("create_ext_table-2", sql_str)
    query = con.query(sql_str)
    
    grant_to_role(ext_tbl, 'ALL', cfg.admin_role)
    return ext_tbl

# ---------------------------------------------------------------------------
# Generate and grant the view for access by users
# ---------------------------------------------------------------------------
def create_user_view(ext_tbl):
    # Defines view name
    vw_nm = "s3_sandbox."
    if cfg.view_prefix != '':
        vw_nm += cfg.view_prefix
    vw_nm += cfg.tbl
    if cfg.view_suffix != '':
        vw_nm += cfg.view_suffix

    # Drops the view if it already exists
    sql_str = "drop view if exists " + vw_nm
    log_it("create_user_view-1", sql_str)
    query = con.query(sql_str)
    
    # Creates the view on the external table
    sql_str = "create view " + vw_nm + " as " \
              "select * from " + ext_tbl

    log_it("create_user_view-2", sql_str)
    query = con.query(sql_str)
    
    grant_to_role(vw_nm, 'ALL', cfg.admin_role)
    grant_to_role(vw_nm, 'SELECT', cfg.viewer_role)
    return vw_nm
    
########################################################################
# 		 	                  MAINLINE                                 #
########################################################################
def main():

    try:
        init()
        validate_and_set_defaults()
    
        if cfg.action == 'genS3externalTable':
            print ("Generate External table for S3 Object", file = log)
            headings = determine_headings()
            if not headings:
                raise Exception("No headings found in " + cfg.s3_key + " in bucket " + cfg.s3_bucket)
            ext_tbl = create_ext_table(headings)
            vw_nm = create_user_view(ext_tbl)
            print (vw_nm)
            sys.exit(0)
        elif cfg.action == '':
            raise Exception("A value is required for action, double check your value in the cfg.py file")
        else:
            raise Exception("Error: Incorrect value for action, please check your value for action in cfg.py")
    except Exception as e:
        print ("Error: " + str(e), file = log)
        raise
        sys.exit(1)
        
if __name__=="__main__":
   main()
