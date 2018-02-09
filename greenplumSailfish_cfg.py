import sys
sys.dont_write_bytecode = True

# Operation
action = 'genS3externalTable'
''' Valid values are
genS3externalTable
'''

# Connection Configuration
host = 'localhost'
port = 6432
db = 'gpadmin'
user = 'gpadmin'

# S3 Info
s3_bucket = ''
s3_key = ''
s3_config = '/home/gpadmin/s3/s3.conf'

# Identify Target table
tbl = ''

# Naming convention for generated objects
ext_tbl_prefix = 'ext_'
ext_tbl_suffix = ''
view_prefix = 'vw_'
view_suffix = ''

# Permissions of generated objects
admin_role = ''
viewer_role = ''