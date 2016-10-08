import boto3

session = boto3.Session(profile_name='cil')
client = session.client('s3', endpoint_url='https://griffin-objstore.opensciencedatacloud.org')

# import boto3.s3.connection
# bucket_name = 'put your bucket name here!'
# gateway = 'griffin-objstore.opensciencedatacloud.org'

# conn = boto.connect_s3(
#         host = gateway,
#         profile_name = 
#         #is_secure=False,               # uncomment if you are not using ssl
#         calling_format = boto.s3.connection.OrdinaryCallingFormat(),
#         )

# ### list buckets::
# for bucket in conn.get_all_buckets():
#         print "{name}\t{created}".format(
#                 name = bucket.name,
#                 created = bucket.creation_date,
#         )