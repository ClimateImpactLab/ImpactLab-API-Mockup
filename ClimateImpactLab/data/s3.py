import boto3
from botocore.exceptions import ClientError
from ..utils.exceptions import ConnectionError

class Griffin(object):
    def __init__(self):
        self._session = boto3.Session(profile_name='cil')
        self._client = self._session.client('s3', endpoint_url='https://griffin-objstore.opensciencedatacloud.org')
        self._s3 = self._session.resource('s3', endpoint_url='https://griffin-objstore.opensciencedatacloud.org')

    def download(self, bucket, object_name):
        try:
            return self._s3.Object(bucket, object_name).get()
        except ClientError:
            raise ConnectionError('Error downloading {}'.format(object_name))

    def upload(self, bucket, object_name, filepath):
        # try:
        self._s3.Object(bucket, object_name).put(Body=open(filepath, 'rb'))
        # except ClientError:
        #     raise ConnectionError('Error uploading {}'.format(object_name))

        