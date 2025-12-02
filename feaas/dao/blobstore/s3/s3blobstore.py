import boto3
from botocore.client import Config
import botocore
from botocore.exceptions import ClientError
from feaas.dao.blobstore import abstract_blobstore as blobstore
from feaas.util import common
from collections import namedtuple
import fnmatch
import logging
from operator import attrgetter
import os

log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))


S3Obj = namedtuple('S3Obj', ['key', 'mtime', 'size', 'ETag'])


class S3Blobstore(blobstore.AbstractBlobstore):

    def __init__(self, bucket_name, access_key=None, secret_key=None, region='us-east-1'):
        super().__init__(bucket_name)
        if access_key is None:
            self.client = boto3.client('s3', config=Config(
                signature_version='s3v4'), region_name=region)
            self.resource = boto3.resource('s3', region_name=region)
        else:
            self.client = boto3.client('s3', aws_access_key_id=access_key,  aws_secret_access_key=secret_key, config=Config(
                signature_version='s3v4'), region_name=region)
            self.resource = boto3.resource(
                's3', aws_access_key_id=access_key,  aws_secret_access_key=secret_key, region_name=region)
        self.bucket_name = bucket_name

    def save_blob(self, key, contents, metadata=None, public=False, content_type='binary/octet-stream'):
        if contents is None:
            log.error(f'Recieved None contents for {key}')
            return
        log.debug(
            f'S3Blobstore save {key} of type {type(contents)} and length {len(contents)}')
        if metadata is not None and 'bucket_name' in metadata:
            bucket_name = metadata['bucket_name']
            del metadata['bucket_name']
        else:
            bucket_name = self.bucket_name
        kwargs = {
            'Bucket': bucket_name,
            'Key': key,
            'Body': contents,
            'ContentType': content_type
        }
        if metadata is not None:
            for k in metadata.keys():
                v = metadata[k]
                if type(v) != str:
                    metadata[k] = str(v)
        if public:
            kwargs['ACL'] = 'public-read'
        if metadata is not None:
            kwargs['Metadata'] = metadata
        r = self.client.put_object(**kwargs)
        if self.exists(f'{key}.waiting'):
            self.delete_blob(f'{key}.waiting')
        return r

    def get_blob_attribute(self, key, attribute):
        o = self.resource.Object(bucket_name=self.bucket_name, key=key)
        return o.metadata[attribute]

    def get_url_from_key(self, key):
        i = key.rfind('.')
        return '{}/{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name, key[:i])

    def get_key_from_url(self, url):
        i = len('{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name))
        return url[i:]

    def delete_blob(self, key):
        log.debug(f'S3Blobstore delete {key}')
        r = self.client.delete_object(Bucket=self.bucket_name, Key=key)
        # {'ResponseMetadata': {'RequestId': 'D5EB64638995D2EE', 'HostId': 'D8U8j+ZVoUOAR8RbndRcgBxVGkb0qosqQsZcnXzfhjRdr8U/2HVXJN0ToSqlrE7XA7GbKgehDpc=', 'HTTPStatusCode': 204, 'HTTPHeaders': {'x-amz-id-2': 'D8U8j+ZVoUOAR8RbndRcgBxVGkb0qosqQsZcnXzfhjRdr8U/2HVXJN0ToSqlrE7XA7GbKgehDpc=', 'x-amz-request-id': 'D5EB64638995D2EE', 'date': 'Wed, 13 Nov 2019 22:51:12 GMT', 'server': 'AmazonS3'}, 'RetryAttempts': 1}}
        return r

    def get_blob(self, key):
        b, _ = self.get_blob_with_content_type(key)
        return b

    def get_blob_with_content_type(self, key):
        try:
            b = self.bucket_name
            resp = self.client.get_object(Bucket=b, Key=str(key))
            m = {'content_type': resp['ContentType']}
            body = resp['Body'].read()
            log.debug(f'S3Blobstore got {key} of type {type(body)} and length {len(body)}')
            return body, m
        except ClientError as ex:
            log.debug(f'S3Blobstore ERROR getting {key}')
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return None, None
            else:
                log.error(ex)
                raise ex

    def move(self, src_key: str, dest_key: str):
        print("MOVE", src_key, '->', dest_key)
        copy_source = {'Bucket': self.bucket_name, 'Key': src_key}
        self.client.copy_object(CopySource=copy_source,
                                Bucket=self.bucket_name, Key=dest_key)
        self.client.delete_object(Bucket=self.bucket_name, Key=src_key)

    def copy(self, src_key: str, dest_key: str, metadata_directive: str = 'COPY'):
        print("COPY", src_key, '->', dest_key,
              "with metadata_directive", metadata_directive)
        copy_source = {'Bucket': self.bucket_name, 'Key': src_key}
        self.client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=dest_key,
                                MetadataDirective=metadata_directive)

    def update_metadata(self, key: str, metadata: dict, metadata_directive: str = 'REPLACE'):
        """
        despite metadata directive, this method adds new metadata, updates existing metadata,
        but does not remove existing metadata
        """
        k = self.client.head_object(Bucket=self.bucket_name, Key=key)
        existing_m = k.get("Metadata", {})
        updated_m = {**existing_m, **metadata}
        copy_source = {'Bucket': self.bucket_name, 'Key': key}
        r = self.client.copy_object(CopySource=copy_source, Bucket=self.bucket_name, Key=key,
                                    Metadata=updated_m, MetadataDirective=metadata_directive)
        return r

    def update_object_tags(self, key: str, new_tags: dict, update: bool = True):
        """
        Add/Update/Overwrite tags to AWS S3 Object
        from https://stackoverflow.com/a/56276313
        """
        old_tags = {}
        if update:
            old = self.client.get_object_tagging(
                Bucket=self.bucket_name,
                Key=key,
            )
            old_tags = {i['Key']: i['Value'] for i in old['TagSet']}
        new_tags = {**old_tags, **new_tags}
        response = self.client.put_object_tagging(
            Bucket=self.bucket_name,
            Key=key,
            Tagging={
                'TagSet': [{'Key': str(k), 'Value': str(v)} for k, v in new_tags.items()]
            }
        )
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def set_acl(self, key, acl):
        response = self.client.put_object_acl(
            ACL=acl, Bucket=self.bucket_name, Key=key)
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def set_public_acl(self, key, is_public):
        if is_public:
            acl = 'public-read'
        else:
            acl = 'private'
        return self.set_acl(key, acl)

    def get_blob_tags(self, key):
        resp = self.client.get_object_tagging(
            Bucket=self.bucket_name,
            Key=key,
        )
        tags = {i['Key']: i['Value'] for i in resp['TagSet']}
        return tags

    def ls(self, prefix, suffix):
        return self._get_matching_s3_keys(prefix, suffix)

    def get_blob_metadata(self, key):
        try:
            r = self.client.head_object(Bucket=self.bucket_name, Key=key)
            if 'Metadata' in r:
                result = r['Metadata']
            else:
                result = {}
        except:
            # TODO: handle exceptions more specifically
            return None
        result['status_code'] = r['ResponseMetadata']['HTTPStatusCode']
        result['last_modified'] = r['LastModified']
        result['content_length'] = r['ContentLength']
        return result

    def exists(self, key):
        try:
            self.resource.Object(self.bucket_name, key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                log.error(e)
                # Something else has gone wrong.
                raise

    def get_keys_matching_pattern(self, pattern='*.crawl', limit=1000):
        arr = pattern.split('*')
        if len(arr) == 1:
            m = self.get_blob_metadata(pattern)
            if m is None:
                return []
            else:
                return [pattern]
        r = list(self._get_matching_s3_keys(
            prefix=arr[0], suffix=arr[-1], limit=limit))
        return r

    def _get_matching_s3_keys(self, prefix='', suffix='', limit=1000):
        """
        Generate the keys in an S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': self.bucket_name, 'Prefix': prefix}
        c = 0
        while c < limit:
            resp = self.client.list_objects_v2(**kwargs)
            if 'Contents' in resp:
                for obj in resp['Contents']:
                    key = obj['Key']
                    if suffix is None or suffix == '' or key.endswith(suffix):
                        c += 1
                        yield key

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break


    def generate_presigned_url(self, key, ttl=1000):
        d = {'Bucket': self.bucket_name, 'Key': key}
        return self.client.generate_presigned_url('get_object', Params=d, ExpiresIn=ttl)


    def s3list(self, path, start=None, end=None, recursive=True, list_dirs=True, list_objs=True, limit=None, match_pattern=None):
        bucket = self.resource.Bucket(self.bucket_name)
        kwargs = dict()
        if start is not None:
            if not start.startswith(path):
                start = os.path.join(path, start)
            # note: need to use a string just smaller than start, because
            # the list_object API specifies that start is excluded (the first
            # result is *after* start).
            kwargs.update(Marker=self.__prev_str(start))
        if end is not None:
            if not end.startswith(path):
                end = os.path.join(path, end)
        if not recursive:
            kwargs.update(Delimiter='/')
            if not path.endswith('/'):
                path += '/'
        kwargs.update(Prefix=path)
        if limit is not None:
            kwargs.update(PaginationConfig={'MaxItems': limit})

        paginator = bucket.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=bucket.name, **kwargs):
            q = []
            if 'CommonPrefixes' in resp and list_dirs:
                q = [S3Obj(f['Prefix'], None, None, None)
                     for f in resp['CommonPrefixes']]
            if 'Contents' in resp and list_objs:
                q += [S3Obj(f['Key'], f['LastModified'], f['Size'], f['ETag'])
                      for f in resp['Contents']]
            # note: even with sorted lists, it is faster to sort(a+b)
            # than heapq.merge(a, b) at least up to 10K elements in each list
            q = sorted(q, key=attrgetter('key'))
            if limit is not None:
                q = q[:limit]
                limit -= len(q)
            for p in q:
                if match_pattern is not None:
                    if not fnmatch.fnmatch(p.key, match_pattern):
                        continue
                if end is not None and p.key >= end:
                    return
                yield p


    def __prev_str(self, s):
        if len(s) == 0:
            return s
        s, c = s[:-1], ord(s[-1])
        if c > 0:
            s += chr(c - 1)
        s += ''.join(['\u7FFF' for _ in range(10)])
        return s

    def delete_prefix(self, src_prefix):
        if not(src_prefix.endswith('/')):
            src_prefix += '/'
        bucket = self.resource.Bucket(self.bucket_name)
        r = bucket.objects.filter(Prefix=src_prefix).delete()
        num_deleted = 0
        for item in r:
            num_deleted += len(item['Deleted'])
        return num_deleted

    def local_cache_key(self, key, dest='/tmp/'):
        b = self.get_blob(key)
        if b is None:
            return None
        fn = common.get_filename_from_key(key)
        try:
            os.makedirs(dest)
        except:
            pass
        absfn = dest + fn
        f = open(absfn, 'wb')
        f.write(b)
        f.close()
        return absfn
