from io import BytesIO

import boto3
import botocore
import hashlib
import logging
import os
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlithree.config import settings
from hashlib import md5

def _get_md5(file_bytes: bytes) -> str:
    """Given bytes, calculate their md5 hash"""
    m = md5()
    m.update(file_bytes)
    return m.hexdigest()

def _get_bytes(filename: str) -> bytes:
    """Read file as bytes"""
    with open(filename, "rb") as f:
        return f.read()

class SQLithreeDialect(SQLiteDialect_pysqlite):

    def load_remote_db(self, dbname=None):
        """
        Load remote S3 DB
        This code is still pretty ugly, but it seems to work, and remains fairly similar to
        # https://github.com/Miserlou/zappa-django-utils/blob/master/zappa_django_utils/db/backends/s3sqlite/base.py
        """

        # user set a simple relative path in SQLALCHEMY_DATABASE_URI, but it was changed to an abspath
        self._local_dbname = os.path.relpath(dbname)
        self.db_hash = None

        bucketname = settings.sqlithree_bucket_name
        self._remote_dbname = self._local_dbname
        if '/tmp/' not in self._local_dbname:
            local_file_path = '/tmp/' + self._local_dbname

            if os.path.isfile(local_file_path):
                current_md5 = _get_md5(_get_bytes(local_file_path))
            else:
                current_md5 = ""

            try:
                # etag = ''
                # if os.path.isfile('/tmp/' + self._local_dbname):
                #     m = hashlib.md5()
                #     with open('/tmp/' + self._local_dbname, 'rb') as f:
                #         m.update(f.read())

                #     # In general the ETag is the md5 of the file, in some cases it's not,
                #     # and in that case we will just need to reload the file, I don't see any other way
                #     etag = m.hexdigest()

                # signature_version = "s3v4"
                # s3 = boto3.resource(
                #     's3',
                #     config=botocore.client.Config(signature_version=signature_version),
                # )
                # obj = s3.Object(bucketname, self._local_dbname)
                # obj_bytes = obj.get(IfNoneMatch=etag)["Body"]  # Will throw E on 304 or 404

                # with open('/tmp/' + self._local_dbname, 'wb') as f:
                #     f.write(obj_bytes.read())

                # m = hashlib.md5()
                # with open('/tmp/' + self._local_dbname, 'rb') as f:
                #     m.update(f.read())

                # self.db_hash = m.hexdigest()

                # In general the ETag is the md5 of the file, in some cases it's
                # not, and in that case we will just need to reload the file,
                # I don't see any other way

                signature_version = "s3v4"
                s3 = boto3.resource(
                    's3',
                    config=botocore.client.Config(signature_version=signature_version),
                )

                obj_bytes = s3.Object(
                    bucketname, self._local_dbname,
                ).get(IfNoneMatch=current_md5,)[
                    "Body"
                ]  # Will throw E on 304 or 404

                # Remote does not match local. Replace local copy.
                with open(local_file_path, "wb") as f:
                    file_bytes = obj_bytes.read()
                    self.db_hash = _get_md5(file_bytes)
                    f.write(file_bytes)

                logging.debug("Loaded remote DB!")
            except botocore.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '304':
                    logging.debug("ETag matches md5 of local copy, using local copy of DB!")
                    self.db_hash = current_md5
                else:
                    logging.debug("Couldn't load remote DB object.")
            except Exception as e:
                # Weird one
                logging.debug(e)


        if '/tmp/' not in self._local_dbname:
            self._local_dbname = '/tmp/' + self._local_dbname

        return self._local_dbname

    def close(self, *args, **kwargs):
        """
        Engine closed, copy file to DB if it has changed
        """

        bucketname = os.environ.get('S3SQLite_bucket')
        try:
            with open(self._local_dbname, 'rb') as f:
                fb = f.read()

                m = hashlib.md5()
                m.update(fb)
                if self.db_hash == m.hexdigest():
                    logging.debug("Database unchanged, not saving to remote DB!")
                    return

                bytesIO = BytesIO()
                bytesIO.write(fb)
                bytesIO.seek(0)

                signature_version = "s3v4"
                s3 = boto3.resource(
                    's3',
                    config=botocore.client.Config(signature_version=signature_version),
                )

                s3_object = s3.Object(bucketname, self._remote_dbname)
                result = s3_object.put(Body=bytesIO)
                logging.debug("Saved to remote DB!")
        except Exception as e:
            logging.debug(e)




    def __init__(self, *args, **kw):
        #print("S3SQLiteDialect.__init__(args=%s, kw=%s)" % (args, kw))
        super(SQLithreeDialect, self).__init__(*args, **kw)
    def connect(self, *args, **kw):
        #print("S3SQLiteDialect.connect(args=%s, kw=%s)" % (args, kw))
        localname = self.load_remote_db(dbname=args[0])
        return super(SQLithreeDialect, self).connect(localname, **kw)
    def do_close(self, *args, **kw):
        #print("S3SQLiteDialect.do_close(args=%s, kw=%s)" % (args, kw))
        out = super(SQLithreeDialect, self).do_close(*args, **kw)
        self.close()
        return out
