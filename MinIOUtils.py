import minio
from minio.commonconfig import Tags
from Progress import Progress

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)


class MinIO:
    def __init__(self, 
            endpoint=os.getenv("MINIO_HOST"), 
            access_key=os.getenv("MINIO_ACCESS"),
            secret_key=os.getenv("MINIO_SECRET")
            ):

        minio_conf =  {
                'endpoint': endpoint,
                'access_key': access_key,
                'secret_key': secret_key,
                'secure': False
                }

        self.client = minio.Minio(**minio_conf)

    def _checkBucketExists(self, bucket: str):
        '''
        检查 bucket 是否存在
        '''
        if not self.client.bucket_exists(bucket):
            return False
        return True

    def getFile(self, bucket: str, obj_name):
        '''
        获取指定文件
        '''
        if not self._checkBucketExists(bucket):
            return None

        data = self.client.get_obj(bucket, obj_name)
        return data.data

    def getFileList(self, bucket: str):
        '''
        递归获取所有文件名
        '''
        objs = []
        if not self._checkBucketExists(bucket):
            return objs

        bucket_objs = self.client.list_objects(bucket, recursive=True)
        for obj in bucket_objs:
            if not obj.is_dir:
                objs.append(obj.object_name)

        return objs

    def getFile(self, bucket: str, fileName):
        '''
        获取指定文件内容
        '''
        if not self._checkBucketExists(bucket):
            return None

        try:
            res = self.client.get_object(bucket, fileName)
            return res.data
        finally:
            res.close()
            res.release_conn()

    def putFile(self, bucket: str, objName: str, filePath: str, tag=None):
        '''
        上传文件至 minio server
        call fput_object
        '''
        if not self._checkBucketExists(bucket):
            return None

        # fixme 
        # how to process with tags arguments?
        if tag:
            _tag = Tags(for_object=True)
            for _k in tag.keys():
                _tag[_k] = tag[_k]
            tag = _tag

        res = self.client.fput_object(
                bucket, objName, filePath, 
                #tags=tag,
                progress=Progress(),
                )
        return True
