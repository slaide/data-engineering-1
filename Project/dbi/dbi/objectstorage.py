import os, boto3, typing as tp

from werkzeug.datastructures import FileStorage
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.client import Config

_BUCKET_NAME_ENV=os.getenv("S3_BUCKET_NAME") ; assert _BUCKET_NAME_ENV is not None
BUCKET_NAME=_BUCKET_NAME_ENV

S3_HOSTNAME=os.getenv("S3_HOSTNAME") ; assert S3_HOSTNAME is not None
_S3_PORT_ENV=os.getenv("S3_PORT") ; assert _S3_PORT_ENV is not None
S3_PORT=int(_S3_PORT_ENV)
S3_ACCESS_KEY_ID=os.getenv("S3_ACCESS_KEY_ID") ; assert S3_ACCESS_KEY_ID is not None
S3_SECRET_ACCESS_KEY=os.getenv("S3_SECRET_ACCESS_KEY") ; assert S3_SECRET_ACCESS_KEY is not None

class S3Client:
    def __init__(self):
        self.session=boto3.session.Session(
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        )
        self.handle=self.session.client(
            "s3",
            endpoint_url=f'http://{S3_HOSTNAME}:{S3_PORT}',
            config=Config(signature_version='s3v4'),
            use_ssl=False, verify=False, # Disable SSL
        )
        assert self.handle is not None

    def ensureBucket(self,bucket_name:str, region=None)->bool:
        """
        ensure the bucket exists in the region, i.e. create it if it does not exist.
        If no region is specified, the bucket is created in the S3 default region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, False is bucket already existed
        """
        
        # Check if the bucket already exists
        try:
            self.handle.head_bucket(Bucket=bucket_name)
            return False
        except ClientError as e:
            assert "Error" in e.response
            assert "Code" in e.response["Error"]
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # The bucket does not exist, create it
                try:
                    if region is None:
                        self.handle.create_bucket(Bucket=bucket_name)
                    else:
                        location = {'LocationConstraint': region}
                        self.handle.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration=location, # type: ignore # the argument type is dict!
                        )

                    return True
                except ClientError as e:
                    print(f"Failed to create bucket: {e}")
                    raise RuntimeError(f"Failed to create bucket: {e}")
            else:
                print(f"Failed to check bucket existence: {e}")
                raise RuntimeError(f"Failed to check bucket existence: {e}")


    def uploadFile(
        self,
        file:tp.Union[str,FileStorage], 
        object_name:str,
        bucket_override:tp.Optional[str]=None, 
    )->bool:
        """
        Uploads a file to the specified S3 bucket on LocalStack

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified, file_name is used
        :return: True if file was uploaded, else False
        """

        bucket:str =bucket_override if bucket_override is not None else BUCKET_NAME
        self.ensureBucket(bucket)

        try:
            if isinstance(file, str):
                self.handle.upload_file(file, bucket, object_name)
            elif isinstance(file, FileStorage):
                self.handle.upload_fileobj(file.stream, bucket, object_name)
            else:
                raise ValueError(f"file must be either a string or a FileStorage object, not {type(file)}")
        except NoCredentialsError:
            print("Credentials not available")
            return False
        return True
    
    def downloadFile(self,
        object_name:str,
        local_filename:str,
        bucket_override:tp.Optional[str]=None,
    ):
        bucket:str=bucket_override if bucket_override is not None else BUCKET_NAME
        self.ensureBucket(bucket)
        self.handle.download_file(bucket, object_name, local_filename)

    def downloadFileObj(self,
        object_name:str,
        fileobj:tp.BinaryIO,
        bucket_override:tp.Optional[str]=None,
    ):
        bucket:str=bucket_override if bucket_override is not None else BUCKET_NAME
        self.ensureBucket(bucket)
        self.handle.download_fileobj(bucket, object_name, fileobj)
