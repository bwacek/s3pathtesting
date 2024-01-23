from pprint import PrettyPrinter

import boto3
from s3path import S3Path, register_configuration_parameter

AWS_ENDPOINT_URL = "https://s3.us-west-004.backblazeb2.com"
BUCKET_NAME = "s3pathtesting"

pp = PrettyPrinter(indent=4)


def main():
    write_file_with_boto("boto(*)!.txt", "text")
    write_file_with_s3path("s3path(*)!.txt", "text")

    list_files_with_boto('/')
    list_files_with_s3path('/')


def write_file_with_boto(file_key, file_body):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL,
    )
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=file_body,
    )
    print(f"Wrote file {file_key} using Boto!")


def write_file_with_s3path(file_key, file_body):
    resource = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL
    )
    s3_path = S3Path.from_bucket_key(BUCKET_NAME, file_key)
    register_configuration_parameter(s3_path, resource=resource)
    with s3_path.open('w') as f:
        f.write(file_body)
    print(f"Wrote file {file_key} using S3Path!\n\n")


def list_files_with_boto(prefix):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL,
    )

    keys = []
    list_params = {
        'Bucket': BUCKET_NAME,
    }
    if prefix:
        list_params['Prefix']: prefix

    while True:
        resp = s3_client.list_objects_v2(**list_params)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            list_params['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    print(f'Boto file listing of {prefix}:\n{pp.pformat(keys)}\n')


def list_files_with_s3path(prefix):
    resource = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL
    )
    if prefix:
        s3_path = S3Path.from_bucket_key(BUCKET_NAME, prefix)
    else:
        s3_path = S3Path.from_bucket_key(BUCKET_NAME, '/')
    register_configuration_parameter(s3_path, resource=resource)

    keys = list(s3_path.glob('*'))

    print(f'S3Path file listing of {prefix}:')
    print(pp.pformat([x.name for x in keys]))


if __name__ == '__main__':
    main()
