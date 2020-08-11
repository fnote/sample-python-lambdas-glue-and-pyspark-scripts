from urllib.parse import urlparse

import boto3


# should be added to glue backup jobs as external scripts

def move_objects_with_prefix(source_bucket, source_prefix, destination_bucket, destination_prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

    print("started moving s3 objects matching prefix %s of bucket %s\n" % (source_prefix, source_bucket))

    matching_objects = []
    for page in pages:
        if page['KeyCount'] > 0:
            matching_objects.extend(page['Contents'])

    for matching_object in matching_objects:
        temp_key = matching_object['Key'].replace(source_prefix, '').lstrip('/')
        destination_path = destination_prefix + temp_key
        move_object_with_key(source_bucket, matching_object['Key'], destination_bucket, destination_path)

    if len(matching_objects) > 0:
        validate_copy_count(source_bucket, source_prefix, len(matching_objects), destination_bucket, destination_prefix)
        delete_directory(source_bucket=source_bucket, prefix=source_prefix)


def move_object_with_key(source_bucket, source_key, destination_bucket, destination_key):
    s3 = boto3.resource('s3')

    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    copy_bucket = s3.Bucket(destination_bucket)

    print("Started moving s3 object from bucket %s with key %s to bucket %s with key %s\n" % (
        source_bucket, source_key, destination_bucket, destination_key))
    copy_bucket.copy(copy_source, destination_key)
    print("Completed moving s3 object from bucket %s with key %s to bucket %s with key %s\n" % (
        source_bucket, source_key, destination_bucket, destination_key))


def move_input_file(source_path, destination_bucket, destination_path_prefix):
    parsed_path = urlparse(source_path, allow_fragments=False)
    source_bucket = parsed_path.netloc
    source_key = parsed_path.path.lstrip('/')
    destination_key = destination_path_prefix + parsed_path.path.split('/')[-1]

    move_object_with_key(source_bucket, source_key, destination_bucket, destination_key)

    validate_copy(destination_bucket, destination_key)

    delete_object(source_bucket, source_key)


def validate_copy(bucket, key):
    print("Validating objects exists in bucket %s with key %s \n" % (bucket, key))
    s3 = boto3.client('s3')
    # will throw an error if the object doesn't exists
    s3.head_object(Bucket=bucket, Key=key)


def validate_copy_count(source_bucket, source_prefix, source_object_count, destination_bucket, destination_prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    copied_pages = paginator.paginate(Bucket=destination_bucket, Prefix=destination_prefix)

    copied_objects = []
    for page in copied_pages:
        if page['KeyCount'] > 0:
            copied_objects.extend(page['Contents'])
    if len(copied_objects) != source_object_count:
        raise ValueError("Source bucket's: %s matching prefix: %s count: %s and destination bucket's: %s  matching "
                         "prefix: %s count %s doesn't match\n" % (
                             source_bucket, source_prefix, source_object_count, destination_bucket,
                             destination_prefix,
                             len(copied_objects)))


def delete_directory(source_bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    source_pages = paginator.paginate(Bucket=source_bucket, Prefix=prefix)

    print("started deleting s3 objects matching prefix: %s of bucket: %s\n" % (prefix, source_bucket))

    for page in source_pages:
        del_object_list = []
        for delete_file in page['Contents']:  # delete 1000 objects at max at once
            del_object = {
                'Key': delete_file['Key']
            }
            del_object_list.append(del_object)
        del_object = {
            'Objects': del_object_list
        }
        s3.delete_objects(Bucket=source_bucket, Delete=del_object)
        print("Successfully deleted s3 objects matching prefix: %s of bucket: %s\n. "
              "Deleted content: %s" % (prefix, source_bucket, del_object_list))


def delete_object(source_bucket, source_key):
    s3 = boto3.client('s3')
    print("Deleting object in bucket %s with key %s\n" % (source_bucket, source_key))
    s3.delete_object(Bucket=source_bucket, Key=source_key)
    print("Successfully deleted object in bucket %s with key %s\n" % (source_bucket, source_key))
