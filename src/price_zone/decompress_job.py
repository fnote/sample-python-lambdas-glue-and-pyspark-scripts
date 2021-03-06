"""
Price zone etl decompression job
"""
# pylint: disable=import-error,too-few-public-methods

import sys
import time

import smart_open
from awsglue.utils import getResolvedOptions


# 200MB min_upload_size


class Configuration:
    """
    configurations class
    """
    TRANSPORT_PARAMS = {"min_part_size": 200 * 1024 * 1024}


args = getResolvedOptions(sys.argv, ['s3_path', 'decompressed_file_path'])
inputFilePath = args['s3_path']
outputFilePath = args['decompressed_file_path']
print("Starting decompression of file %s at time %.7f to bucket %s" %
      (inputFilePath, time.time(), outputFilePath))
with smart_open.open(outputFilePath, 'w', transport_params=Configuration.TRANSPORT_PARAMS) as fout:
    for line in smart_open.open(inputFilePath, 'rb', encoding='utf8'):
        fout.write(line)
print("Completed decompression time %.7f" % time.time())
