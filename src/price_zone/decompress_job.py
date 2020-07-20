import sys
import time
import smart_open
from awsglue.utils import getResolvedOptions
# 200MB min_upload_size


class Configuration:
    TRANSPORT_PARAMS = {"min_part_size": 200 * 1024 * 1024}
    OUTPUT_FILE_PATH = "s3://cp-ref-price-poc-bucket/eats_input/decompress.csv"


args = getResolvedOptions(sys.argv, ['s3_path'])
inputFilePath = args['s3_path']

print("starting decompression time %.7f" % time.time())
with smart_open.open(Configuration.OUTPUT_FILE_PATH, 'w', transport_params=Configuration.TRANSPORT_PARAMS) as fout:
    for line in smart_open.open(inputFilePath, 'rb', encoding='utf8'):
        fout.write(line)
print("completed decompression time %.7f" % time.time())
