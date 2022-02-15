manifest = {
    "fileLocations": [
        {
            "URIPrefixes": [
                "s3://daground-quant/stage/cwl/quicksight/data",
            ]
        }
    ],
    "globalUploadSettings": {
        "format": "CSV",
        "delimiter": ",",
        "textqualifier": "'",
        "containsHeader": "true"
    }
}

import json
with open('./manifest.json', 'w') as f:
    json.dump(manifest, f)

import boto3
s3 = boto3.client('s3')
s3.upload_file("./manifest.json", 'daground-quant', "stage/cwl/quicksight/manifest.json")