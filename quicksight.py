# 쿼리 결과를 S3에 저장해서 Quicksight용으로 사용하기

import sys
import os
sys.path.append("/Users/chanwoo/git/quant-zipline/falcon/performance")
from connection import tunnel, getQueryEngine
import logging

bucket = 'daground-quant'
prefix = 'stage/cwl/quicksight/data'
S3_path = f's3://{bucket}/{prefix}'


logger = logging.getLogger('quicksight')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('quicksight.log')
sh = logging.StreamHandler()
fm = logging.Formatter("%(asctime)s %(message)s")
sh.setFormatter(fm)
logger.addHandler(fh)
logger.addHandler(sh)


# 1. Connection
# 2. query 결과 받기
# 3. S3에 저장

def getViewData(name, version):
    with open(f"./{version}/{name}.sql") as f:
        with tunnel() as server:
            sql = f.read()
            getQueryEngine(sql, server).to_parquet(S3_path + f"/{name}.csv")

def getAllView(version='v5'):
    for _, __, files in os.walk(f"./{version}"):
        for file in files:
            name = file.split('.sql')[0]
            if name in ['perf']: continue
            logger.info(f'{name} View 업데이트...')
            getViewData(name, version)
            logger.info(f'{name} View 업데이트 완료')
        break



if __name__ == "__main__":
    logger.info(f'Quicksight 업데이트 시작')
    getAllView()
    logger.info(f'Quicksight 업데이트 종료')
