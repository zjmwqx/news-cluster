#!/bin/bash
source /root/.bashrc
cd /datayes/bdb/news-cluster-ath/bin
nohup python ../batch/batchClassify.py >> ../logs/acc.log 2>&1 &
