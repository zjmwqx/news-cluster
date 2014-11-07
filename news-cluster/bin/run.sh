#!/bin/bash
nohup python ../batch/batchClassify.py 2013 1 1 > batch.log 2>&1 &
#备份以前的crontab内容
crontab -l >../etc/crontab.bak
#把要加入的命令写入备份的文件后面
echo "0 2 * * * /datayes/bdb/news-cluster-ath/bin/run-single.sh > /datayes/bdb/news-cluster-ath/logs/tmp.log" >>../etc/crontab.bak
#导入文件
crontab ../etc/crontab.bak