#! /bin/bash

cd `dirname "$0"`
export HADOOP_HOME=/usr/local/hadoop/hadoop-release
export HIVE_HOME=/usr/local/hadoop/hive-0.8.1/
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH

main_pid=`ps -ef|grep "python acl_main.py"|sed '/grep/d'|awk '{print $2}'`
log_pid=`ps -ef|grep "python log_watcher.py"|sed '/grep/d'|awk '{print $2}'`
usercheck_pid=`ps -ef|grep "python user_check.py"|sed '/grep/d'|awk '{print $2}'`

if [ "x$main_pid" == "x" ] ; then
   nohup python acl_main.py >/dev/null 2>&1 &
else
   echo "acl_main.py @ $main_pid"
fi
if [ "x$log_pid" == "x" ] ; then
   nohup python log_watcher.py >/dev/null 2>&1 &
else
   echo "log_watcher.py @ $log_pid"
fi
if [ "x$usercheck_pid" == "x" ] ; then
   nohup python user_check.py >/dev/null 2>&1 &
else
   echo "user_check.py @ $usercheck_pid"
fi
