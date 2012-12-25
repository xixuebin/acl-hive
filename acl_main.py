#!/usr/bin/env python
'''
Created on Oct 11, 2012

@author: hadoop
'''
from kazoo.client import KazooClient,KazooState
from acl_handler import ACLHandler
from conf import Config
import logging, logging.handlers
import sys,os,time



def state_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        log.info('connection lost from zk server')
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        log.info('connection suspended from zk server')
    else:
        # Handle being connected/reconnected to Zookeeper
        log.info('connected to zk server')
        
def child_watch(event):
    log.info( 'trigger child watch')
    handle(aclhandler,zk,task_path)

def handle(aclhandler,zk,task_path,blacklist_path):
    children = zk.get_children(task_path)
    if len(children) !=0:
        
        children_str = ','.join(children)
        all_tasks = children_str.split(',')
        all_tasks.sort()
        log.debug('find %s tasks with names: %s' % (len(all_tasks), all_tasks))
        for node in all_tasks:
            aclhandler.handle_hive_acl(zk,task_path,node)


if __name__ == '__main__':
    
#    logging.basicConfig(filename = os.path.join(os.getcwd(), 'log.txt'), filemode = 'a',level = logging.INFO, format = '%(asctime)s - %(levelname)s: %(message)s') 
    logging.basicConfig(stream = sys.stdout,level = logging.DEBUG, format = '%(asctime)s [%(module)s] [%(funcName)s] [%(levelname)s] %(message)s') 
    
    log = logging.getLogger('ACL-Hive')
    log.setLevel(logging.DEBUG)
    
    filehandler = logging.handlers.TimedRotatingFileHandler("logs/acl-hive.log", 'midnight', 1, 0)
    filehandler.suffix = "%Y%m%d"
    formatter = logging.Formatter('%(asctime)s [%(module)s] [%(funcName)s] [%(levelname)s] %(message)s')
    filehandler.setFormatter(formatter)
    log.addHandler(filehandler)
    
    conf = Config().getconfig('acl-hive.cfg')
    log.info('hive-acl started. conf: \n%s' %(conf))
    
    server_list = conf['zk_hosts']
    task_path = conf['zk_task_path']
    blacklist_path = conf['zk_blacklist_path']
    
    zk = KazooClient(server_list)
    zk.add_listener(state_listener)
    zk.start()
#    zk.stop()
    zk.ensure_path(task_path)
    zk.ensure_path(blacklist_path)
    
    aclhandler = ACLHandler(zk,conf)
    
    while True :
        handle(aclhandler,zk,task_path,blacklist_path)
        time.sleep(30)
