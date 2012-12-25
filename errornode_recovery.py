from kazoo.client import KazooClient,KazooState
from kazoo.exceptions import NoNodeError
from conf import Config
import sys


def state_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        print 'connection lost from zk server'
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        print 'connection suspended from zk server'
    else:
        # Handle being connected/reconnected to Zookeeper
        print 'connected to zk server'

def usage():
    print 'errornode_recovery.py [nodename]'

if __name__ == '__main__':
    
    if len(sys.argv)!=2:
        usage()
    else:
        conf = Config().getconfig('acl-hive.cfg')
        
        server_list = conf['zk_hosts']
        zk_blacklist_path = conf['zk_blacklist_path']
        
        zk = KazooClient(server_list)
        zk.start()
        
        nodename = sys.argv[1]
        try:
            blacklist_str, stat = zk.get(zk_blacklist_path)
            blacklist = blacklist_str.split(',')
            print 'current blacklist: %s' %(blacklist)
            blacklist.remove(nodename)
            new_blacklist_str = ','.join(blacklist)
            zk.set(zk_blacklist_path,bytes(new_blacklist_str))
            
            print 'node :%s removed from blacklist.current blacklist: %s' %(nodename,new_blacklist_str)
            
        except NoNodeError:
            print 'no such node.'