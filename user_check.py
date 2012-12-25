from ldap_admin import LDAPAdmin
from kazoo.client import *
from conf import Config
import sys,time,logging,logging.handlers

logging.basicConfig(stream = sys.stdout,level = logging.INFO, format = '%(asctime)s [%(module)s] [%(funcName)s] [%(levelname)s] %(message)s') 
log = logging.getLogger('User-Check')
log.setLevel(logging.INFO)
    
filehandler = logging.handlers.TimedRotatingFileHandler("logs/user-check.log", 'midnight', 1, 0)
filehandler.suffix = "%Y%m%d"
formatter = logging.Formatter('%(asctime)s [%(module)s] [%(funcName)s] [%(levelname)s] %(message)s')
filehandler.setFormatter(formatter)
log.addHandler(filehandler)

ZK_USERPATH='/test/Hive'

class UserChecker():
    def __init__(self,conf):
        self.lDAPAdmin = LDAPAdmin(conf['ldap_host'],conf['ldap_base_dn'],conf['ldap_mgr_cred'],conf['ldap_mgr_passwd'])
        self.zk = KazooClient(conf['zk_hosts'])
        self.zk.start()
        self.zk.ensure_path(ZK_USERPATH)

    
    def list_ldap_all_users(self):
        user_list = self.lDAPAdmin.list_entrys('objectClass=posixAccount', attrib=['sn'])
        all_user = [user[1]['sn'][0] for user in user_list]
        
        return all_user
    
    
    def get_zk_all_users(self):
        try:
            data,stat = self.zk.get(ZK_USERPATH)
            return data
        except NoNodeError:
            return None
    def save2zk(self,data):
        self.zk.set(ZK_USERPATH, data)
    
if __name__ =='__main__':
    conf = Config().getconfig('acl-hive.cfg')
    user_checker = UserChecker(conf)
    
    userlist_in_zknode = user_checker.get_zk_all_users()
    log.debug('all current users in zknode: %s' %(userlist_in_zknode))
    
    while True:
        
        userlist_in_ldap = user_checker.list_ldap_all_users()
        log.debug('all current users in ldap: %s' %(userlist_in_ldap))
    
        #userlist_in_ldap is a list, while userlist_in_zknode is a string
        # convert userlist_in_ldap to str, then compare them. if not equal, save userlist_in_ldap to zknode
        if userlist_in_zknode != str(userlist_in_ldap):
            log.info('user in ldap and zknode are not equal , save latest user list in ldap to zk node')
            user_checker.save2zk(bytes(userlist_in_ldap))
            userlist_in_zknode = str(userlist_in_ldap)
        else:
            log.debug('user in ldap and zknode are equal , user list in zk node is latest')
        
        time.sleep(60)
