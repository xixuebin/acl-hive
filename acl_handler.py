
from krb5_admin import Krb5Admin
from ldap_admin import LDAPAdmin
from acl_parser import HiveAclParser
from hive_priv import *
import sys,os,commands,logging,ldap,time

log = logging.getLogger('ACL-Hive')
ACL_OU = 'acl'
DEFAULT_DATABASE = 'bi.'

class ACLHandler(object):
    
    def __init__(self,zk,conf):
        self.conf = conf
        self.krb5admin = Krb5Admin(conf['kdc_host'], conf['kdc_ssh_port'], conf['kdc_ssh_username'], conf['kdc_ssh_passwd'])
        self.lDAPAdmin = LDAPAdmin(conf['ldap_host'],conf['ldap_base_dn'],conf['ldap_mgr_cred'],conf['ldap_mgr_passwd'])
        self.blacklist_path = conf['zk_blacklist_path']
        blacklist_str, stat = zk.get(self.blacklist_path)
        self.blacklist = set(blacklist_str.split(','))
        log.info('ACLHandler initialized. blacklist: %s' %(self.blacklist))
    def handle_hive_acl(self,zk,task_path,node):
        blacklist_str, stat = zk.get(self.blacklist_path)
        self.blacklist = set(blacklist_str.split(','))
        if node not in self.blacklist:
            node_fullpath = task_path+'/'+str(node)
            data, stat = zk.get(node_fullpath)
            log.info('handle hive acl node: %s .' %(node_fullpath))
            log.debug('acl node: %s, data: %s' %(node_fullpath,data))
            try:
                parser = HiveAclParser(data)
                storage_type = parser.get_storage_type()
                if storage_type == 'hive':
                    msg = parser.get_msg()
                    for action in msg:
                        if action['type'] == 0:
                            self.add_user(action['user_name'],action['ou'],action['gidnumber'],action['passwd'])
                        elif action['type'] == 1:
                            self.delete_user(action['user_name'],action['ou'])
                        elif action['type'] == 2:
                            self.update_user(action['user_name'],action['passwd'])
                        elif action['type'] == 6:
                            self.grant_privileges_to_user(action['user_name'],action['high_group'],action['table_ids'])
                        elif action['type'] == 7:
                            self.revoke_privileges_to_user(action['user_name'],action['table_ids'])
                        else:
                            log.error('acl msg error, cannot determine action type.')
                            raise Exception('acl msg error, cannot determine action type.')
                    
                    zk.delete(node_fullpath)
                    log.debug('node: %s deleted. ' %(node_fullpath))
                else:
                    log.error('storage type error. acl msg is not for hive.')
                    raise Exception('storage type error. acl msg is not for hive.')
            except Exception,e:
                log.error('error task node: %s, %s' %(node, e))
                self.blacklist.add(node)
                blacklist_str = ','.join(self.blacklist)
                zk.set(self.blacklist_path,bytes(blacklist_str))
                log.info('add task node %s to blacklist' %(node))
        else:
            log.debug('node: %s is in blacklist.' %(node))
        
    def _generate_uidNumber(self):
        res_list = self.lDAPAdmin.list_entrys(attrib=['uidNumber'])
        uidNumber_list = []
        for l in res_list:
            uidNumber_list.append(l[1]['uidNumber'])
            
        uidNumber_list.sort()
        current_max_uid_Number = uidNumber_list.pop()[0]
        
        uidNumber = int(current_max_uid_Number)+1
        return uidNumber
    
    def add_user(self,user_name,user_ou,user_gidNumber,passwd):
        
        log.info('adding user :%s ' %(user_name))
        
        try:
            self.lDAPAdmin.add_newou(user_ou)
            self.lDAPAdmin.add_newgroup(user_ou, user_ou, user_gidNumber)
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
        
        try:
            #generate user info
            uidNumber = self._generate_uidNumber()
            gidNumber = user_gidNumber
            sn = user_name
            uid = user_name
            homeDirectory = '/data/home/'+user_name
            loginShell = '/bin/dpsh'
            
            entry_info = {'sn': str(sn),'uid': str(uid),'uidNumber':str(uidNumber),'gidNumber':str(gidNumber),'homeDirectory': str(homeDirectory),'loginShell': str(loginShell),'objectClass': ['inetOrgPerson','posixAccount','top'] }
            
            #add user info to ldap
            self.lDAPAdmin.add_entry(user_name, user_ou, entry_info)
            
            #add user info to kdc
            self.krb5admin.add_principal(user_name, self.conf['kdc_realm'])
            self.krb5admin.change_principal_passwd(user_name, self.conf['kdc_realm'], passwd)
            
            #create user local home directory
            local_mkdir_res = commands.getstatusoutput('if [ ! -d %s ] ;then sudo mkdir %s; fi' %(homeDirectory,homeDirectory))
            if local_mkdir_res[0]!=0:
                log.error('cannot create local home directory for user: %s. %s' %(user_name,local_mkdir_res[1]))
                raise Exception('cannot create local home directory for user: %s. %s' %(user_name,local_mkdir_res[1]))
            local_chown_res = commands.getstatusoutput('sudo chown %s:%s %s' %(user_name,user_ou,homeDirectory))
            if local_chown_res[0]!=0:
                log.error('cannot chown local home directory for user: %s. %s' %(user_name,local_chown_res[1]))
                raise Exception('cannot chown local home directory for user: %s. %s' %(user_name,local_chown_res[1]))
            
            #create user HDFS home directory
            hdfs_homedir_exist_res = commands.getstatusoutput('hadoop fs -test -z /user/%s' %(user_name))
            if hdfs_homedir_exist_res[0]!=0:
                hdfs_mkdir_res = commands.getstatusoutput('hadoop fs -mkdir /user/%s' %(user_name))
                if hdfs_mkdir_res[0]!=0:
                    log.error('cannot create HDFS home directory for user: %s. %s' %(user_name,hdfs_mkdir_res[1]))
                    raise Exception('cannot create HDFS home directory for user: %s. %s' %(user_name,hdfs_mkdir_res[1]))
            else:
                log.warn('HDFS home directory for user: %s already exists. %s' %(user_name,hdfs_homedir_exist_res[1]))
            
            hdfs_chown_res = commands.getstatusoutput('hadoop fs -chown %s:%s /user/%s' %(user_name,user_ou,user_name))
            if hdfs_chown_res[0]!=0:
                log.error('cannot chown HDFS home directory for user: %s. %s' %(user_name,hdfs_chown_res[1]))
                raise Exception('cannot chown HDFS home directory for user: %s. %s' %(user_name,hdfs_chown_res[1]))

            log.info('user added :%s ' %(user_name))
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
            raise
    
            
        
    def delete_user(self,user_name,user_ou):
        
        log.info('deleting user: %s' %(user_name) )
        try:
            #delete user account from ldap
            self.lDAPAdmin.delete_entry(user_name, user_ou)
            #delete user account from kdc
            self.krb5admin.del_principal(user_name, self.conf['kdc_realm'])
            log.info('user deleted: %s' %(user_name) )
            #TODO del user homedir
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
            raise
    
    def update_user(self,user_name,passwd):
        log.info('updating password for user :%s ' %(user_name))
        try:
            #update user password in kdc
            self.krb5admin.change_principal_passwd(user_name, self.conf['kdc_realm'], passwd)
            log.info('user password updated for :%s ' %(user_name))
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
            raise
    
    def grant_privileges_to_user(self,user_name,high_group,table_ids):
        #if user is high group, add to aclhigh group
        group= "aclhigh" if high_group ==1 else "acl"
        try:
            self.lDAPAdmin.modify_entry(group, ACL_OU, [(ldap.MOD_ADD, 'memberUid', str(user_name))])
            log.info('add user: %s to %s group.' %(user_name,group))
        except Exception,e:
            log.error('error when adding user %s to %s group. %s' %(user_name,group,e))
        #hive -e "grant ..."
        log.info('granting privileges for user %s  to tables:  %s' %(user_name,table_ids))
        try:
            for table in table_ids:
                res = hive_grant(user_name,DEFAULT_DATABASE+table['table_name'],table['priv'])
                time.sleep(1)
                if res:
                    log.info('privileges granted  for user %s  to table:  %s with priv: %s' %(user_name,table['table_name'],table['priv']))
                else:
                    raise Exception
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
            raise
            
    
    def revoke_privileges_to_user(self,user_name,table_ids):
        #hive -e "revoke ..."
        log.info('revoking privileges for user: %s to tables:  %s' %(user_name,table_ids))
        try:
            for table in table_ids:
                res = hive_revoke(user_name,DEFAULT_DATABASE+table['table_name'],table['priv'])
                time.sleep(1)
                if res:
                    log.info('privileges revoked for user: %s to table:  %s with priv: %s' %(user_name,table['table_name'],table['priv']))
                else:
                    raise Exception
        except Exception,e:
            log.error('Unexpected error: %s' %(e))
            raise
