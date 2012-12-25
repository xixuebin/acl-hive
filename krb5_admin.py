import pexpect
import paramiko
import os
import sys
import logging

log = logging.getLogger('ACL-Hive')

class Krb5Admin(object):
    
    
    def __init__(self,kdc_host, kdc_ssh_port, ssh_username,ssh_passwd):
        self.kdc_host = kdc_host
        self.kdc_ssh_port = kdc_ssh_port
        self.ssh_username = ssh_username
        self.ssh_passwd = ssh_passwd
        
    def _connect2KDC(self):
        try:
            self.child = pexpect.spawn('ssh -p%s %s@%s' %(self.kdc_ssh_port,self.ssh_username, self.kdc_host))
            self.child.expect('%s@%s\'s password:' %(self.ssh_username,self.kdc_host))
            self.child.sendline(self.ssh_passwd)
            self.child.expect('%s@.* ~' %(self.ssh_username))
        except:
            log.error('cannot contract KDC.kdc_host: %s ,kdc_ssh_port: %s ,ssh_username: %s , ssh_passwd: %s' %(self.kdc_host,self.kdc_ssh_port,self.ssh_username,self.ssh_passwd))
            raise
    def _close(self):
        self.child.sendline('exit')
        self.child.sendline('exit')
    
    def add_principal(self,principal,realm):
        self._connect2KDC()
        if self.child != None:
            try:
                self.child.sendline(' su root')
                self.child.expect('Password:')
                self.child.sendline('***********')
                self.child.expect('root@')
                self.child.sendline('/usr/sbin/kadmin.local')
                self.child.expect('kadmin.local:')
                self.child.sendline('addprinc -randkey -maxrenewlife 1days %s' %(principal))
                res = self.child.expect(['Principal \"'+principal+'@'+realm+'\" created','Principal or policy already exists while creating \"'+principal+'@'+realm+'\"'])
                if res==0:
                    log.info('added principal: %s' %(principal))
                elif res ==1:
                    log.error('principal: %s already exists' %(principal))
                    raise Exception('principal already exists')
                self.child.sendline('quit')
            except Exception,e:
                log.error('error occurred while adding principal: %s. %s' %(principal,e))
                raise
            finally:
                self._close()
        
    def del_principal(self, principal,realm):
        self._connect2KDC()
        if self.child != None:
            try:
                self.child.sendline(' su root')
                self.child.expect('Password:')
                self.child.sendline('***********')
                self.child.expect('root@')
                self.child.sendline('/usr/sbin/kadmin.local')
                self.child.expect('kadmin.local:')
                self.child.sendline('delprinc  %s' %(principal))
                self.child.expect('yes/no')
                self.child.sendline('yes')
                res = self.child.expect(['Principal \"'+principal+'@'+realm+'\" deleted','Principal does not exist while deleting principal \"'+principal+'@'+realm+'\"'])
                if res==0:
                    log.info('deleted principal: %s' %(principal))
                elif res ==1:
                    log.warn('principal: %s does not exists' %(principal))
#                    raise Exception('principal does not exists')
                self.child.sendline('quit')
            except Exception,e:
                log.error('error occurred while deleting principal: %s. %s' %(principal,e))
                raise
            finally:
                self._close()
        
    def change_principal_passwd(self, principal,realm, principal_passwd):
        self._connect2KDC()
        if self.child != None:
            try:
                self.child.sendline(' su root')
                self.child.expect('Password:')
                self.child.sendline('***********')
                self.child.expect('root@')
                self.child.sendline('/usr/sbin/kadmin.local')
                self.child.expect('kadmin.local:')
                self.child.sendline('cpw -pw %s %s' %(principal_passwd,principal))
                res = self.child.expect(['Password for \"'+principal+'@'+realm+'\" changed','Principal does not exist while changing password for \"'+principal+'@'+realm+'\"'])
                if res==0:
                    log.info('passwd changed succeeded for principal: %s' %(principal))
                elif res ==1:
                    log.error('principal: %s does not exists' %(principal))
                    raise Exception('principal does not exists')
                self.child.sendline('quit')
            except Exception,e:
                log.error('error occurred while changing  password for principal: %s. %s' %(principal,e))
                raise
            finally:
                self._close()
        
        
    def generate_keytab(self, principal, keytabfile):
        self._connect2KDC()
        if self.child != None:
            try:
                self.child.sendline(' su root')
                self.child.expect('Password:')
                self.child.sendline('***********')
                self.child.expect('root@')
                self.child.sendline('/usr/sbin/kadmin.local')
                self.child.expect('kadmin.local:')
                self.child.sendline('xst -norandkey -k %s %s' %(keytabfile,principal))
                res = self.child.expect(['added to keytab WRFILE','Principal '+principal+' does not exist'])
                if res==0:
                    log.info('Keytab file for principal %s generated.' %(principal))
                elif res ==1:
                    log.error('principal: %s does not exists' %(principal))
                    raise Exception('principal does not exists')
                self.child.sendline('quit')
            except Exception,e:
                log.error('error occurred while generating keytab file for principal: %s. %s' %(principal,e))
                raise
            finally:
                self._close()
    
    def conf_local_keytabfile(self,principal,kdc_keytab):
        home = '/home/hadoop/hiveuserkeytab/'
        os.system('if [ ! -d %s ] ; then mkdir -p %s; fi' %(home,home))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(kdc_host, kdc_ssh_port, ssh_username, ssh_passwd)
        ftp = ssh.open_sftp()
        ftp.get(kdc_keytab, home+principal+'.keytab')
        ftp.close()
    
