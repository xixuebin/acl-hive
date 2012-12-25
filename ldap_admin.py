
#ldapadd -x -D "cn=Manager,dc=unisonis,dc=com" -W -f entrys.ldif 
#ldapsearch -x -b 'dc=unisonis,dc=com' '(o=entrys)'


import sys, ldap, logging

log = logging.getLogger('ACL-Hive')
ENTRY_FILTER = 'sn=*'

class LDAPAdmin:

    def __init__(self, ldap_host, ldap_base_dn, mgr_cred, mgr_passwd):
        self.ldap_host = ldap_host
        self.ldap_base_dn = ldap_base_dn
        self.mgr_cred = mgr_cred
        self.mgr_passwd = mgr_passwd
    def _bind(self):
        try:
            self.ldapconn = ldap.open(self.ldap_host)
            self.ldapconn.simple_bind(self.mgr_cred, self.mgr_passwd)
            self.ldap_base_dn = self.ldap_base_dn
        except ldap.SERVER_DOWN, e:
            log.error( 'cannot connect to LDAP Server. %s' %(e) )
            raise
        except ldap.LDAPError, e:
            log.error('problem with ldap %s' %(e))
            raise
            
    def add_newou(self,ou_name):
        self._bind()
        ou_dn = 'ou=%s,%s' % (ou_name, self.ldap_base_dn)
        ou_info = {'ou':str(ou_dn),'objectClass':['organizationalUnit','top']}
        ou_attrib = [(k, v) for (k, v) in ou_info.items()]
        try:
            self.ldapconn.add_s(ou_dn, ou_attrib)
            log.info("Added to LDAP a new ou : %s" % (ou_dn))
        except ldap.ALREADY_EXISTS,e:
            log.warn('ou: %s already exists. %s' %(ou_dn,e))
            
    def add_newgroup(self,ou_name,group_name,gidNumber):
        self._bind()
        group_dn = 'cn=%s,ou=%s,%s' % (group_name,ou_name, self.ldap_base_dn)
        group_info = {'cn':str(group_name),'gidNumber':str(gidNumber),'objectClass':['posixGroup','top']}
        group_attrib = [(k, v) for (k, v) in group_info.items()]
        try:
            self.ldapconn.add_s(group_dn, group_attrib)
            log.info("Added to LDAP a new group %s on ou %s" % (group_name,ou_name))
        except ldap.ALREADY_EXISTS,e:
            log.warn('group %s  already exists. %s' %(group_dn,e))
            
    def list_entrys(self, entry_filter=None, attrib=None):
        self._bind()
        if not entry_filter:
            entry_filter = ENTRY_FILTER
        try:
            s = self.ldapconn.search_s(self.ldap_base_dn, ldap.SCOPE_SUBTREE, entry_filter, attrib)
            entry_list = []
            for entry in s:
                entry_list.append(entry)
            return entry_list
        except ldap.LDAPError, e:
            log.error('problem with ldap listing entry: %s' %(e))
            raise

    def add_entry(self, entry_name, entry_ou, entry_info):
        self._bind()
        entry_dn = 'cn=%s,ou=%s,%s' % (entry_name, entry_ou, self.ldap_base_dn)
        entry_attrib = [(k, v) for (k, v) in entry_info.items()]
        try:
            self.ldapconn.add_s(entry_dn, entry_attrib)
            log.info("Added to LDAP a new entry %s with ou=%s" % (entry_name, entry_ou))
        except ldap.ALREADY_EXISTS, e:
            log.error('entry already exists. %s ' %(e))
            raise
        except ldap.LDAPError, e:
            log.error('problem with ldap adding entry: %s' %(e))
            raise


    def modify_entry(self, entry_name, entry_ou, entry_attrib):
        self._bind()
        entry_dn = 'cn=%s,ou=%s,%s' % (entry_name, entry_ou, self.ldap_base_dn)
        try:
            self.ldapconn.modify_s(entry_dn, entry_attrib)
            log.info("Modified entry %s with ou=%s" % (entry_name, entry_ou))
        except ldap.TYPE_OR_VALUE_EXISTS,e:
            log.warn('already exists . %s' %(e))
        except ldap.NO_SUCH_OBJECT, e:
            log.error('no such object. %s' %(e))
            raise
        except ldap.LDAPError, e:
            log.error('problem with ldap modifing entry: %s' %(e))
            raise

    def delete_entry(self, entry_name, entry_ou):
        self._bind()
        entry_dn = 'cn=%s,ou=%s,%s' % (entry_name, entry_ou, self.ldap_base_dn)
        try:
            self.ldapconn.delete_s(entry_dn)
            log.info( "Deleted an entry %s with ou=%s" % (entry_name, entry_ou))
        except ldap.NO_SUCH_OBJECT, e:
            log.warn('no such object. %s' %(e))
#            raise
        except ldap.LDAPError, e:
            log.error('problem with ldap deleting entry: %s' %(e))
            raise

