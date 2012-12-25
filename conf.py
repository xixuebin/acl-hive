# -*- coding: utf-8 -*-
import ConfigParser

_SECTION_ZK='ZK'
_SECTION_LDAP='LDAP'
_SECTION_KDC='KDC'
_SECTION_MNT='MONITOR'

class Config(object):
	
	def getconfig(self,conf_file):
		parser = ConfigParser.ConfigParser()
		parser.read(conf_file)
		result = {}
		
		zk_config = parser.items(_SECTION_ZK)
		for (name,value) in zk_config:
			result[name]=value
			
		ldap_config = parser.items(_SECTION_LDAP)
		for (name,value) in ldap_config:
			result[name]=value
			
		kdc_config = parser.items(_SECTION_KDC)
		for (name,value) in kdc_config:
			result[name]=value
	
		mnt_config = parser.items(_SECTION_MNT)
		for (name,value) in mnt_config:
			result[name]=value
			
		return result
			
#		self.ldap_ip=ldap_config['ldap_ip']
		
if __name__ == '__main__':
	config = Config()
	conf =  config.getconfig('acl-hive.cfg')
	print conf