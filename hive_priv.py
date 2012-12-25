import logging
import commands
import re

log = logging.getLogger('ACL-Hive')
HIVE_PATH='/usr/local/hadoop/hive-0.8.1/bin/'

def parse_priv_code(priv_code):
        # priv_code:
    # 1: read only 2: write only 3: read write
    if priv_code == 1:
        hive_priv = 'Select,Show_Database'
    elif priv_code == 2:
        hive_priv = 'Update'
    elif priv_code ==3:
        hive_priv = 'Select,Show_Database,Update'
    else:
        log.error('error priv code :%s' %(priv_code))
    return hive_priv
        
def hive_grant(user_name,table_name,priv_code):
    
    privs = parse_priv_code(priv_code).split(',')
    
    for p in privs:
        hql = "GRANT "+p+" ON TABLE \`"+table_name +"\` TO USER \`"+user_name+"\`"
        if hive_cmd(hql) == False:
            raise Exception
            return False
    
    return True
def hive_revoke(user_name,table_name,priv_code):

    privs = parse_priv_code(priv_code).split(',')
    for p in privs:
        hql = "REVOKE "+p+" ON TABLE \`"+table_name +"\` FROM USER \`"+user_name+"\`";
        if hive_cmd(hql) == False:
            raise Exception
            return False
    
    return True
    
def hive_cmd(hql):
    cmd = HIVE_PATH+'hive -e \"'+hql+ '\"'
    log.info(cmd)
    res = commands.getstatusoutput(cmd)
    if res[0] == 0:
        return True
    elif res[0] == 2304:
        log.warning('priv already granted. %s' %(res[1]))
        return True
    else:
        log.error('hive command error. %s' %(res[1]))
        return False
    
def get_current_priv(user_name,table_name):
    hql = "SHOW GRANT USER \`"+user_name+"\` ON TABLE \`"+table_name+"\`";
    cmd = HIVE_PATH+'hive -e \"'+hql+ '\"'
    log.info(cmd)
    res = commands.getstatusoutput(cmd)
    if res[0] == 0:
        patobj = re.compile('privilege\t\w+')
        result = patobj.findall(res[1])
        for r in result:
            r.replace('privilege\t','')
        
        cur_priv_list = [r.replace('privilege\t','') for r in result]
        return cur_priv_list
    else:
        log.error('cannot get current priv of table %s on user %s. %s' %(table_name,user_name,res[1]))
        raise Exception('cannot get current priv of table %s on user %s. %s' %(table_name,user_name,res[1]))
