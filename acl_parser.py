import json

class HiveAclParser(object):


    def __init__(self,aclstr):
        self.acl = json.loads(aclstr)
        
    def get_result(self):
        return self.acl['code']
    
    def get_storage_type(self):
        return self.acl['storage_type']
    def get_msg(self):
        return self.acl['msg']
    
if __name__ == '__main__':
    acl = '{"code":200,"storage_type":"hive","msg":[{"type":0,"user_name":"test","passwd":"test"},{"type":1,"user_name":"test","passwd":"test"}]}'

    parser = HiveAclParser(acl)
    res = parser.get_result()
    print res
    storage_type = parser.get_storage_type()
    print storage_type
    msg = parser.get_msg()
    
    for m in msg:
        print m
