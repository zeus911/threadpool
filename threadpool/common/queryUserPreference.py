#!/usr/bin/env python
#-*- coding: utf-8 -*-
import upps_pb2
import rpc_client.client.channel as channel
import rpc_client.client.controller as controller
import rpc_client.client.transport as transport
import rpc_client.client.stub as stub
import rpc_client.client.protocol as protocol
import rpc_client.client.error as error
host="10.52.75.52"
port=7788

def queryUserPreference(servicekey,secretkey,subservice,cuid,srcType):
    try:
        request = upps_pb2.GetUserPreferenceRequest()
        request.header.servicekey = servicekey
        request.header.secretkey = secretkey
        request.header.subservice = subservice
        request.cuid = cuid
        request.srcType.append(srcType)
        '''request.include_tag.append('76')
        request.include_tag.append('AAAAA')
        request.exclude_tag.append('25146')'''

        ctrl = controller.RpcController()
        ctrl.serviceName = "UserService"
        tt = transport.Transport()
        pp = protocol.PbProtocol(tt)
        cc = channel.RpcChannel(pp, host, port, True)
        ctrl.response_buffer_size = 10000
        ss =stub.RpcStub(upps_pb2.UserService_Stub, cc, request, ctrl)
        response = ss.GetUserPreference(request, ctrl)
        print("response:"+str(response))

        key='values'
        res = []
        obj_list = eval('response.%s' %key)
        for o in obj_list:
            data = {}
            for f in ["tag","level","value","srcType"]:
                try:
                    if o.HasField(f):
                        exec('data["%s"]=o.%s'%(f, f))
                except:
                    exec('data["%s"]=o.%s'%(f, f))
            res.append(data)

        result_list=res
        result_list = sorted(result_list, key=lambda x:(x.get('level', 0),x.get('value', 0)))
        #print(result_list)
        exp_list=[{'tag': u'AAAAA', 'srcType': 12}, {'tag': u'76', 'srcType': 12, 'value': 3.4571070671081543, 'level': 1}]
        #assert(exp_list == result_list)
        #assert(len(result_list) != 0)
        #return 0
        return result_list
    except Exception as e:
        print(str(e))
        return []

if __name__ == "__main__":
    ret=queryUserPreference("tuangou","10af214253a015b8ffdfbac9f98077b4","userpreference","1FF79C96DA05AE4E4F6DF52A30AB445C|497502520559068",12)
    print ret

