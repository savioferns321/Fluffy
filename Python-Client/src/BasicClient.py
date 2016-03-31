import socket
import common_pb2
import pbd
from common.MessageBuilder import *
from protobuf_to_dict import protobuf_to_dict
from asyncore import read

class BasicClient:
    def __init__(self,host,port):
        self.host = host
        self.port = port
        self.sd = socket.socket()
    
    def setName(self,name):
        self.name = name

    def getName(self):
        return self.name
    
    def startSession(self):
        self.sd.connect((self.host,self.port))
        print("Connected to Host:",self.host,"@ Port:",self.port)

    def stopSession(self):
        builder = MessageBuilder()
        msg = builder.encode(MessageType.leave, self.name,'', '')
        print(msg)
        self.sd.send(msg)
        self.sd.close()
        self.sd = None
    
    def join(self,name):
        builder = MessageBuilder()
        self.name = name
        msg = builder.encode(MessageType.join, name, '', '')
        self.sd.send(msg)
        
    def sendMessage(self,message):
        if len(message) > 1024:
            print('message exceeds 1024 size')
            
        builder = MessageBuilder()
        msg = builder.encode(MessageType.msg,self.name,message,'')
        self.sd.send(msg)
        
    def getFile(self,name):
        req = common_pb2.Task()
        req.tasktype = common_pb2.Task.TaskType.READ
        req.sender = "127.0.0.1"
        req.filename = name
        
        request = req.SerializeToString()
        return request
    
    def chunckFile(self,file):
        fileChunck = []
        with open(file, "rb") as fileContent:
            data = ifile.read(1024*1024)
            while data:
                fileChunck.append(data)
                data = ifile.read(1024*1024)
        return fileChunck
        
            
    
    def sendFile(self,filename,filecontent, noofchunck,chunckid):
        req = common_pb2.Task()
        req.tasktype = common_pb2.Task.TaskType.WRITE
        req.sender = "127.0.0.1"
        req.filename = filename
        req.fileContent = filecontent
        req.chunck_no = chunckid
        req.no_of_chucnk = noofchunck
        request = req.SerializeToString()
        return request
    
    def sendData(self,data,host,port):
        s = socket.socket()
        s.connect((host, port))
        msg_len = struct.pack('>L', len(msg_out))
        s.sendall(msg_len + msg_out)
        len_buf = receiveMsg(s, 4)
        msg_in_len = struct.unpack('>L', len_buf)[0]
        msg_in = receiveMsg(s, msg_in_len)
        r = comm_pb2.Request()
        r.ParseFromString(msg_in)
        s.close
        return r


    def receiveMsg(socket, n):
        buf = ''
        while n > 0:
            data = socket.recv(n)
            if data == '':
                raise RuntimeError('data not received!')
            buf += data
            n -= len(data)
        return buf
        