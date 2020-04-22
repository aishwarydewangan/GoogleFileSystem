import socket
import os
import pickle
import sys
import math 

MASTER_SV_PORT = 7082
MASTER_SV_BACKUP_PORT = 7083

#Connecting to the master_server
def connect_to_master_server(getCommand, no_of_arg):
    try:
        s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'),MASTER_SV_PORT))
    except:
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),MASTER_SV_BACKUP_PORT))
        except:
            print("Master server is not active...Try again later!!!")        
            sys.exit()    

    if no_of_arg==2:
        decision, filename = getCommand.split(' ')
        if(decision=="write"):
            size=str(os.path.getsize(filename))
            fileplussize="client"+":write:"+filename+":"+size
            s.send(fileplussize.encode("ascii"))
            status=s.recv(2048).decode("ascii")
            return status
            
        if(decision=="read"):
            file_read= "client"+":read:"+filename
            s.send(file_read.encode("ascii"))
            status = s.recv(2048).decode("ascii")
            return status
       

        if(decision == "append"):
            size=str(os.path.getsize(filename))
            fileplussize="client"+":append:"+filename+":"+size
            s.send(fileplussize.encode("ascii"))
            status=s.recv(2048).decode("ascii")
            return status
            

#Connecting to the chunk_server
def send_to_chunk_server(decision,chunkInfo,filename):
    if(decision=="read"):
        info_arr = chunkInfo.split(";")
        for files in chunkInfo:
            filename_chunkid = files.split("=")[0]
            ip_ports = files.split("=")[1]
            ip_port_arr = ip_ports.split(",")
            for ip_port in ip_port_arr:
                ip = int(ip_port.split(":")[0])
                port = int(ip_port.split(":")[1])
                chunk_server_msg = "client:" + "read:" + filename_chunkid
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname(ip),port))
                s.send(chunk_server_msg.encode('ascii'))
                status = s.recv(2048)
                if not status:
                    print("Unsuccessful Read")
                    return

    if(decision=="append"):
        ip_port_size_arr = chunkInfo.split(",")
        for ip_port_size in ip_port_arr:
            ip_port, writeSize = ip_port_size.split("=")
            ip, port = ip_port.split(":")
            ip = int(ip)
            port = int(port)
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname(ip),port))
            chunk_server_msg = "client:" + "append:" + filename_chunkid + ":" + writeSize
            s.send(chunk_server_msg.encode('ascii'))
            status = s.recv(2048)
            if not status:
                print("Unsuccessful Write")
                return

            
if __name__=="__main__":
    while True:
        getCommand=input()
        a=len(getCommand.split())

        if a==2:
            decision, filename = getCommand.split(' ')
            if(decision=="write" or decision == "append"):
                chunkInfo=connect_to_master_server(getCommand,a)
                if chunkInfo:
                    send_to_chunk_server(decision,chunkInfo,filename)
                    print("File Written Successfully")
                else: 
                    print("File already Present")    
            
            if(decision=="read"):
                chunkInfo=connect_to_master_server(getCommand,a)
                send_to_chunk_server(decision,chunkInfo,filename)
                if chunkInfo:
                    print("File Read Successfully")
                else:
                    print("File Read is Unsuccessful")


