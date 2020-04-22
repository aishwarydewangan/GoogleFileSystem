import math
import socket
import threading

HOST = "127.0.0.1"
PORT = 8080

MAXSIZE = 2048

chunkservers = {}
files = {}

def getFileName(name):
    name = name.split('_')
    return name[0]

class FileInfo:

    def __init__(self, name, totalSize):
        self.name = name
        self.totalSize = totalSize
        self.chunkInfo = {}
        self.hasLastChunk = False
        self.lastChunkID = None
        self.totalChunks = math.ceil(self.totalSize/MAXSIZE)
        self.updateLastChunkStatus()

    def updateLastChunkStatus(self):
        self.hasLastChunk = (self.totalSize%MAXSIZE != 0)
        self.lastChunkID = math.ceil(self.totalSize/MAXSIZE)

    def updateFileSize(self, size):
        self.totalSize += size
        self.updateLastChunkStatus()

    def updateChunkInfo(self, chunk, cs):
        if chunk not in chunkInfo.keys():
            self.chunkInfo[chunk] = []
        self.chunkInfo[chunk].append(chunkservers[cs])

    def getLastChunkStatus(self):
        return self.hasLastChunk

    def getTotalSize(self):
        return self.totalSize

    def getTotalChunks(self):
        return self.totalChunks

    def getLastChunkID(self):
        return self.lastChunkID

    def getChunkInfo(self, chunkID):
        return self.chunkInfo[chunkID]

    def getAllChunkInfo(self):
        return self.chunkInfo

    def getFirstChunkServer(self, chunkID):
        return self.chunkInfo[chunkID][0]

    def removeServerInfo(self, chunk, cs):
        i = 0

        for obj in chunkInfo[chunk]:
            if cs[0]==obj.getIP() and cs[1]==obj.getPort():
                break
            i += 1

        chunkInfo[chunk].pop(i)

class ChunkServer:

    def __init__(self, ip, port, status):
        self.ip = ip
        self.port = port
        self.status = status
        self.load = 0
        self.chunkInfo = {}

    def addChunk(self, chunk, size):
        self.chunkInfo[chunk] = size
        self.load += 1

    def getStatus(self):
        return self.status

    def setStatus(self, status):
        self.status = status

    def getIP(self):
        return self.ip

    def getPort(self):
        return self.port

    def getChunks(self):
        return self.chunkInfo.keys()

    def updateChunk(self, chunk_list):

        chunks = chunk_list.split(',')

        for cl in chunks:
            c = cl.split(':')
            self.addChunk(c[0], int(c[1]))


class ClientThread(threading.Thread):

    def __init__(self, client_address, client_socket, info):
        threading.Thread.__init__(self)
        self.caddress = client_address
        self.csocket = client_socket
        self.info = info
        print("Client Connected: ", self.caddress)

    def run(self):
        
        msg = ''

        if self.info[0]=='read':
            msg = self.readFile(self.info[1])

        if self.info[0]=='write':
            msg = self.writeFile(self.info[1], self.info[2])

        if self.info[0]=='append':
            msg = self.appendFile(self.info[1], self.info[2])

        csocket.sendall(bytes(msg), 'UTF-8')

        print("Client " + caddress + " wanted to " + self.info[0] + " on file " + self.info[1])
        print("Response sent to Client " + caddress + "-->" + msg)

    def readFile(self, name):

        obj = files[name]

        chunkInfo = obj.getAllChunkInfo()

        msg = ''

        for chunk in chunkInfo.keys():
            serverList = ''
            for cs in chunkInfo[chunk]:
                ip = cs.getIP()
                port = cs.getPort()
                serverList += ip + ":" + port + ','
            msg = msg + chunk + "=" + serverList[:-1] + ";"

        msg = msg[:-1]

        return msg


    def writeFile(self, name, size):

        obj = FileInfo(name, size)

        files[name] = obj

        cs_list = chunkservers.values()

        cs_list.sort(key=operator.attrgetter('load'))

        i = 0

        j = 0

        msg = ''

        while i < obj.getTotalChunks():

            if j==len(cs_list)-1:
                j = 0

            ip = cs_list[j].getIP()
            port = cs_list[j].getPort()
            writeSize = MAXSIZE

            if i==obj.getTotalChunks()-1:
                writeSize = size%MAXSIZE

            msg += str(ip) + ":" + str(port) + "=" + str(writeSize) + ","
            i += 1
            j += 1

        msg = msg[:-1]

        return msg

    def appendFile(self, name, size):

        newSize = size

        obj = files[name]

        cs_list = chunkservers.values()

        cs_list.sort(key=operator.attrgetter('load'))

        msg = ''

        if obj.getLastChunkStatus():
            oldSize = obj.getTotalSize()

            lastChunkSize = oldSize%MAXSIZE

            to_add = MAXSIZE-lastChunkSize

            chunkServerList = obj.getChunkInfo(obj.getLastChunkID())
            ip = chunkServerList[0].getIP()
            port = chunkServerList[1].getPort()

            if newSize <= to_add:
                msg = str(ip) + ":" + str(port) + "=" + str(newSize)
                obj.updateFileSize(size)
                return msg
            else:
                msg = str(ip) + ":" + str(port) + "=" + str(to_add) + ","
                newSize -= to_add

        i = 0

        j = 0

        total_chunks = math.ceil(newSize/MAXSIZE)

        while i < total_chunks:

            if j==len(cs_list)-1:
                j = 0

            ip = cs_list[j].getIP()
            port = cs_list[j].getPort()
            writeSize = MAXSIZE

            if i==total_chunks-1:
                writeSize = size%MAXSIZE

            msg += str(ip) + ":" + str(port) + "=" + str(writeSize) + ","
            i += 1
            j += 1

        msg = msg[:-1]

        obj.updateFileSize(size)

        return msg


class HeartbeatThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        print("HeartbeatThread Started")

    def chunkServerDown(self, cs):

        # Find which chunks to copy
        chunks_to_copy = chunkservers[cs].getChunks()

        # Make List
        msg = "master:copy:"

        for chunk in chunks_to_copy:

            fileName = getFileName(chunk)

            fileObj = files[fileName]

            fileObj.removeServerInfo(chunk, cs)

            obj = fileObj.getFirstChunkServer(chunk)

            msg += obj.getIP() + ":" + obj.getPort() + "=" + chunk + ","

        msg = msg[:-1]

        chunkservers[cs].setStatus(False)

        del chunkservers[cs]

        # Send this list to one of the chunkservers
        cs_list = chunkservers.values()

        cs_list.sort(key=operator.attrgetter('load'))

        cs = cs_list[0]

        send_ip = cs.getIP()
        send_port = cs.getPort()

        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((send_ip, send_port))
        sender.sendall(bytes(msg, 'UTF-8'))
        sender.close()

    def run(self):
        for cs in chunkservers.keys():
            if chunkservers[cs].getStatus():
                ip = cs[0]
                port = cs[1]
                heartbeat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                check = False
                try:
                    heartbeat.connect((ip, port))
                except socket.error:
                    try:
                        heartbeat.connect((ip, port))
                    except socket.error:
                        print("Unable to connect to ChunkServer ", cs)
                        check = True
                if not check:
                    heartbeat.sendall(bytes("master:heartbeat", 'UTF-8'))
                    data = heartbeat.recv(1024)
                    length = int(data.decode())
                    if length > 0:
                        heartbeat.sendall(bytes("ok", 'UTF-8'))
                        data = heartbeat.recv(length+100)
                        chunkservers[cs].updateChunk(data.decode())
                        for cl in data.decode().split(','):
                            chunk_info = cl.split(':')
                            chunkName = chunk_info[0]
                            fileName = getFileName(chunkName)

                            fileObj = files[fileName]

                            fileObj.updateChunkInfo(chunkName, cs)
                heartbeat.close()
                if check:
                    self.chunkServerDown(cs)
        print("HeartbeatThread Completed")
        threading.Timer(60, self.run).start()

class RegisterChunkServerThread(threading.Thread):

    def __init__(self, chunk_address, chunk_socket):
        threading.Thread.__init__(self)
        self.csocket = chunk_socket
        self.caddress = chunk_address
        print("ChunkServer Connected: ", self.caddress)

    def run(self):
        self.csocket.sendall(bytes("ok", 'UTF-8'))
        data = self.csocket.recv(20000)
        if data:
            data = data.decode().split(',')
            ip = self.caddress[0]
            port = self.caddress[1]

            obj = ChunkServer(ip, port, True)

            for d in data:
                chunk_info = d.split(':')
                obj.addChunk(chunk_info[0], int(chunk_info[1]))

            chunkservers[self.caddress] = obj

            for d in data:
                chunk_info = d.split(':')

                chunkName = chunk_info[0]

                fileName = getFileName(chunkName)

                fileObj = files[fileName]

                fileObj.updateChunkInfo(chunkName, self.caddress)


            print("ChunkServer ", self.caddress, " registered...")

        print("ChunkServer ", self.caddress, " disconnected...")

class InfoThread(threading.Thread):

    def __init__(self, chunk_address, chunk_socket, chunk_name):
        threading.Thread.__init__(self)
        self.csocket = chunk_socket
        self.caddress = chunk_address
        self.cname = chunk_name
        print("ChunkServer Connected: ", chunk_address)

    def run(self):

        fileName = getFileName(self.cname)

        fileObj = files[fileName]

        chunk_server_info = fileObj.getChunkInfo(self.cname)

        msg = ''

        for obj in chunk_server_info:
            msg += obj.getIP() + ":" + obj.getPort() + ','

        msg = msg[:-1]

        csocket.sendall(bytes(msg, 'UTF-8'))

        print("ChunkServer ", self.caddress, " disconnected...")

if __name__ == '__main__':
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))

    print("Primary Server started")
    print("Waiting for requests..")
    HeartbeatThread().start()
    msg = ''
    while True:
        server.listen()
        sock, address = server.accept()
        data = sock.recv(2048)
        msg = data.decode()
        if msg == 'register':
            RegisterChunkServerThread(address, sock).start()
        words = msg.split(':')
        if words[0] == 'info':
            InfoThread(address, sock, words[1]).start()
        elif words[0] == 'client':
            ClientThread(address, sock, words[1:]).start()