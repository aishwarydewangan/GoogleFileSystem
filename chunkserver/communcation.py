import socket, threading
import os
import sys

MASTER_IP = "127.0.0.1"
MASTER_PORT = 8080
DUPLICATE_MASTER_IP = "127.0.0.1"
DUPLICATE_MASTER_PORT = 8081
MAX_CHUNK_SIZE = 2048

class chunkserver():
	dirty = False
	mutual_excl = {}
	def __init__(self):
		self.myport = int(sys.argv[1])
		print("Registering chunk server")
		try:
			client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			client.bind(("127.0.0.1",self.myport))
			client.connect((MASTER_IP, MASTER_PORT))
		except:
			try:
				client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				client.bind(("127.0.0.1",self.myport))
				client.connect((DUPLICATE_MASTER_IP, DUPLICATE_MASTER_PORT))
			except:
				sys.exit()
		self.path = "./"+str(self.myport)
		if os.path.exists(self.path):
			chunks = os.listdir(self.path)
		else:
			os.mkdir(self.path)
			chunks = []
		msgtosend = ""
		for file in chunks:
			filename = self.path+"/"+file
			file_stats = os.stat(filename)
			currsize = file_stats.st_size
			msgtosend+=file+":"+str(currsize)+","
		if len(chunks)!=0:
			msgtosend=msgtosend[:-1]
		client.sendall("register".encode())
		client.recv(60)
		client.sendall(msgtosend.encode())
		print("chunk server registered")
		client.close()

	def run(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(("127.0.0.1",self.myport))
		while True:
			sock.listen()
			client, address = sock.accept()
			threading.Thread(target = self.checkoperation,args = (client,address)).start()

	def checkoperation(self,client,address):
		recv=client.recv(400).decode("utf-8")
		print("msg recieved: ",recv)
		to_recv=recv.split(":")
		if(to_recv[0]=="master"):
			if(to_recv[1]=="heartbeat"):
				self.heartbeat_reply(client)
			elif(to_recv[1]=="copy"):
				client.close()
				self.copyfromchunkserver(recv[12:])
		elif(to_recv[0]=="client"):
			if to_recv[1]=="read":
				self.sendchunk(to_recv,client,address)
			elif to_recv[1]=="append":
				if to_recv[2] in (self.mutual_excl).keys():
					mutual = [to_recv,client]
					self.mutual_excl[recv[2]].append(mutual)
				else:
					self.appendchunk(to_recv,client)
			elif to_recv[1]=="write":
				if len(self.mutual_excl[to_recv[2]])!=0:
					mutual = [to_recv,client]
					self.mutual_excl[recv[2]].append(mutual)
				else:
					self.appendchunk(to_recv,client)
		elif(to_recv[0]=="chunkserver"):
			if(to_recv[1]=="appendinfo"):
				if to_recv[2] in (self.mutual_excl).keys():
					mutual = [to_recv,client]
					((self.mutual_excl)[to_recv[2]]).append(mutual)
				else:
					self.appendchunk(to_recv,client)
			elif(to_recv[1]=="sendcopy"):
				self.sendchunk(to_recv,client,address)

	
	def heartbeat_reply(self,client):
		print("reply to heartbeat msg")
		if(self.dirty == False):
			st = "0"
			client.sendall(st.encode())
		else:
			chunks = os.listdir(self.path)
			msgtosend = ""
			for file in chunks:
				filename = self.path+"/"+file
				file_stats = os.stat(filename)
				currsize = file_stats.st_size
				msgtosend+=file+":"+str(currsize)+","
			if len(chunks)!=0:
				msgtosend=msgtosend[:-1]
			nums = str(sys.getsizeof(msgtosend))
			client.sendall(nums.encode())
			client.recv(60)
			client.sendall(msgtosend.encode())
			self.dirty = False
		client.close()

	def copyfromchunkserver(self,copylist):
		print("copy from chunkserver")
		copylist = copylist.split(',')
		for item in copylist:
			item = item.split('=')
			chunkname = item[1]
			self.dirty = True
			item = item[0].split(":")
			serverip, serverport  = item[0],item[1]
			tosend = "chunkserver:sendcopy:"+chunkname
			s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			s1.connect((serverip, int(serverport)))
			s1.sendall(tosend.encode())
			chunk = self.path+"/"+chunkname
			with open(chunk,'wb') as f1:
				data = s1.recv(MAX_CHUNK_SIZE)
				f1.write(data)
			s1.close()
			print("chunk recieved")

	def sendchunk(self,to_recv,client,address):
		print("send chunk")
		chunk=self.path+"/"+to_recv[2]
		with open(chunk, 'rb') as f:
			data=f.read(MAX_CHUNK_SIZE)
			client.sendall(data)
		print("chunk sent")
		client.close()

	def appendchunk(self,recv,client_con):
		print("appending data")
		(self.mutual_excl)[recv[2]] = []
		mutual = [recv,client_con]
		(self.mutual_excl)[recv[2]].append(mutual)
		while len((self.mutual_excl)[recv[2]])!=0:
			conn =  (self.mutual_excl)[recv[2]][0]
			to_recv,client = conn[0],conn[1]
			chunk=self.path+"/"+to_recv[2]
			sizetoappend=int(to_recv[3])
			client.sendall("ok".encode())
			self.dirty = True	
			with open(chunk,'ab') as f1:
				data = client.recv(sizetoappend)
				f1.write(data)
			client.close()
			if to_recv[0]=="client":
				self.sendtosecondary(data,sizetoappend,to_recv[2])
			client.close()
			self.mutual_excl[recv[2]].pop(0)
		print("data appended")

	def sendtosecondary(self,data,sizetoappend,file):
		print("copying to secondary replicas")
		try:
			s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s1.connect((MASTER_IP, MASTER_PORT))
		except:
			try:
				s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s1.connect((DUPLICATE_MASTER_IP, DUPLICATE_MASTER_PORT))
			except:      
				sys.exit()
		s1.sendall(("info:"+file).encode())
		getlist = s1.recv(MAX_CHUNK_SIZE).decode()
		getlist = getlist.split(',')
		s1.close()
		print(getlist)
		for item in getlist:
			print(item)
			if len(item)>0:
				item = item.split(":")
				serverip, serverport = item[0],item[1]
				if serverport!=self.myport:
					s1 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					tosend = "chunkserver:appendinfo:"+file+":"+str(sizetoappend)
					s1.connect((serverip,serverport))
					s1.sendall(tosend.encode())
					st = s1.recv(1024)
					s1.sendall(data.encode())
					s1.close()
		print("copied to secondary replicas")

master = chunkserver()	
master.run()