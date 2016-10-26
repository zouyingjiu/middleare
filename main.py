# -*- coding: gbk -*-

import socket
import time
import select
import signal
import sys
import os
import errno
import thread
import threading
import httplib
import urllib
import urllib2
import Queue
import json
import binascii

import config

FLAG_WAITTING = 0
FLAG_FINISH = 1

TCP_BIND_IP 	= config.SysConfig.get("bind_ip", "127.0.0.1")
TCP_PORT 	= config.SysConfig.get("port", 58422)
HTTP_URL	= config.SysConfig.get("url", "http://127.0.0.1:8080/log/add")

MAX_LOG_SIZE	= config.SysConfig.get("max_log_size", 100)
POOL_SIZE	= config.SysConfig.get("pool_size", 16)
MAX_TASK_SIZE 	= config.SysConfig.get("max_task_size", 100)

DATA_FOLDER	= config.SysConfig.get("data_folder", "data")
STATUS_FOLDER	= config.SysConfig.get("status_folder", "status")
STATUS_FILENAME	= config.SysConfig.get("status_file", "status")

SERVER_ID	= config.SysConfig.get("srvid")

#S2L协议
S2L_REGISTER	= 0x93	#游戏服向Logsrv注册
S2L_LOGDATA	= 0x94
	
class CStatus(object):
	def __init__(self):
		self.m_HeadSeq = 0
		self.m_TailSeq = 0
		self.m_CurSeq = 0
		
		statusFilename = STATUS_FOLDER + "/" + STATUS_FILENAME
		self.m_Fp = open(statusFilename, "a+", 0)
		self.m_Bfp = open(statusFilename + ".bak", "a+", 0)
		self.Load()
	
	def _Load(self, line):
		try:
			lst = line.split(",")
			crc32 = int(lst[0]) & 0xffffffff
			self.m_HeadSeq = int(lst[1])
			self.m_CurSeq = int(lst[2])
			self.m_TailSeq = int(lst[3])
			s = str(self.m_HeadSeq) + "," + str(self.m_CurSeq) + "," + str(self.m_TailSeq)
			oldCrc = binascii.crc32(s) & 0xffffffff
			#检验文件完整性
			if oldCrc != crc32:
				return False
		except Exception, e:
			Log("载入状态信息异常", e)
			return False
		return True
	
	def Load(self):
		line1 = self.m_Fp.readline()
		line2 = line = self.m_Bfp.readline()
		if not self._Load(line1):
			Log("载入状态文件失败，尝试加载备份文件")
			if line2:
				if not self._Load(line2):
					Log("严重错误：载入历史状态失败，请检查磁盘获取接收状态，与统计中心获取发送状态.")
					sys.exit()
				else:
					#丢失的数据可能是cur，也可能是tail
					#cur可忽略。tail根据硬盘数据重建
					self._RecoverTail()
		if self.m_CurSeq > self.m_TailSeq:
			Log("严重错误：中转程序发送数量大于接收数量")
			sys.exit()
				
	def UpdateStatus(self):
		#status文件.crc,headSeq,curSeq,tailSeq
		s = str(self.m_HeadSeq) + "," + str(self.m_CurSeq) + "," + str(self.m_TailSeq)
		crc32 = binascii.crc32(s) & 0xffffffff
		newRecord = str(crc32) + "," + s		
		self.m_Fp.seek(0)
		self.m_Fp.truncate()
		self.m_Fp.write(newRecord)
		
		self.m_Bfp.seek(0)
		self.m_Bfp.truncate()
		self.m_Bfp.write(newRecord)
		
	def _RecoverTail(self)	:
		tailFilename = self.GetTailFilename()
		f = open(tailFilename, "r")
		lineNum = len(f.readlines())
		#tailSeq和硬盘校验不通过
		if lineNum  != self.m_TailSeq % MAX_LOG_SIZE:
			self.m_TailSeq = (self.m_TailSeq / MAX_LOG_SIZE) * MAX_LOG_SIZE + lineNum
		
	def LocateFinFile(self):
		return self.m_CurSeq / MAX_LOG_SIZE == self.m_TailSeq / MAX_LOG_SIZE
		
	def HasNext(self):
		return self.m_CurSeq % MAX_LOG_SIZE == 0 and self.m_CurSeq / MAX_LOG_SIZE <= self.m_TailSeq / MAX_LOG_SIZE
	
	def GetCurFilename(self):
		return self._GetFilenameBySeq(self.m_CurSeq)
	
	def GetTailFilename(self):
		return self._GetFilenameBySeq(self.m_TailSeq)
		
	def _GetFilenameBySeq(self, seq):
		return (DATA_FOLDER + "/" + "srvlog_" + str(seq / MAX_LOG_SIZE))

class CFlag(object):
	def __init__(self, size):
		self.m_Size = size
		self.m_Data = [FLAG_WAITTING] * size
		self.m_Tail = 0
		self.m_Head = 0

	def GetIndex(self):
		if self.Full():
			return -1
		ret = self.m_Tail
		self.m_Tail = (self.m_Tail + 1) % self.m_Size
		return ret

	def SetIndex(self, index):
		self.m_Data[index] = FLAG_FINISH

	def MoveHead(self):
		step = 0
		while not self.Empty():
			if self.m_Data[self.m_Head] == FLAG_FINISH:
				step += 1
				self.m_Head = (self.m_Head + 1) % self.m_Size
			else:
				break
		return step
	
	def Empty(self):
		return self.m_Head == self.m_Tail
		
	def Full(self):
		return (self.m_Tail +1) % self.m_Size == self.m_Head

def Log(data, e = None):
	if e:
		l = StrNow() + ": " + data + " error:" + str(e)
	else:
		l = StrNow() + ": " + data
	print l
		
def StrNow():
	sd = time.strftime('[%Y-%m-%d %H:%M:%S]',time.localtime(time.time()))
	return sd
		
def SplitSeq(line):
	seq = ""
	for s in line:
		if s == ":":
			break
		seq += s
	msg = line[len(seq) + 1:-1]
	return seq, msg
		
def LoadFile(nextFilename, offset = 0):
	Log("载入文件 %s" %nextFilename)
	global g_Queue
	try:
		file = open(nextFilename, "r")
	except IOError, e:
		Log("严重错误：载入文件%s 失败" %nextFilename, e)
		os.kill(os,getpid(), signal.SIGINT)
		return
	lines = file.readlines()
	lines = lines[offset:]
	for line in lines:
		try:
			seq, msg = SplitSeq(line)
		except Exception, e:
			Log("载入数据 %s 错误" %line, e)
			continue
		g_Queue.put((seq, msg))
	file.close()

def Int2Str(src, strLength):
	dst = ""
	length = len(str(src))
	if length < strLength:
		dst = "0" * (strLength - length) + str(src)
		return dst
	else:
		return str(src)[:strLength]
		
def SendToHttpServer(seq, msg):
	try:
		body = {"seq":seq, "log": msg}
		httpFp = urllib.urlopen(HTTP_URL, urllib.urlencode(body))
		responseData = httpFp.read()
		try:
			kv = json.loads(responseData)
			if kv["err"] == 0:
				return True
			elif kv["err"] == -2:
				Log("HTTP服务器：日志 %s:%s 已发送过，当前记录属于重发，请注意检查" %(seq, msg))
				return True
			else:
				Log("HTTP服务器：发送日志失败，返回 %s" %str(kv))
				return False
		except Exception, e:
			Log("HTTP服务器响应数据格式与预设不符 %s " %responseData, e)
			return False
		return False
	except IOError, e:
		Log("连接HTTP服务器失败", e)
		return False
	
def HttpSubThread():
	while g_Running:
		try:
			#非阻塞，防止SIGINT无法退出
			index, seq, msg = g_TaskQueue.get(block = False)
		except:
			time.sleep(1)
			continue
		success = SendToHttpServer(seq, msg)
		while not success:
			if not g_Running:
				return
			else:
				time.sleep(10)
				success = SendToHttpServer(seq, msg)
		g_Flags.SetIndex(index)

def PublishTask():
	while g_Running and not g_Queue.empty():
		index = g_Flags.GetIndex()
		if index == -1:
			return
		g_Flags.m_Data[index] = FLAG_WAITTING
		seq, msg = g_Queue.get()
		g_TaskQueue.put((index, seq, msg))
		
def HttpThreadFunc():
	Log("HTTP线程启动......")
	#开启消费线程
	threadPool = []
	for i in range(POOL_SIZE):
		t = threading.Thread(target = HttpSubThread, args = ())
		t.start()
		Log("HTTP子线程 %d 启动" %(i+1))
		threadPool.append(t)
	while g_Running:
		step = g_Flags.MoveHead()
		if step:
			g_StatusLock.acquire()
			g_Status.m_CurSeq += step
			g_Status.UpdateStatus()
			#检查文件末尾
			if g_Status.HasNext():
				nextFilename = g_Status.GetCurFilename()
				LoadFile(nextFilename)
				Log("当前准备转发文件 %s" %nextFilename)
			g_StatusLock.release()
		PublishTask()
		time.sleep(0.001)		
		#time.sleep(5)
	for t in threadPool:
		t.join()

def UnpackUnsigned(buff, size):
	n = 0
	size = min(len(buff), size)
	for i in xrange(size):
		n |= ord(buff[i]) << 8 * i
	
	return n, buff[size:]

def UnpackString(buff, length = 0):
	if length == 0:
		length = len(buff)
	
	s = buff[:length]
	n = s.find('\0')
	if n != -1:
		s = s[:n]
	return s, buff[length:]

def SplitPack(s):
	lst = []
	size = len(s)
	while size > 0:
		if ord(s[0]) == 0xff:
			if size < 4:
				break
			length = (ord(s[3])<<16) | (ord(s[2])<<8) | ord(s[1])
			head = 4
		else:
			length = ord(s[0])
			head = 1
		length += head
		
		if size < length:
			break
		cmd = ord(s[head + 2])
		m = s[head + 3:length]
		
		s = s[length:]
		size -= length
		lst.append((cmd, m))
	return lst, s

def Unpack(data):
	global g_Srvid
	logList = []
	lst, s = SplitPack(data)
	for cmd, pack in lst:
		if cmd == S2L_REGISTER:
			srvid, pack = UnpackUnsigned(pack, 4)
			if srvid == SERVER_ID:
				Log("服务器 %d 注册成功" %srvid)
				g_Srvid = srvid
			else:
				Log("严重错误：注册的srvid %d 与配置的srvid %d不符，游戏服连接关闭" %(srvid, SERVER_ID))
				return None, None
				
		elif cmd == S2L_LOGDATA:
			if g_Srvid == 0:
				Log("客户端尚未注册")
				return None, None
			kv = {}
			srvid, pack = UnpackUnsigned(pack, 4)
			if srvid != g_Srvid:
				Log("未知的服务器 %d 连接, 当前注册的服务器是 %d" %(srvid, g_Srvid))
				return None, None
				
			len, pack = UnpackUnsigned(pack, 4)
			str, pack = UnpackString(pack, len)
			logList.append(str)
		
	return logList, s

def StartTCP():
	global g_Srvid
	global g_Running
	
	g_Srvid = 0
	host = (TCP_BIND_IP, TCP_PORT)
	try:
		serverSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		serverSock.bind(host)
	except Exception, e:
		Log("服务器启动失败", e)
		g_Running = False
		return
	serverSock.listen(5)
	
	inputFd = [serverSock]
	sock2Data = {}
	tmpFileInstance = open(g_Status.GetTailFilename(), "a+", 0)
	tmpFileInstance.seek(0, 2)
	Log("TCP服务器启动 %s:%d...." %(TCP_BIND_IP, TCP_PORT))
	while g_Running:
		try:
			inputReady, outputReady, exceptReady = select.select(inputFd, [], [])
		except Exception, e:
			Log("select 错误", e)
			#bad file descriptor
			if e.args[0] == errno.EBADF:
				inputFd = [serverSock]
				continue
			else:
				g_Running = False
				break
		for fd in inputReady:
			if fd == serverSock:
				clientSock, addr = serverSock.accept()
				clientSock.setblocking(0)
				inputFd.append(clientSock)
				sock2Data[clientSock] = ""
			else:
				while g_Running:
					try:
						data = fd.recv(1024)
					except Exception, e:
						if e.args[0] == errno.EWOULDBLOCK:
							break
						else:
							Log("接收数据失败", e)
							fd.close()
							inputFd.remove(fd)
							sock2Data.pop(fd)
							g_Srvid = 0
							break
						
					if not data:
						Log("客户端连接断开")
						fd.close()
						inputFd.remove(fd)
						sock2Data.pop(fd)
						g_Srvid = 0
						break
					data = sock2Data[fd] + data
					logList, sock2Data[fd] = Unpack(data)
					#非法连接，直接关闭
					if logList == None and sock2Data[fd] == None:
						fd.close()
						inputFd.remove(fd)
						sock2Data.pop(fd)
						break
						
					g_StatusLock.acquire()
					for s in logList:
						seq = Int2Str(g_Srvid, 4) + Int2Str(g_Status.m_TailSeq, 16)
						#写文件
						tmpFileInstance.write(seq + ":" + s + "\n")
						#加入队列
						if g_Status.LocateFinFile():
							g_Queue.put((seq, s))
						#更新状态
						g_Status.m_TailSeq += 1
						g_Status.UpdateStatus()
						#尝试创建下一个文件
						if g_Status.m_TailSeq % MAX_LOG_SIZE == 0:
							nextFilename = g_Status.GetTailFilename()
							tmpFileInstance.close()
							tmpFileInstance = open(nextFilename, "a+", 0)
							Log("准备写入文件 %s" %nextFilename)
					g_StatusLock.release()
	
	for fd in inputFd:
		fd.close()
	tmpFileInstance.close()
	
def Recover():
	last = g_Status.GetCurFilename()
	offset = g_Status.m_CurSeq % MAX_LOG_SIZE
	Log("恢复上次转发状态, 文件名 %s 文件偏移（行） %d" %(last, offset))
	if g_Status.m_CurSeq == 0 and g_Status.m_TailSeq == 0:
		return
	LoadFile(last, offset = offset)

def Initial():
	global g_Queue
	global g_Status
	global g_StatusLock
	global g_Flags
	global g_TaskQueue
	global g_Running
	
	if not os.path.exists(DATA_FOLDER):
		os.mkdir(DATA_FOLDER)	
	if not os.path.exists(STATUS_FOLDER):
		os.mkdir(STATUS_FOLDER)
	
	g_Queue 	= Queue.Queue()
	g_Status 	= CStatus()
	g_StatusLock 	= thread.allocate_lock()
	g_TaskQueue 	= Queue.Queue(MAX_TASK_SIZE)
	g_Flags 	= CFlag(MAX_TASK_SIZE)
	g_Running	= True
	
	signal.signal(signal.SIGINT, OnDestroy)
	
	Log("Logsrv初始化: 已删除 %d， 已发送 %d， 已接收 %d" %(g_Status.m_HeadSeq, g_Status.m_CurSeq, g_Status.m_TailSeq))
	
def OnDestroy(signNum, frame):
	global g_Running
	g_Running = False
	Log("正在关闭logsrv.....")
	
def main(argv = None):
	Log("logsrv启动......")
	Initial()
	Recover()
	httpThread = threading.Thread(target = HttpThreadFunc, args = ())
	httpThread.start()
	StartTCP()
	
	httpThread.join()
	#强制更新数据
	g_Status.UpdateStatus()
	g_Status.m_Fp.close()
	g_Status.m_Bfp.close()
	Log("logsrv关闭成功。")
	
if __name__ == "__main__":
	main(sys.argv)
