##可靠的数据转发
数据转发，业务服务器->转发中间件->统计中心。要求数据可靠，传输可靠，不能遗漏信息，尽量高效

###设计
建立tcp服务器接收server发送的数据信息，tcp服务器将这一信息发布到队列当中，http客户端是由一系列的线程组成，不断的从日志队列当中取出信息，然后发送到统计中心，由于要保证数据的有序性，线程池取不能随机进行取

###处理流程
* tcp服务器接收日志数据
* 日志数据落地，落地文件包含多个文件
* 保存已接受数据包个数，目的是重启能够恢复
* 将日志数据放入消息队列
* http客户端父线程将数据取出，投入到与子线程通信的队列q2当中
* 子线程将信息提交到统计中心
* http客户端父线程修改已转发状态，存盘。

比较关键的两点：

* 写入状态信息失败，为了解决写入状态失败的情况，引入了副本机制，而引入副本机制能够判别状态的真伪，但是这一判别的过程需要依赖本身的数据对比，所以针对每个文件写入一个校验值，如果校验值通过，则必为真实的数据，校验值校验不通过，则可能是在存盘的时候发生了异常退出。
```
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
```
* 已转发数量的更新必须与落地当中转发的行数一致，会出现类似这样的一种情形，需要转发N+1条数据，后面的N条数据都转发了，但是第一条数据没转发，这种情况不能修改已转发的数量。为了解决这一情况，引入了一个环形的flag缓冲队列，每次转发完成，修改对应的flag，在父线程结算已转发数量时，取前K个flag被标志为已转发状态的作为增加值。
```
def MoveHead(self):
        step = 0
        while not self.Empty():
            if self.m_Data[self.m_Head] == FLAG_FINISH:
                step += 1
                self.m_Head = (self.m_Head + 1) % self.m_Size
            else:
                break
        return step

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

    for t in threadPool:
        t.join()
```
