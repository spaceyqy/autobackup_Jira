'''
采用ftplib完成文件上传和远程服务器中文件的删除
此程序维护两份数据（无论何时运行均可，鲁棒性强）：
1.JIRA服务器中保留80个文件包，多余的删除掉
2.备份的FTP服务器中一直保留最新的6个文件包

执行算法：
Begin
1.获取本地目录清单L1，找出含有多少个.zip包，前缀是以”2“开头；
  注释：里面还有一种包是"structure-20211130-0300.zip"形式，容量比较小暂不用备份和管理
2.L1从大到小排序，if len(L1) > 80 : 删除排序最小的len(L1)-80个zip包
3.提取排序最大的6个数据包形成列表L
4.获取远程目录清单L2，找出含有多少个.zip包
5.对L中每个elem，判断是否存在于L2中，如果不存在则调用上传程序uploadfile上传到远程目录
6.再次获取远程目录清单L2_new，找出含有多少个.zip包
7.L2_new从大到小排序, if len(L2_new) > 6 Then delete 排序最小的len(L2_new)-6个包
END
'''
from ftplib import FTP, error_perm
import socket
import os
import time

def ftpconnect(host, port, username, password):
    ftp = FTP()
    # ftp.set_debuglevel(2)         #打开调试级别2，显示详细信息
    ftp.encoding = 'utf-8'  # 解决中文编码问题，默认是latin-1
    try:
        ftp.connect(host, port)  # 连接
        ftp.login(username, password)  # 登录，如果匿名登录则用空串代替即可
    except(socket.error, socket.gaierror):  # ftp 连接错误
        print("ERROR: cannot connect [{}:{}]" .format(host, port), file=backup_log_f)
        return None
    except error_perm:  # 用户登录认证错误
        print("ERROR: user Authentication failed ", file=backup_log_f)
        return None
    return ftp

def uploadfile(ftp, remotepath, localpath):
    '''
    上传文件
    :param ftp:
    :param remotepath:远程服务器文件路径
    :param localpath:本地文件路径
    :return:
    '''
    bufsize = 1024
    fp = open(localpath, 'rb')
    res = ftp.storbinary('STOR ' + remotepath, fp, bufsize)  # 上传文件
    if res.find('226') != -1:
        print('upload file complete', remotepath, file=backup_log_f)
    ftp.set_debuglevel(0)
    fp.close()

def deleteFileFromList(Dir, filelist, local=True, ftp=""):
    '''
    :param Dir:文件路径
    :param filelist:删除文件列表
    :param local:是否为本地删除，True为是，False为远程删除
    :param ftp:仅当远程删除时需要提供
    :return:
    '''
    if len(filelist) > 0:
        for f in filelist:
            filepath = os.path.join(Dir, f)
            if filepath.endswith('.zip'):
                if local==True:
                    os.remove(filepath)
                    print("Local: " + filepath + " was removed!", file=backup_log_f)
                else:
                    ftp.delete(filepath)
                    print("Remote: " + filepath + " was removed!", file=backup_log_f)

if __name__ == "__main__":
    # 备份服务器配置
    host = "10.19.5.93"
    port = 21
    username = "test"
    password = "123456"
    ftp = ftpconnect(host, port, username, password)
    localFilepath = "D:/jira_data/export/" #待备份的文件本地存放目录
    remoteFilepath = "/JIRA_backup/" #备份服务器远程ftp地址目录

    # 备份数据包设置
    mainFileNum = 80#主服务器中保留的数据包数量
    backupFileNum = 6#备用服务器中保留的数据包数量

    # 运行日志设置
    backup_log_f = open("./backup_log.txt", 'a')  # 直接打开一个文件用于追加，如果文件不存在则创建日志文件
    print("-------------------"+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"-------------------", file=backup_log_f)#添加时间戳

    # ------------Begin-------------------
    # 1.获取本地目录清单L1，找出含有多少个.zip包，前缀是以”2“开头；
    localFilelist = os.listdir(localFilepath)
    localFilelist = [elem for elem in localFilelist if elem.startswith('2') and elem.endswith('.zip')]

    # 2.L1从大到小排序，if len(L1) > mainFileNum : 删除排序最小的len(L1)-mainFileNum个zip包
    localFilelist.sort(reverse=True)
    deletedFilelist = []

    if len(localFilelist) > mainFileNum:
        deleteFileNum = len(localFilelist)-mainFileNum#要在主服务器清除的文件数量
        deletedFilelist = localFilelist[-1 : -deleteFileNum-1 : -1]#截取排序最小的deleteFileNum个数据

    deleteFileFromList(localFilepath, deletedFilelist, local=True)#删除本地多余文件

    # 3.提取排序最大的backupFileNum个数据包形成列表L
    L = localFilelist[0:backupFileNum]

    # 4.获取远程目录清单L2，找出含有多少个以2开头的.zip包
    ftp.cwd(remoteFilepath)#切换工作目录
    ftpFilelist = ftp.nlst()
    ftpFilelist = [elem for elem in ftpFilelist if elem.startswith('2') and elem.endswith('.zip')]

    # 5.对L中每个elem，判断是否存在于L2中，如果不存在则调用上传程序uploadfile上传到远程目录
    for elem in L:
        if elem not in ftpFilelist:
            uploadfile(ftp, os.path.join(remoteFilepath, elem), os.path.join(localFilepath, elem))  # 上传文件

    # 6.再次获取远程目录清单L2_new，找出含有多少个.zip包
    ftpFilelist = ftp.nlst()
    ftpFilelist = [elem for elem in ftpFilelist if elem.startswith('2') and elem.endswith('.zip')]

    # 7.L2_new从大到小排序,if len(L2_new) > 6 Then delete 排序最小的len(L2_new)-6个包
    ftpFilelist.sort(reverse=True)
    deletedFilelist = []
    if len(ftpFilelist) > backupFileNum:
        deleteFileNum = len(ftpFilelist)-backupFileNum#要在备份服务器清除的文件数量
        deletedFilelist = ftpFilelist[-1 : -deleteFileNum-1 : -1]#截取排序最小的deleteFileNum个数据
    deleteFileFromList(remoteFilepath, deletedFilelist, local=False, ftp=ftp)

    backup_log_f.close
    ftp.quit()