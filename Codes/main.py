# -*- coding = utf-8 -*-
# @Time : 2020-11-11 15:33
# @Author : Wenhao_Zhou
# @File : main.py
# @Software: PyCharm
import argparse
import math
import struct
import threading
import time
import zipfile
from os.path import join
from tqdm import tqdm
import os
from socket import *

file_names = {}
add_times = 0
finish = True
modified_file_name = ''
# Two UDP sockets to listen to the other two peers
clientInfo_Socket1 = socket(AF_INET, SOCK_DGRAM)
clientInfo_Socket1.settimeout(1)
clientInfo_Socket2 = socket(AF_INET, SOCK_DGRAM)
clientInfo_Socket2.settimeout(1)
serverInfo_socket = socket(AF_INET, SOCK_DGRAM)
# serverInfo_socket.settimeout(1)


def _argparse():
    print('parsing args...')
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", "-ip_addr", default='', help="ip addr of peers")
    arg = parser.parse_args()
    return arg


# Find the file added into the directory and compress them
def zip_new_file(file_dir, out_partial_name):
    global add_times
    global file_names
    global modified_file_name
    global finish

    while True:
        file_mtime_dict = get_file_mtime(file_dir)
        morefiles = (len(get_file_mtime(file_dir)) > file_listener1()[0]) and \
                    (len(get_file_mtime(file_dir)) > file_listener2()[0])
        equalfiles = (len(get_file_mtime(file_dir)) == file_listener1()[0]) and \
                     (len(get_file_mtime(file_dir)) == file_listener2()[0])

        # if new files are added into folder, and the current file number is bigger than peer side, add them
        if len(get_file_mtime(file_dir)) > len(file_names) and morefiles:
            finish = False
            print('Detect newly added files...')
            new_file_dict = {}
            server_addtimes = add_times + 1
            time.sleep(1)

            for new_file in list(get_file_mtime(file_dir).keys()):
                find = False
                for file in file_names:
                    if new_file == file:
                        find = True
                        break
                if not find:
                    new_file_dict.update({new_file: os.path.getmtime(new_file)})  # add new added files to newfileList

            file_names.update(new_file_dict)  # add new added files to the original filelist
            print('start compressing newly added files')
            # start to compress files
            zip_file = zipfile.ZipFile(out_partial_name + str(server_addtimes) + '.zip', 'w', zipfile.ZIP_DEFLATED)
            zip_start = time.time()

            # Compress newly added files and leave out root name
            for new_file in list(new_file_dict.keys()):
                print(new_file[8:])
                zip_file.write(new_file, new_file[8:])
            zip_file.close()
            zip_end = time.time()
            add_times += 1  # Update addtimes
            print('Finish compression, compression time: ' + str(zip_end - zip_start))
            finish = True

        # find modified file and compress it to zip directory
        # If modified file name is empty and file number is equal to peers and other processes are not writing files
        # in share directory
        if modified_file_name == '' and finish and equalfiles:
            time.sleep(1)
            file_mtime_dict1 = get_file_mtime(file_dir)
            time.sleep(1)
            file_mtime_dict2 = get_file_mtime(file_dir)
            find = False
            modified_file = ''

            # iterate through the file dictionary and new file dictionary to find the file with changed mtime
            # If the file mtime is not continuously changed which indicate it is not a extracting file,
            # then broadcast to peers, compress it, and add addtimes by 1.
            for file in file_mtime_dict1.keys():
                if file_mtime_dict1[file] != file_mtime_dict2[file]:
                    find = True
                    modified_file = str(file)
                    # update modify time of the partially changed file
                    file_mtime_dict1[file] = file_mtime_dict2[file]
            if find:
                server_addtimes = add_times + 1
                print(modified_file + " has been modified, start to zip it...")
                modified_file_name = modified_file
                # send peers news that the updated file has been found
                clientInfo_Socket1.sendto(modified_file.encode(), (peer_addr1, peer_info_port1))
                clientInfo_Socket2.sendto(modified_file.encode(), (peer_addr2, peer_info_port2))

                zip_file = zipfile.ZipFile(out_partial_name + str(server_addtimes) + '.zip', 'w', zipfile.ZIP_DEFLATED)
                zip_start = time.time()
                # Compress modified file and leave out root name
                zip_file.write(modified_file, modified_file[8:])
                zip_file.close()
                zip_end = time.time()
                add_times += 1  # update addtimes
                print('Finish compressing modified file, compression time: ' + str(zip_end - zip_start))


def get_file_mtime(file_dir):
    file_mtime = {}
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            file_mtime.update({os.path.join(root, file): os.path.getmtime(os.path.join(root, file))})
    return file_mtime


# file_listener1() and file_listener2() are used to check the file information in peer folders...
# if the folder already have the file in the server folder, do not compress them
# apply UDP to broadcast and obtain the fileList along with the file numbers and addtimes in server side
def file_listener1():  # File listener of machine1
    global add_times
    global file_names
    try:
        # Try to connect peer #1
        clientInfo_Socket1.sendto(''.encode(), (peer_addr1, peer_info_port1))
        return_msg, _ = clientInfo_Socket1.recvfrom(20480)
    except:
        time.sleep(1)
        online = False
        # print("Device 1 is not connected...")
        return -1, [], -1, online
    else:
        # parse information
        # print('Connect to device 1...')
        online = True
        file_num_b = return_msg[:4]
        addtimes_b = return_msg[4:8]
        file_mtime_dict_b = return_msg[8:]
        file_num = struct.unpack('!I', file_num_b)[0]
        # print('file number is: ' + str(file_num))
        peer_addtimes = struct.unpack('!I', addtimes_b)[0]
        # print("peer addtimes is: " + str(addtimes))
        file_mtime_dict = eval(file_mtime_dict_b.decode('utf-8'))  # reform the fileList information
        # print('file_list dict is: ' + str(file_mtime_dict))
        return file_num, file_mtime_dict, peer_addtimes, online


# File listener of peer2
def file_listener2():
    global add_times
    global file_names
    try:
        # Try to connect peer #2
        clientInfo_Socket2.sendto(''.encode(), (peer_addr2, peer_info_port2))
        return_msg, _ = clientInfo_Socket2.recvfrom(20480)
    except:
        time.sleep(1)
        online = False
        # print("Device 2 is not connected...")
        return -1, [], -1, online
    else:
        # parse information
        # print('Connect to device 2...')
        online = True
        file_num_b = return_msg[:4]
        addtimes_b = return_msg[4:8]
        file_mtime_dict_b = return_msg[8:]
        file_num = struct.unpack('!I', file_num_b)[0]
        # print('file number is: ' + str(file_num))
        peer_addtimes = struct.unpack('!I', addtimes_b)[0]
        # print("peer addtimes is: " + str(addtimes))
        file_mtime_dict = eval(file_mtime_dict_b.decode('utf-8'))  # reform the fileList information
        return file_num, file_mtime_dict, peer_addtimes, online


def file_downloader():
    global add_times
    global file_names
    while True:
        # If peer 1 is online
        if file_listener1()[3]:
            peer_addtimes = file_listener1()[2]
            peer_file_num = file_listener1()[0]
            peer_file_list = dict(file_listener1()[1])
            # print(peer_file_list)
            # If the number of files in peer1 side is more than that in this machine
            if peer_addtimes > add_times and peer_file_num > len(get_file_mtime(source_dir)) and len(file_names) != 0:
                print('Detect files added in peer 1...')
                # update addtimes and filenames
                file_names = peer_file_list
                add_times = peer_addtimes
                # Start a client thread to download the zip file
                cli = Client(peer_addr1, peer_port1, download_dir, 'zip' + str(add_times) + '.zip')
                cli.start()

            # If peer addtimes is larger than this side and file list length is equal, then retrieve the updated file
            if peer_addtimes > add_times and peer_file_num == len(get_file_mtime(source_dir)) and len(file_names) != 0:
                print('Detect file update in peer 1...')
                # update addtimes
                add_times = peer_addtimes
                # start a client to retrieve the updated file
                cli = Client(peer_addr1, peer_port1, download_dir, 'zip' + str(add_times) + '.zip')
                cli.start()

            # If this app has been killed and restart again, synchronize the file list and addtimes from peer1
            if peer_addtimes > add_times and len(file_names) == 0:
                print("Restart app and reconnected to peer 1...")
                # Update addtimes and filenames to synchronize with peer1
                add_times = peer_addtimes
                file_names = peer_file_list
                print("Files in peer2 are: " + str(peer_file_list.keys()) + " addtimes is: " + str(peer_addtimes))

                # If the number of files is smaller than that in the peer1 side, than retrieve the latest zip file
                if len(get_file_mtime(source_dir)) < len(file_names):
                    print('Start retrieving the latest added files in peer1 since last offline...')
                    cli = Client(peer_addr1, peer_port1, download_dir, 'zip' + str(add_times) + '.zip')
                    cli.start()
                else:
                    print('Files are up to date comparing to peer1...')

        # If peer2 is online, then ask for file download requests
        if file_listener2()[3]:
            peer2_addtimes = file_listener2()[2]
            peer2_file_num = file_listener2()[0]
            peer2_file_list = dict(file_listener2()[1])

            # If the number of files in peer2 side is more than that in this machine
            if peer2_addtimes > add_times and peer2_file_num > len(get_file_mtime(source_dir)) and len(file_names) != 0:
                print('Detect files added in peer 2...')
                # update addtimes and filenames
                file_names = peer2_file_list
                add_times = peer2_addtimes
                # Start a client thread to download the zip file
                cli = Client(peer_addr2, peer_port2, download_dir, 'zip' + str(add_times) + '.zip')
                cli.start()

            # If peer addtimes is larger than this side and file list length is equal, then retrieve the updated file
            if peer2_addtimes > add_times and peer2_file_num == len(get_file_mtime(source_dir)) and len(
                    file_names) != 0:
                print('Detect file update in peer 2...')
                # update addtimes
                add_times = peer2_addtimes
                # start a client to retrieve the updated file
                cli = Client(peer_addr2, peer_port2, download_dir, 'zip' + str(add_times) + '.zip')
                cli.start()

            # If this app has been killed and restart again, synchronize the file list and addtimes from peer1
            if peer2_addtimes > add_times and len(file_names) == 0:
                print("Restart app and reconnected to peer 2...")
                # Update addtimes and filenames to synchronize with peer1
                add_times = peer2_addtimes
                file_names = peer2_file_list
                print("Files in peer2 are: " + str(peer2_file_list.keys()) + " addtimes is: " + str(peer2_addtimes))

                # If the number of files is smaller than that in the peer1 side, than retrieve the latest zip file
                if len(get_file_mtime(source_dir)) < len(file_names):
                    print('Start retrieving the latest added files in peer2 since last offline...')
                    cli = Client(peer_addr2, peer_port2, download_dir, 'zip' + str(add_times) + '.zip')
                    cli.start()
                else:
                    print('Files are up to date comparing to peer2...')


# send file information to client side
def send_file_info():
    global modified_file_name
    print('start sending file information...')
    serverInfo_socket.bind(('', serverInfo_port))
    while True:
        bin_file_mtime = str(get_file_mtime(source_dir)).encode('utf-8')
        try:
            modified_file_name_b, peer_address = serverInfo_socket.recvfrom(102400)
            if modified_file_name_b == b'':
                serverInfo_socket.sendto(struct.pack('!II', len(get_file_mtime(source_dir)), add_times)
                                     + bin_file_mtime, peer_address)

            # if serverInfo_socket receives modified file information, update it
            else:
                modified_file_name = modified_file_name_b.decode()
                print('receive modified file name ' + str(modified_file_name))
        except:
            print('One peer is off line')


class Client(threading.Thread):

    def __init__(self, server_address, server_port, file_dir, filename):
        threading.Thread.__init__(self)
        self.client_socket = socket(AF_INET, SOCK_DGRAM)
        self.file_dir = file_dir
        self.server_address = server_address
        self.server_port = server_port
        self.filename = filename

    def make_get_file_information_header(self, filename):
        operation_code = 0
        header = struct.pack('!I', operation_code)
        header_length = len(header + filename.encode())
        return struct.pack('!I', header_length) + header + filename.encode()

    def make_get_fil_block_header(self, filename, block_index):
        operation_code = 1
        header = struct.pack('!IQ', operation_code, block_index)
        header_length = len(header + filename.encode())
        return struct.pack('!I', header_length) + header + filename.encode()

    def parse_file_information(self, msg):
        header_length_b = msg[:4]
        header_length = struct.unpack('!I', header_length_b)[0]
        header_b = msg[4:4 + header_length]
        client_operation_code = struct.unpack('!I', header_b[:4])[0]
        server_operation_code = struct.unpack('!I', header_b[4:8])[0]
        if server_operation_code == 0:  # get right operation code
            file_size, block_size, total_block_number = struct.unpack('!QQQ', header_b[8:32])
            # md5 = header_b[32:].decode()
        else:
            file_size, block_size, total_block_number = -1, -1, -1

        return file_size, block_size, total_block_number

    def parse_file_block(self, msg):
        header_length_b = msg[:4]
        header_length = struct.unpack('!I', header_length_b)[0]
        header_b = msg[4:4 + header_length]
        client_operation_code = struct.unpack('!I', header_b[:4])[0]
        server_operation_code = struct.unpack('!I', header_b[4:8])[0]

        if server_operation_code == 0:  # get right block
            block_index, block_length = struct.unpack('!QQ', header_b[8:24])
            file_block = msg[4 + header_length:]
        elif server_operation_code == 1:
            block_index, block_length, file_block = -1, -1, None
        elif server_operation_code == 2:
            block_index, block_length, file_block = -2, -2, None
        else:
            block_index, block_length, file_block = -3, -3, None

        return block_index, block_length, file_block

    def get_file_size(self, filename):
        return os.path.getsize(join(self.file_dir, filename))

    def unzipFiles(self, addtimes):
        global add_times
        global file_names
        global finish
        print('Start extracting files...')
        startTime = time.time()
        file_path = './download/zip'
        des_dir = './share'

        # If there exits the required file unzip it
        if os.path.exists(file_path + str(addtimes) + '.zip'):
            zipFile = zipfile.ZipFile(file_path + str(addtimes) + '.zip', 'r')
            for file in zipFile.namelist():  # extract files in the download folder
                zipFile.extract(file, des_dir)
            zipFile.close()

            endTime = time.time()
            print('Extracting time is: ' + str(endTime - startTime))
            print('Client end. ')
            finish = True
        else:
            print('Requested file is in another peer...')
            # Refresh add_times and file_names
            add_times = add_times - 1
            file_names = ''

    def run(self):
        global finish
        print('Download start!')
        finish = False
        startTime = time.time()

        # Get file information
        print('start to download the ' + str(self.filename[-5]) + ' times added file...')

        self.client_socket.sendto(self.make_get_file_information_header(self.filename),
                                  (self.server_address, self.server_port))
        msg, _ = self.client_socket.recvfrom(102400)
        file_size, block_size, total_block_number = self.parse_file_information(msg)

        if file_size > 0:
            print('Filename:', self.filename)
            print('File size:', file_size)
            print('Block size:', block_size)
            print('Total block:', total_block_number)
            # print('MD5:', md5)

            # Creat a file
            f = open(join(self.file_dir, self.filename), 'wb')

            # Start to get file blocks
            for block_index in tqdm(range(total_block_number)):
                self.client_socket.sendto(self.make_get_fil_block_header(self.filename, block_index),
                                          (self.server_address, self.server_port))
                msg, _ = self.client_socket.recvfrom(block_size + 100)
                block_index_from_server, block_length, file_block = self.parse_file_block(msg)
                f.write(file_block)
            f.close()

            # Check the MD5
            file_size_download = self.get_file_size(self.filename)
            if file_size_download == file_size:
                print('Downloaded file is completed.')
            else:
                print('Downloaded file is broken.')
        else:
            print('No such file.')
        endTime = time.time()
        print('Transfer time: ' + str(endTime - startTime))
        if self.filename[-4:] == '.zip':
            self.unzipFiles(self.filename[-5:-4])


class Server(threading.Thread):

    def __init__(self, file_dir, block_size, server_Uport):
        threading.Thread.__init__(self)
        self.server_socket = socket(AF_INET, SOCK_DGRAM)
        self.server_Uport = server_Uport
        self.file_dir = file_dir
        self.block_size = block_size

    def get_file_size(self, filename):
        return os.path.getsize(join(self.file_dir, filename))

    def get_file_block(self, filename, block_index):
        # global block_size
        f = open(join(self.file_dir, filename), 'rb')
        f.seek(block_index * self.block_size)
        file_block = f.read(self.block_size)
        f.close()
        return file_block

    def make_return_file_information_header(self, filename):
        # global block_size
        if os.path.exists(join(self.file_dir, filename)):  # find file and return information
            client_operation_code = 0
            server_operation_code = 0
            file_size = self.get_file_size(filename)
            total_block_number = math.ceil(file_size / self.block_size)
            # md5 = self.get_file_md5(filename)
            header = struct.pack('!IIQQQ', client_operation_code, server_operation_code,
                                 file_size, self.block_size, total_block_number)
            header_length = len(header)
            print(filename, file_size, total_block_number)
            return struct.pack('!I', header_length) + header

        else:  # no such file
            client_operation_code = 0
            server_operation_code = 1
            header = struct.pack('!II', client_operation_code, server_operation_code)
            header_length = len(header)
            return struct.pack('!I', header_length) + header

    def make_file_block(self, filename, block_index):
        file_size = self.get_file_size(filename)
        total_block_number = math.ceil(file_size / self.block_size)

        if os.path.exists(join(self.file_dir, filename)) is False:  # Check the file existence
            client_operation_code = 1
            server_operation_code = 1
            header = struct.pack('!II', client_operation_code, server_operation_code)
            header_length = len(header)
            return struct.pack('!I', header_length) + header

        if block_index < total_block_number:
            file_block = self.get_file_block(filename, block_index)
            client_operation_code = 1
            server_operation_code = 0
            header = struct.pack('!IIQQ', client_operation_code, server_operation_code, block_index, len(file_block))
            header_length = len(header)
            # print(filename, block_index, len(file_block))
            return struct.pack('!I', header_length) + header + file_block
        else:
            client_operation_code = 1
            server_operation_code = 2
            header = struct.pack('!II', client_operation_code, server_operation_code)
            header_length = len(header)
            return struct.pack('!I', header_length) + header

    def msg_parse(self, msg):
        header_length_b = msg[:4]
        header_length = struct.unpack('!I', header_length_b)[0]
        header_b = msg[4:4 + header_length]
        client_operation_code = struct.unpack('!I', header_b[:4])[0]

        if client_operation_code == 0:  # get file information
            filename = header_b[4:].decode()
            return self.make_return_file_information_header(filename)

        if client_operation_code == 1:
            block_index_from_client = struct.unpack('!Q', header_b[4:12])[0]
            filename = header_b[12:].decode()
            return self.make_file_block(filename, block_index_from_client)

        # Error code
        server_operation_code = 400
        header = struct.pack('!II', client_operation_code, server_operation_code)
        header_length = len(header)
        return struct.pack('!I', header_length) + header

    def run(self):
        print('Service start!')
        self.server_socket.bind(('', self.server_Uport))
        while True:
            # self.get_fileList_info()
            msg, client_address = self.server_socket.recvfrom(10240)  # Set buffer size as 10kB
            return_msg = self.msg_parse(msg)
            self.server_socket.sendto(return_msg, client_address)


if __name__ == '__main__':
    start = time.time()
    args = _argparse()
    peer_addr1 = args.ip.split(',')[0]
    print(str(peer_addr1))
    peer_port1 = 22001
    peer_info_port1 = 22002
    peer_addr2 = args.ip.split(',')[1]
    print(str(peer_addr2))
    peer_port2 = 22001
    peer_info_port2 = 22002
    download_dir = 'download'
    server_file_dir = 'zip'
    block_size = 20480
    server_port = 22001
    serverInfo_port = 22002
    source_dir = './share'
    zip_dir = './zip/zip'

    if not os.path.exists('share'):
        os.makedirs('share')
    if not os.path.exists('download'):
        os.makedirs('download')
    if not os.path.exists('zip'):
        os.makedirs('zip')

    # initialize a server obj
    server = Server(server_file_dir, block_size, server_port)
    server.start()

    # Initialize a file detector
    newFile_detector = threading.Thread(target=zip_new_file, args=(source_dir, zip_dir,))
    newFile_detector.start()
    # Initialize a file information sender
    file_info_sender = threading.Thread(target=send_file_info)
    file_info_sender.start()
    # Initialize a file downloader to listen to peer1
    file_downloader = threading.Thread(target=file_downloader())
    file_downloader.start()