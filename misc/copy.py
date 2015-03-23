# -*- coding: utf-8 -*-
#!/bin/python

import os
import paramiko
from paramiko import SSHClient
from random import shuffle
import multiprocessing
import math
import re
import time

global ssh

import os, errno

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

# DISKS = ['/data1', '/data2', '/data3', '/data4']
DISKS = ['./data1', './data2', './data3', './data4']
CLIENT = 'iusacell'


regexps = [
    # Formato para 2013
    '(.*)/(?P<file_type_1>[A-Za-z_\s]*)/(?P<year>20\d{2})/(?P<month>0?[1-9]|1[012])_?(?P<file_type_2>geo)?/(?:[A-Za-z_\s]*)\d{2}[-|_](?P<day>\d{2})[^\.]*\.(?P<extension>.*)',

    
    '(.*)/(?P<file_type_1>[A-Za-z_\s]*)/(?P<year>\d{4})/(?P<month>0?[1-9]|1[012])(?:_geo)?/(?:[A-Za-z_\s]*)[^\.]*\.(?P<extension>.*)',
]

def download_init(mgr):
    global ssh
    ssh = connect_ssh(mgr.host, mgr.port, username = mgr.username)

def parse_file(f):
    for regex in regexps:
        m = re.match(regex, f)

        if m:
            return m.groupdict()

    return None

    
def download(args):
    global ssh

    disk, files = args 

    time.sleep(0.1)   # Por algún motivo, si no hago esto, no crea nuevos procesos.

    print 'In process: %s' % multiprocessing.current_process()
    
    for f in files:
        file_data = parse_file(f)

        if not file_data:
            print 'Warning: file %s does not match' % f
            continue

        file_data['client'] = CLIENT
        file_data['disk'] = disk

        
        file_type = file_data['file_type_1']
        del file_data['file_type_1']

        i = 2
        while 'file_type_%s' % i in file_data:
            if file_data['file_type_%s' % i] is not None:
                file_type += '_' + file_data['file_type_%s' % i]

            del file_data['file_type_%s' % i]
            i += 1

        file_data['file_type'] = file_type
        file_data = dict((k.lower(), v.lower()) for k,v in file_data.iteritems())
        
        local_dir = '{disk}/{client}/{year}/{month:0>2}/{file_type}'.format(**file_data)
        mkdir_p(local_dir)

        file_dest = '%s_%04d%02d' % (file_data['file_type'], int(file_data['year']), int(file_data['month']))
        if 'day' in file_data:
            file_dest += '%02d' % int(file_data['day'])

        file_dest += '.' + file_data['extension']
        
        file_dest = os.path.join(local_dir, file_dest)

        print 'Transfering: %s => %s (%s)' % (f, file_dest, multiprocessing.current_process())

        def show_progress(so_far, total):
            if total > 0:
                print 'Transfering: %s => %s (%s) | (%0.2f %%)' % (f, file_dest, multiprocessing.current_process(), (((so_far * 1.0) / total)) * 100)
        sftp = paramiko.SFTPClient.from_transport(ssh.get_transport())
        sftp.get(f, file_dest, callback=show_progress)
        


def connect_ssh(host, port, username):

    agent = paramiko.Agent()
    agent_keys = agent.get_keys() 
    if len(agent_keys) == 0:
        return

    for key in agent_keys:
        print 'Trying ssh-agent key %s' % key.get_fingerprint().encode('hex'),
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
        
            ssh.connect(host, port, username = username, pkey = key)

            print '... success!'
            return ssh
        except paramiko.SSHException, e:
            print '... failed!', e


class DownloaderManager(object):
    def __init__(self, host, port, username):
        self.host = host
        self.port = port
        self.username = username
        
    def download_files(self, glob, disks):


        ls_ssh = connect_ssh(self.host, self.port, self.username)

        workers = multiprocessing.Pool(processes=len(disks), initializer=download_init, initargs=[self])

        (stdin, stdout, stderr) = ls_ssh.exec_command('ls -d %s' % glob)
        
        input_files = stdout.read().split('\n')
        ls_ssh.close()

        shuffle(input_files)

        # print input_files
        
        splitted_files = [(disks[i], input_files[i::len(disks)]) for i in range(len(disks))]

        
        results = workers.imap(download, splitted_files)
        for r in results:
            print '\t', r
    
def main():    
    remote_files_host = '199.193.117.2'
    remote_files_port = 22
    remote_files_user = 'run-im'


    downloader = DownloaderManager(remote_files_host, 
        remote_files_port, 
        remote_files_user)
    
    downloader.download_files('/gd/im/data/*/*/*/*', DISKS)


if __name__ == "__main__":
    main()

