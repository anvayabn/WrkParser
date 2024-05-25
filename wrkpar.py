'''
The wrkparser is a python program that basically automates the testing process by generating load
and then parsing the output of the wrk command to saving the output in the file. 

psuedo code

wrk command
wrk -t 10 -c 1-200 http://10.10.1.1:80/[64 - 16KB ]

wrk is set by the path
t = the number of threads 
c = number of connection [list from 1 - 200 connections]
/httppath - set by a list [list of data size to test]

wrk_parser -ip <ip_addr> -p <port> 

'''
# importing libraries 
# from distutils.util import execute
import datetime
import re
import subprocess
import sys
import pandas as pd
import os

# wrk path 
wrk_path = "/usr/bin/wrk"

# debug flag 
debug = False
debug_exe_print = False

# for work command 
threads = 10 
concurrency = [1, 50, 100, 150, 200]
data_sizes = [64, 1024, 4096, 8192, 16384]
repetitions = 10

#header
header = "data_size,concurrency,lat_avg,lat_stdev,lat_max,req_avg," +\
         "req_stdev,req_max,tot_requests,tot_duration,read," +\
         "err_connect,err_read,err_write,err_timeout,req_sec_tot," +\
         "read_tot\n"
         
#regex expressions 
"""
^ - begining of the line matching 
\s+ -  skip leading white space 
Latency - look for line matching this word

"""
latency_matching = "^\s+Latency\s+(\d+\.\d+\w*)\s+(\d+\.\d+\w*)\s+(\d+\.\d+\w*).*$"          
req_sec = "^\s+Req/Sec\s+(\d+\.\d+\w*)\s+(\d+\.\d+\w*)\s+(\d+\.\d+\w*).*$"
total_req = "^\s(\d+)\ request in (\d+\.\d+\w*)\,\s+(\d+\.\d+\w*)\ read.*$"
req_p_sec = "^Requests\/sec\:\s+(\d+\.*\d*).*$"
transfer_sec = "^Transfer\/sec\:\s+(\d+\.*\d*\w+).*$"
socket_error = "^\s+Socket errors:\ connect (\d+\w*)\,\ read (\d+\w*)\,\ write\ (\d+\w*)\,\ timeout\ (\d+\w*).*$"

#usage string
usage_string = "python3 wrkpar -ip <ip_addr> -p <port> \n"\
               "Options:\n"\
               "-ds <different data size>\n"
               
def print_usage():
    print("Error!! Please check usage\n")
    print(usage_string); 

def execute_cmd(command):
    if debug_exe_print:
        print("Running Command:\n", command)
    try:    
        process = subprocess.run(command.split(" "), check=True, 
                                stdout=subprocess.PIPE, universal_newlines=True, timeout=15)
        output = process.stdout
        word = output[0]
        if word == "unable":
            print("Error!! failed")
            return (-1, output)
        else :
            return (0, output)  
    except subprocess.TimeoutExpired:
        print("Cmd timeout !!\n")
        return (-1, "Time Error!\n") 

def wrk_cmd_const(hostaddress):
    cmd = wrk_path + " " + "-t" + str(
            threads) + " " + "-c" + "200 "  + hostaddress
    return cmd
def get_bytes(size_str):
    x = re.search("^(\d+\.*\d*)(\w+)$", size_str)
    if x is not None:
        size = float(x.group(1))
        suffix = (x.group(2)).lower()
    else:
        return size_str

    if suffix == 'b':
        return size
    elif suffix == 'kb' or suffix == 'kib':
        return size * 1024
    elif suffix == 'mb' or suffix == 'mib':
        return size * 1024 ** 2
    elif suffix == 'gb' or suffix == 'gib':
        return size * 1024 ** 3
    elif suffix == 'tb' or suffix == 'tib':
        return size * 1024 ** 3
    elif suffix == 'pb' or suffix == 'pib':
        return size * 1024 ** 4

    return False


def get_number(number_str):
    x = re.search("^(\d+\.*\d*)(\w*)$", number_str)
    if x is not None:
        size = float(x.group(1))
        suffix = (x.group(2)).lower()
    else:
        return number_str

    if suffix == 'k':
        return size * 1000
    elif suffix == 'm':
        return size * 1000 ** 2
    elif suffix == 'g':
        return size * 1000 ** 3
    elif suffix == 't':
        return size * 1000 ** 4
    elif suffix == 'p':
        return size * 1000 ** 5
    else:
        return size


def get_ms(time_str):
    x = re.search("^(\d+\.*\d*)(\w*)$", time_str)
    if x is not None:
        size = float(x.group(1))
        suffix = (x.group(2)).lower()
    else:
        return time_str

    if suffix == 'us':
        return size / 1000
    elif suffix == 'ms':
        return size
    elif suffix == 's':
        return size * 1000
    elif suffix == 'm':
        return size * 1000 * 60
    elif suffix == 'h':
        return size * 1000 * 60 * 60
    else:
        return size

    return False

# this function is going to parse the output and write to file
def parse_op_ato_file(op, concurrency, data_size, filename):
    retval = {}
    retval['data'] = data_size
    retval['concurrency'] = concurrency
    for line in op.splitlines():
        x = re.search(latency_matching, line)
        if x is not None:
            retval['lat_avg'] = get_ms(x.group(1))
            retval['lat_stdev'] = get_ms(x.group(2))
            retval['lat_max'] = get_ms(x.group(3))
        x = re.search(req_sec, line)
        if x is not None:
            retval['req_avg'] = get_number(x.group(1))
            retval['req_stdev'] = get_number(x.group(2))
            retval['req_max'] = get_number(x.group(3))
        x = re.search(total_req, line)
        if x is not None:
            retval['tot_requests'] = get_number(x.group(1))
            retval['tot_duration'] = get_ms(x.group(2))
            retval['read'] = get_bytes(x.group(3))        
        x = re.search(req_p_sec, line)
        if x is not None:
            retval['req_sec_tot'] = get_number(x.group(1))        
        x = re.search(transfer_sec, line)
        if x is not None:
            retval['read_tot'] = get_bytes(x.group(1))        
        x = re.search(socket_error, line)
        if x is not None:
            retval['err_connect'] = get_number(x.group(1))
            retval['err_read'] = get_number(x.group(2))
            retval['err_write'] = get_number(x.group(3))
            retval['err_timeout'] = get_number(x.group(4))            
    if 'err_connect' not in retval:
        retval['err_connect'] = 0
    if 'err_read' not in retval:
        retval['err_read'] = 0
    if 'err_write' not in retval:
        retval['err_write'] = 0
    if 'err_timeout' not in retval:
        retval['err_timeout'] = 0
    
    #process the dictionary     
    output_string = process_ret_val(retval)
    filename.write(output_string + "\n")
    return 

def process_ret_val(op):
    st = str(op.get('data')) + ',' + str(op.get('concurrency')) + ',' + str(op.get('lat_avg')) + ',' + str(
        op.get('lat_max')) + ',' + str(op.get('req_avg')) + ',' + str(
        op.get('req_stdev')) + ',' + str(op.get('req_max')) + ',' + str(
        op.get('tot_requests')) + ',' + str(op.get('tot_duration')) + ',' + str(
        op.get('read')) + ',' + str(op.get('err_connect')) + ',' + str(
        op.get('err_read')) + ',' + str(op.get('err_write')) + ',' + str(
        op.get('err_timeout')) + ',' + str(op.get('req_sec_tot')) + ',' + str(
        op.get('read_tot'))
    return st   

def main():
    # get  number of arguments 
    number_args = len(sys.argv)
    if number_args < 5:
        print_usage()
        exit()
    
    # check that second and fourth arg are -ip and -p 
    first_arg = str(sys.argv[1])
    third_arg = str(sys.argv[3])
    if first_arg != "-ip" or third_arg != "-p":
        print_usage()
        exit()
        
        
    # setting the host address
    ipaddress = str(sys.argv[2])
    port = str(sys.argv[4])     
    hostaddress ="http://" + ipaddress + ":" + port + "/"
    print("Running wrk test to host %s.....\n" % hostaddress)
    

    #perform warmup 
    print("Performing warmup....\n")
    #construct command
    command =  wrk_cmd_const(hostaddress)
    ret_val, ret_str = execute_cmd(command)
    if ret_val == -1:
        print("warm up failed !!!\n")
        print(ret_str)
        exit()
    else :
        if debug:
         print(ret_str)
    
    #create output file
    #manipulate the file with headers 
    now =  datetime.datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H%M%S")
    if debug:
        print(dt_string)
        
    filename = dt_string + ".csv"
    print("Data will be saved to:",filename)
    #openfile 
    file = open(filename,"a")
    file.write(header)
    
    #run the command and parse the output and add to csv 
    for ds in data_sizes:
        for conc in concurrency:
            for i in range(0, repetitions):
                host_address = hostaddress + str(ds)
                if debug :
                    print("Contacting host : %s : iteration no : %d" %(host_address, i))
                command = wrk_cmd_const(host_address)    
                ret_val, ret_str = execute_cmd(command)
                if ret_val == -1 :
                    print("Error: %s" %ret_str)
                    exit()
                parse_op_ato_file(ret_str, conc, ds, file)  
                
    #close file / cleanup 
    file.close()
        
if __name__ == '__main__' : 
    main()    