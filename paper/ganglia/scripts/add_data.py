'''
paper.ganglia.scripts.add_data

adds data to ganglia database by polling host machines

author | Immanuel Washington

Functions
---------
two_round | rounds to two decimal places
filesystem | gathers info for table
iostat | gathers info for table
ram_free | gathers info for table
cpu_perc | gathers info for table
add_data | adds data to ganglia database
'''
from __future__ import division
import os
import sys
import psutil
import time
import uuid
import paper as ppdata
from paper.ganglia import dbi as pyg

def two_round(num):
    '''
    rounds value to two decimal places

    Parameters
    ----------
    num (int/float/string): number

    Returns
    -------
    float(2): number
    '''
    return round(float(num), 2)

def filesystem(ssh, host, path):
    '''
    generates table information for filesystem table

    Parameters
    ----------
    ssh | object: ssh object
    host | str: host of filesystem
    path | str: path to search

    Returns
    -------
    dict: filesystem information
    '''
    if ssh is None:
        fi = psutil.disk_usage(path)
        total = fi.total
        used = fi.used
        free = fi.free
        percent = fi.percent
    else:
        _, folio, _ = ssh.exec_command('df -B 1')

        for output in folio.splitlines():
            filesystem = output.split()[-1]
            if filesystem in (path,):
                total = int(output.split()[-4])
                used = int(output.split()[-3])
                free = int(output.split()[-2])
                percent = int(output.split()[-1].split('%')[-1])

    system_data = {'host': host,
                    'system': path,
                    'total_space': total,
                    'used_space': used,
                    'free_space': free,
                    'percent_space': percent,
                    'filesystem_id': str(uuid.uuid4()),
                    'timestamp': int(time.time())}

    return system_data

def iostat(ssh, host):
    '''
    generates table information for iostat table

    Parameters
    ----------
    ssh | object: ssh object
    host | str: host of filesystem

    Returns
    -------
    dict: iostat information
    '''
    timestamp = int(time.time())
    iostat_data = {}
    if ssh is None:
        io = psutil.disk_io_counters(perdisk=True)

        for device, value in io.items():
            tps = None
            read_s = two_round(value.read_count / value.read_time)
            write_s = two_round(value.write_count / value.write_time)
            bl_reads = value.read_bytes
            bl_writes = value.write_bytes

            iostat_data[device] = {'host': host,
                                    'device': device,
                                    'tps': tps,
                                    'read_s': read_s,
                                    'write_s': write_s,
                                    'bl_reads': bl_reads,
                                    'bl_writes': bl_writes,
                                    'iostat_id': str(uuid.uuid4()),
                                    'timestamp': timestamp}

    else:
        _, folio, _ = ssh.exec_command('iostat')

        folio_use = []
        folio_name = folio.splitlines()[0].split()[2].strip('()')
        devices = ('sda', 'sda1', 'sda2','dm-0', 'dm-1')
        for output in folio.splitlines():
            device = output.split()[0]
            if device in devices:
                line = output[:].split()
                new_line = filter(lambda a: a not in [''], line)
                folio_use.append(new_line)

        #Convert all numbers to floats, keep words as strings
        for row in folio_use:
            device = row[0]
            tps = two_round(row[1])
            read_s = two_round(row[2])
            write_s = two_round(row[3])
            bl_reads = int(row[4])
            bl_writes = int(row[5])

            iostat_data[device] = {'host': host,
                                    'device': device,
                                    'tps': tps,
                                    'read_s': read_s,
                                    'write_s': write_s,
                                    'bl_reads': bl_reads,
                                    'bl_writes': bl_writes,
                                    'iostat_id': str(uuid.uuid4()),
                                    'timestamp': timestamp}

    return iostat_data

def ram_free(ssh, host):
    '''
    generates table information for ram table

    Parameters
    ----------
    ssh | object: ssh object
    host | str: host of filesystem

    Returns
    -------
    dict: ram information
    '''
    #Calculates ram usage on folio
    if ssh is None:
        ram1 = psutil.virtual_memory()
        ram2 = psutil.swap_memory()
        total = ram1.total
        used = ram1.used    
        free = ram1.available
        shared = ram1.shared
        buffers = ram1.buffers
        cached = ram1.cached
        bc_used = ram1.used - (ram1.buffers + ram1.cached)
        bc_free = ram1.total - bc.used
        swap_total = ram2.total
        swap_used = ram2.used
        swap_free = ram2.free

    else:
        _, folio, _ = ssh.exec_command('free -b')

        ram = []
        for output in folio.splitlines():
            line = output[:].split()
            new_line = filter(lambda a: a not in [''], line)
            ram.append(new_line)    

        for key, row in enumerate(ram[1:-1]):
            if key == 0:
                total = int(row[1])
                used = int(row[2])
                free = int(row[3])
                shared = int(row[4])
                buffers = int(row[5])
                cached = int(row[6])
            elif key == 1:
                bc_used = int(row[2])
                bc_free = int(row[3])
            elif key == 2:
                swap_total = int(row[1])
                swap_used = int(row[2])
                swap_free = int(row[3])

    ram_data = {'host': host,
                'total': total,
                'used': used,
                'free': free,
                'shared': shared,
                'buffers': buffers,
                'cached': cached,
                'bc_used': bc_used,
                'bc_free': bc_free,
                'swap_total': swap_total,
                'swap_used': swap_used,
                'swap_free': swap_free,
                'ram_id': str(uuid.uuid4()),
                'timestamp': int(time.time())}

    return ram_data

def cpu_perc(ssh, host):
    '''
    generates table information for cpu table

    Parameters
    ----------
    ssh | object: ssh object
    host | str: host of filesystem

    Returns
    -------
    dict: cpu usage information
    '''
    #Calculates cpu usage on folio
    timestamp = int(time.time())
    cpu_data = {}
    if ssh is None:
        cpu_all = psutil.cpu_times_percent(interval=1, percpu=True)

        for key, value in enumerate(cpu_all):
            cpu_data[key] = {'host': host,
                            'cpu': key,
                            'user_perc': value.user,
                            'sys_perc': value.sys,
                            'iowait_perc': value.iowait,
                            'idle_perc': value.idle,
                            'intr_s': None,
                            'cpu_id': str(uuid.uuid4()),
                            'timestamp': timestamp}

    else:
        _, folio, _ = ssh.exec_command('mpstat -P ALL 1 1')

        cpu_list = []
        for output in folio.splitlines():
            if output not in folio.splitlines()[:1]:
                line = output[:].split()
                new_line = filter(lambda a: a not in [''], line)
                if new_line[0] not in ['Average:']:
                    cpu_list.append(new_line)

        for key, row in enumerate(cpu_list[3:]):
            cpu_data[key] = {'host': host,
                            'cpu': int(row[2]),
                            'user_perc': two_round(row[3]),
                            'sys_perc': two_round(row[5]),
                            'iowait_perc': two_round(row[6]),
                            'idle_perc': two_round(row[10]),
                            'intr_s': two_round(row[11]),
                            'cpu_id': str(uuid.uuid4()),
                            'timestamp': timestamp}

    return cpu_data

def add_data(host):
    '''
    generates table information for all tables

    Parameters
    ----------
    ssh | object: ssh object
    host | str: host of filesystem
    '''
    with ppdata.ssh_scope(host) as ssh:
        dbi = pyg.DataBaseInterface()
        with dbi.session_scope() as s:
            iostat_all_data = iostat(ssh, host)
            for name, iostat_data in iostat_all_data.items():
                dbi.add_entry_dict(s, 'Iostat', iostat_data)

            ram_data = ram_free(ssh, host)
            dbi.add_entry_dict(s, 'Ram', ram_data)

            cpu_all_data = cpu_perc(ssh, host)
            for key, cpu_data in cpu_all_data.items():
                dbi.add_entry_dict(s, 'Cpu', cpu_data)

            if host in ('folio',):
                paths = ('/data3', '/data4')
                for path in paths:
                    system_data = filesystem(ssh, path)
                    dbi.add_entry_dict(s, 'Filesystem', system_data)

if __name__ == '__main__':
    hosts = ('folio', 'node01', 'node02', 'node03', 'node04', 'node05', 'node06', 'node07', 'node08', 'node09', 'node10')
    named_host = socket.gethostname()
    for host in hosts:
        add_data(host)
