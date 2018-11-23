import logging
import os
import platform
import re
import subprocess


from os_helper import OsHelper
from cmd import run_command

log = logging.getLogger(__name__)


def get_processor_name():
    system_name = OsHelper.name()
    if system_name == "Windows":
        return platform.processor()
    elif system_name == "MacOSX":
        os.environ['PATH'] = os.environ['PATH'] + os.pathsep + '/usr/sbin'
        command = "sysctl -n machdep.cpu.brand_string"
        return subprocess.check_output(command).strip()
    elif platform.system() == "Linux":
        command = "cat /proc/cpuinfo"
        all_info = subprocess.check_output(command, shell=True).strip()
        for line in all_info.split("\n"):
            if "model name" in line:
                return re.sub(".*model name.*:", "", line, 1)
    return ""


class Monitor(object):
    @classmethod
    def get_cpu(cls):
        system_name = OsHelper.name()
        if system_name == "Windows":
            cpu = run_command("wmic cpu get loadpercentage, CurrentClockSpeed, name")[1].split("@")
            cpu_max = cpu[1]
            cpu = cpu[0].split()
            result = {
                "clock_speed": normalize_value(cpu.pop(0) + "Mhz"),
                "used": cpu.pop(0),
                "max": normalize_value(cpu_max.strip()),
                "name": " ".join(cpu)
            }
        elif system_name == "Linux":
            cpus = run_command('top -b -n2 -p 1 | fgrep "Cpu(s)" | tail -1')[0].split()
            index = 0
            idle = 0
            for val in cpus:
                if val == "id,":
                    idle = cpus[index-1]
                    print "Idle: %s" % idle
                index += 1
            used = 100 - float(idle)
            clock = run_command("dmidecode -t processor | grep Current")[0].split()
            clock_string = run_command("dmidecode -t 4 | grep Version")[0].split(" CPU @ ")
            clock_max = normalize_value(clock_string[1])

            result = {
                "clock_speed": normalize_value((clock[2] + clock[3]) + "Mhz"),
                "clock_max": clock_max,
                "used": used,
                "name": clock_string[0]
            }
        elif system_name == "MacOSX":
            log.debug("OSX CPU QUERY")
            cpu_string = run_command("sysctl -n machdep.cpu.brand_string").split(" @ ")
            cpu_freq = run_command("sysctl hw.cpufrequency").split(": ")[1]
            cpu_used = run_command("ps -A -o %cpu | awk '{s+=$1} END {print s}'")
            result = {
                "clock_speed": normalize_value(cpu_freq),
                "used": cpu_used,
                "clock_max": cpu_string[1],
                "name": cpu_string[0]
            }

        else:
            result = {}

        return result

    @classmethod
    def get_memory(cls):
        system_name = OsHelper.name()
        if system_name == "Windows":
            mem_total = int(run_command("wmic ComputerSystem get totalPhysicalMemory")[1])
            mem_free = int(run_command("wmic OS get freePhysicalMemory")[1]) * 1024
            mem_used = mem_total - mem_free

        elif system_name == "Linux":
            mem_data = run_command("free")
            phys_data = mem_data[1].split()

            mem_total = phys_data[1] + "KB"
            mem_free = phys_data[3] + "KB"
            mem_used = phys_data[2] + "KB"

        elif system_name == "MacOSX":
            log.debug("Fetching OSX Memory info")
            mem_data = run_command("""vm_stat | perl -ne '/page size of (\d+)/ and $size=$1; /Pages\s+([^:]+)[^\d]+(
            \d+)/ and printf("%-16s % 16.2f MB\n", "$1:", $2 * $size / 1048576);'""")
            adds = ["active", "inactive", "speculative", "throttled", "wired down", "purgeable"]
            mem_free = 0
            mem_total = 0
            for line in mem_data:
                info = line.split()
                title = info[0].strip(":")
                value = info[1]
                if title == "Free":
                    mem_free = abs(value)
                elif title in adds:
                    mem_total += abs(value)
            mem_used = mem_total - mem_free
        else:
            mem_total = 0
            mem_free = 0
            mem_used = 0

        memdata = {
            "mem_total": normalize_value(mem_total),
            "mem_used": normalize_value(mem_used),
            "mem_free": normalize_value(mem_free)
        }

        return memdata

    @classmethod
    def get_disk(cls):
        disks = []
        os_name = OsHelper.name()
        if os_name == "Windows":
            disk_data = run_command("powershell get-psdrive -psprovider filesystem")
        else:
            disk_data = run_command("df -h | grep /dev")

        header_line = disk_data.pop(0)
        del disk_data[0]
        headers = header_line.split()
        for line in disk_data:
            data = line.split()
            if OsHelper.name() == "Windows":
                sizes = ["KB", "MB", "GB", "TB"]
                used_tag = "Gb"
                free_tag = "Gb"
                for size in sizes:
                    if size in headers[1]:
                        used_tag = size[:1]
                    if size in headers[2]:
                        free_tag = size[:1]

                used_size = float(data[1])
                free_size = float(data[2])
                total_size = used_size + free_size
                percent = total_size / used_size
                used = normalize_value("%s %s" % (used_size, used_tag))
                free = normalize_value("%s %s" % (free_size, free_tag))
                total_size = normalize_value("%s %s" % (total_size, free_tag))
                name = data[0]
                drive = data[4]
            else:
                name = data[5]
                total_size = data[1]
                used = data[2]
                free = data[3]
                percent = data[4]
                drive = data[0]
            disk = {
                "percent": percent,
                "used": used,
                "free": free,
                "total": total_size,
                "drive": drive,
                "name": name
            }
            disks.append(disk)
        return disks

    @classmethod
    def get_net(cls):
        nic_info = {}
        if OsHelper.name() == "Windows":
            log.debug("Get windows stuff here...")
        elif OsHelper.name() == "Linux":
            log.debug("Getting 'nix net info")
            net_data = run_command("cat /proc/net/dev")
            net_data.pop(0)
            for line in net_data:
                info = line.split()
                interface = info[0].strip(":")
                device = {
                    "rx": info[1],
                    "tx": info[9]
                }
                nic_info[interface] = device
        elif OsHelper.name() == "MacOSX":
            net_data = run_command("netstat -ib")
            net_data.pop(0)
            for line in net_data:
                info = line.split()
                interface = info[0]
                tx = info[6]
                rx = info[9]
                nic = nic_info.get(interface) or {
                    "tx": 0,
                    "rx": 0
                }
                tx += nic['tx']
                rx += nic['rx']
                nic["tx"] = tx
                nic["rx"] = rx
                nic_info[interface] = nic
        return nic_info





def normalize_value(value):
    suffix = 'b'
    if value == unicode(value):
        value = value.lower().replace(" ", "")
        num = value
        power = 5
        for remove in ['p', 't', 'g', 'm', 'k', 'b']:
            if remove in value:
                # log.debug("Found a power: %s" % remove)
                split_item = value.split(remove)
                new = split_item[0]
                if len(split_item) > 1:
                    suffix = split_item[1]
                for type_ in [int, float, long]:
                    try:
                        new = type_(new)
                    except ValueError:
                        continue
                adjuster = pow(1024, power)
                # log.debug("Trying to multiply %s by %s" % (adjuster, new))
                num = adjuster * new
                break
            power -= 1
    else:
        num = value
    for type_ in [int, float, long]:
        try:
            num = type_(num)
        except ValueError:
            continue
    if (suffix == "") | (suffix == "b"):
        suffix = "B"
    if suffix == "hz":
        suffix = "Hz"
    for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
