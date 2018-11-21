import os
import platform
import re
import subprocess

from os_helper import OsHelper
from cmd import run_command


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
            cpu = run_command("wmic cpu get loadpercentage, CurrentClockSpeed")[1].split()
            result = {
                "clock_speed": cpu[0],
                "used": cpu[1]
            }
        elif system_name == "Linux":
            cpus = run_command("top -b -n2 | grep Cpu")[0].split()
            clock = run_command("dmidecode -t processor | grep Current")[0].split()
            used = float(cpus[1]) + float(cpus[3]) + float(cpus[7])
            result = {
                "clock_speed": clock[2],
                "used": used
            }
        else:
            result = {}

        result["processor_name"] = get_processor_name()

        return result

    @classmethod
    def get_memory(cls):
        system_name = OsHelper.name()
        if system_name == "Windows":
            mem_total = int(run_command("wmic ComputerSystem get totalPhysicalMemory")[1])
            mem_free = int(run_command("wmic OS get freePhysicalMemory")[1])
            mem_used = mem_total - mem_free

        elif system_name == "Linux":
            mem_data = run_command("free")
            phys_data = mem_data[1].split()

            mem_total = phys_data[1]
            mem_free = phys_data[3]
            mem_used = phys_data[2]

        else:
            mem_total = 0
            mem_free = 0
            mem_used = 0

        memdata = {
            "mem_total": mem_total,
            "mem_used": mem_used,
            "mem_free": mem_free
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
                used_tag = free_tag = "GB"
                for size in sizes:
                    if size in headers[1]:
                        used_tag = size[:1]
                    if size in headers[2]:
                        free_tag = size[:1]

                used_size = float(data[1])
                free_size = float(data[2])
                total_size = used_size + free_size
                percent = total_size / used_size
                used = "%s%s" % (used_size, used_tag)
                free = "%s%s" % (free_size, free_tag)
                total_size = "%s%s" % (total_size, free_tag)
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
