from __future__ import division
import logging
import time

from os_helper import OsHelper
from cmd import run_command


log = logging.getLogger(__name__)


def to_base(value):
    if value == unicode(value):
        value = value.lower().replace(" ", "")
        power = 5
        for remove in ['p', 't', 'g', 'm', 'k', 'b']:
            if remove in value:
                split_item = value.split(remove)
                new = split_item[0]
                for type_ in [int, float, long]:
                    try:
                        new = type_(new)
                    except ValueError:
                        continue
                adjuster = pow(1024, power)
                value = adjuster * new
                break
            power -= 1

    for type_ in [int, float, long]:
        try:
            value = type_(value)
        except ValueError:
            continue
    return value


def from_base(value, suffix):
    out_unit = ''
    if (suffix == "") | (suffix == "b"):
        suffix = "B"
    if suffix == "hz":
        suffix = "Hz"
    if suffix != "%":
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(value) < 1024.0:
                out_unit = unit
                break
            value /= 1024.0
    value = round(value, 2)
    return "%s%s%s" % (value, out_unit, suffix)


class Monitor(object):

    is_friendly = False

    def __init__(self, friendly=False):
        cls = self.__class__
        cls.is_friendly = friendly

    @classmethod
    def get_cpu(cls):
        cpu_speed = 0
        cpu_max = 0
        cpu_name = "unknown"
        cpu_used = 0
        system_name = OsHelper.name()
        if system_name == "Windows":
            cpu_data = run_command("wmic cpu get loadPercentage, CurrentClockSpeed, maxClockSpeed, name")[1]
            log.debug("CPU DATA: %s" % cpu_data)
            cpu = cpu_data.split()
            cpu_speed = cpu.pop(0) + "Mhz"
            cpu_used = cpu.pop(0)
            cpu_max = cpu.pop(0)
            cpu_name = " ".join(cpu)
            if "@" in cpu_name:
                cpu_name = cpu_name.split("@")[0].strip()
        elif system_name == "Linux":
            cpus = run_command('top -b -n2 -p 1 | fgrep "Cpu(s)" | tail -1')[0].split()
            index = 0
            idle = 0
            for val in cpus:
                if val == "id,":
                    idle = cpus[index-1]
                    print "Idle: %s" % idle
                index += 1
            clock = run_command("dmidecode -t processor | grep Current")[0].split()
            clock_string = run_command("dmidecode -t 4 | grep Version")[0].split(" CPU @ ")

            cpu_speed = (clock[2] + clock[3]) + "Mhz"
            cpu_max = clock_string[1]
            cpu_used = 100 - float(idle)
            cpu_name = clock_string[0]
        elif system_name == "MacOSX":
            log.debug("OSX CPU QUERY")
            clock_string = run_command("sysctl -n machdep.cpu.brand_string")[0].split(" @ ")

            cpu_speed = run_command("sysctl hw.cpufrequency")[0].split(": ")[1]
            cpu_max = clock_string[1]
            cpu_used = run_command("ps -A -o %cpu | awk '{s+=$1} END {print s}'")[0]
            cpu_name = clock_string[0]

        result = {
            "cpu_clock_current": cls.normalize_value(cpu_speed, "hz"),
            "cpu_clock_max": cls.normalize_value(cpu_max, 'hz'),
            "cpu_pct_used": cls.normalize_value(cpu_used, '%'),
            "cpu_name": cpu_name
        }

        return result

    @classmethod
    def get_memory(cls):
        system_name = OsHelper.name()
        if system_name == "Windows":
            mem_total = long(run_command("wmic ComputerSystem get totalPhysicalMemory")[1])
            mem_free = long(run_command("wmic OS get freePhysicalMemory")[1]) * 1024
            mem_used = long(mem_total - mem_free) + .0
            mem_pct_used = (mem_used/mem_total) * 100
            mem_total = "%s B" % mem_total
            mem_free = "%s B" % mem_free
            mem_used = "%s B" % mem_used

        elif system_name == "Linux":
            mem_data = run_command("free")
            phys_data = mem_data[1].split()

            mem_total = phys_data[1] + " KB"
            mem_free = phys_data[3] + " KB"
            mem_used = phys_data[2] + " KB"
            mem_pct_used = (phys_data[1] / phys_data[2]) * 100
        elif system_name == "MacOSX":
            log.debug("Fetching OSX Memory info")
            mem_data = run_command("vm_stat")
            page_size = mem_data.pop(0).split("page size of ")[1].split()[0]
            adds = ["Pages active", "Pages inactive", "Pages wired down"]
            mem_free = 0
            mem_total = 0
            for line in mem_data:
                info = line.split(":")
                title = info[0]
                value = int(info[1].strip().strip(".")) * int(page_size)
                if title == "Pages free":
                    mem_free = int(value)
                elif title in adds:
                    mem_total += int(value)
            mem_used = mem_total - mem_free
            mem_pct_used = (mem_total / mem_used) * 100
            mem_total = "%s B" % mem_total
            mem_used = "%s B" % mem_used
            mem_free = "%s B" % mem_free
        else:
            mem_total = "0 B"
            mem_free = "0 B"
            mem_used = "0 B"
            mem_pct_used = 0

        memdata = {
            "mem_total": cls.normalize_value(mem_total),
            "mem_used": cls.normalize_value(mem_used),
            "mem_free": cls.normalize_value(mem_free),
            "mem_pct_used": cls.normalize_value(mem_pct_used, "%")
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
                used_tag = "GB"
                free_tag = "GB"
                for size in sizes:
                    if size in headers[1]:
                        used_tag = size[:1]
                    if size in headers[2]:
                        free_tag = size[:1]

                used_size = float(data[1])
                free_size = float(data[2])
                total_size = used_size + free_size
                percent = (used_size / total_size) * 100
                used = "%s %s" % (used_size, used_tag)
                free = "%s %s" % (free_size, free_tag)
                total_size = "%s %s" % (total_size, free_tag)
                name = data[0]
                drive = data[4]
            else:
                if OsHelper.name() == "MacOSX":
                    name = data[8]
                else:
                    name = data[5]

                total_size = to_base(data[1])
                used = to_base(data[2])
                free = to_base(data[3])
                percent = used / free
                drive = data[0]

            disk = {
                "hdd_pct_used": cls.normalize_value(percent, "%"),
                "hdd_used": cls.normalize_value(used),
                "hdd_free": cls.normalize_value(free),
                "hdd_total": cls.normalize_value(total_size),
                "hdd_path": drive,
                "hdd_name": name
            }
            disks.append(disk)
        disks = sorted(disks, key=lambda z: z['hdd_total'], reverse=True)
        return disks

    @classmethod
    def get_net(cls):
        nic_info = {}
        if OsHelper.name() == "Windows":
            log.debug("Get windows stuff here...")
            net_data = run_command("wmic path Win32_PerfRawData_Tcpip_NetworkInterface get Name,BytesReceivedPersec,"
                                   "BytesSentPersec,CurrentBandwidth")
            time.sleep(1)
            net_data2 = run_command("wmic path Win32_PerfRawData_Tcpip_NetworkInterface get Name,BytesReceivedPersec,"
                                   "BytesSentPersec")

            net_data.pop(0)
            net_data2.pop(0)
            for line in net_data:
                info = line.split()
                info2 = net_data2.pop(0).split()
                tx1 = int(info2.pop(0))
                tx2 = int(info.pop(0))
                rx1 = int(info2.pop(0))
                rx2 = int(info.pop(0))
                tx = tx1 - tx2
                rx = rx1 - rx2
                nic_max = (int(info.pop(0)) * 1024) + .0
                log.debug("Tx1 txt2 and rx1 and rx2 are %s and %s and %s and %s" % (tx1, tx2, rx1, rx2))
                device = {
                    "net_rx": rx,
                    "net_tx": tx,
                    "net_max": to_base(str(nic_max) + " KB")
                }
                interface = " ".join(info)
                nic_info[interface] = device
        elif OsHelper.name() == "Linux":
            log.debug("Getting 'nix net info")
            net_data = run_command("cat /proc/net/dev")
            net_data.pop(0)
            for line in net_data:
                info = line.split()
                interface = info[0].strip(":")
                net_max = run_command("ethtool eth0 | grep Speed:")[0].split(": ")[1].strip("/s")
                device = {
                    "net_rx": int(info[1]),
                    "net_tx": int(info[9]),
                    "net_max": to_base(net_max)
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
                log.debug("Values: %s and %s and %s" % (interface, tx, rx))
                tx = int(tx)
                rx = int(rx)
                nic = nic_info.get(interface) or {
                    "net_tx": 0,
                    "net_rx": 0
                }
                tx += nic['net_tx']
                rx += nic['net_rx']
                nic["net_tx"] = tx
                nic["net_rx"] = rx
                nic_info[interface] = nic

        nic_list = []
        for interface, nic in nic_info.items():
            nic_rx_pct = 0
            nic_tx_pct = 0
            try:
                nic_rx_pct = (nic["net_rx"] / nic["net_max"]) * 100
                nic_tx_pct = (nic["net_tx"] / nic["net_max"]) * 100
            except ZeroDivisionError:
                log.debug("ZERO DIVISION ERRR")
                continue
            nic['net_rx_pct'] = cls.normalize_value(nic_rx_pct, "%")
            nic['net_tx_pct'] = cls.normalize_value(nic_tx_pct, "%")
            nic["net_tx"] = cls.normalize_value(nic['net_tx'])
            nic["net_rx"] = cls.normalize_value(nic['net_rx'])
            nic["nic_name"] = interface
            nic_list.append(nic)
        nic_list = sorted(nic_list, key=lambda z: z['net_rx_pct'], reverse=True)
        return nic_list

    @classmethod
    def normalize_value(cls, value, suffix='B', friendly='nodata'):

        num = to_base(value)

        if friendly == 'nodata':
            friendly = cls.is_friendly

        if friendly:
            num = from_base(num, suffix)
        elif suffix == "%":
            num = round(float(value), 2)

        return num
