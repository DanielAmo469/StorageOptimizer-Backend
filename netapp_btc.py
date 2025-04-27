import json
from netapp_ontap import HostConnection
from netapp_ontap.resources import Svm, IpInterface, CifsShare, Volume
import os
import smbclient
import os
from datetime import datetime



smbclient.ClientConfig(username="hatul\\Administrator", password="Netapp1!")

def get_svm_collection():
    return [svm.to_dict() for svm in Svm.get_collection(fields="name")]

def get_lif_ips():
    lif_ips = []
    for lif in IpInterface.get_collection(fields="name,ip.address"):
        lif_dict = lif.to_dict()
        if lif_dict.get('name') == "lif_data":
            lif_ips.append(lif_dict.get('ip', {}).get('address'))
    return lif_ips

def get_cifs_volumes():
    volumes = []
    for share in CifsShare.get_collection(fields="name,volume"):
        share_dict = share.to_dict()
        share_name = share_dict.get('name')
        if share_name and 'data' in share_name and '$' not in share_name:
            volumes.append({
                'share_name': share_name,
                'volume': share_dict.get('volume', {}).get('name')
            })
    return volumes

def get_svm_data_volumes():
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):

        svm_data_dict = {}
        for svm_dict in get_svm_collection():
            if svm_dict.get('name') == "svm_data":
                svm_data_dict['svm_name'] = svm_dict['name']
                
                # Fetch LIF IPs
                svm_data_dict['ip_addresses'] = get_lif_ips()
                
                # Fetch CIFS Volumes
                svm_data_dict['volumes'] = get_cifs_volumes()

        return svm_data_dict
    
def get_cifs_archive_volumes():
    volumes = []
    for share in CifsShare.get_collection(fields="name,volume"):
        share_dict = share.to_dict()
        share_name = share_dict.get('name')
        if share_name and 'archive' in share_name and '$' not in share_name:
            volumes.append({
                'share_name': share_name,
                'volume': share_dict.get('volume', {}).get('name')
            })
    return volumes

def get_svm_archive_volumes():
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        svm_archive_dict = {}
        
        for svm_dict in get_svm_collection():
            if svm_dict.get('name') == "svm_data":
                svm_archive_dict['svm_name'] = svm_dict['name']
                
                # Fetch LIF IPs
                svm_archive_dict['ip_addresses'] = get_lif_ips()
                
                # Fetch CIFS Archive Volumes
                svm_archive_dict['volumes'] = get_cifs_archive_volumes()

        return svm_archive_dict


def get_archive_path(file_path):
    archive_volumes = get_svm_archive_volumes()  
    archive_ip = "192.168.16.15"

    if not archive_volumes or "volumes" not in archive_volumes:
        print("Error: No valid archive volumes found.")
        return None

    archive_map = {}
    for archive in archive_volumes['volumes']:
        share_name = archive['share_name']
        if "archive1" in share_name.lower():
            archive_map["data1"] = f"\\\\{archive_ip}\\{share_name}"
        elif "archive2" in share_name.lower():
            archive_map["data2"] = f"\\\\{archive_ip}\\{share_name}"

    file_path_lower = file_path.lower()

    if "\\\\192.168.16.14\\data1\\" in file_path_lower:
        return archive_map.get("data1")
    elif "\\\\192.168.16.14\\data2\\" in file_path_lower:
        return archive_map.get("data2")

    print(f"Invalid source path for archive mapping: {file_path}")
    return None


def get_first_ip_address(volume: dict) -> str | None:
    try:
        nas_server = volume.get('nas_server')
        if not nas_server:
            print("No 'nas_server' found in volume")
            return None

        interfaces = nas_server.get('interfaces')
        if not interfaces or not isinstance(interfaces, list) or len(interfaces) == 0:
            print("No 'interfaces' found in 'nas_server'")
            return None

        first_interface = interfaces[0]
        ip = first_interface.get('ip')
        if not ip:
            print("No 'ip' found in first interface")
            return None

        return ip
    except Exception as e:
        print(f"Error in get_first_ip_address: {e}")
        return None


def access_CIFS_share(share, ip_address):

    share_name = share.get('share_name')
    if not share_name or not ip_address:
        return None, None
    
    share_path = f"\\\\{ip_address}\\{share_name}"
    return share_path, share_name



#Scan a single share (volume) and return list of file metadata
def scan_volume(share_name: str, volume: dict, blacklist: list[str]) -> list[dict]:
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        if "nas_server" not in volume or not volume["nas_server"].get("interfaces"):
            volume["nas_server"] = {"interfaces": [{"ip": "192.168.16.14"}]}

        ip_address = get_first_ip_address(volume)
        if not ip_address:
            print(f"Failed to get IP address for volume: {volume}")
            return []

        share_path, confirmed_share_name = access_CIFS_share({"share_name": share_name}, ip_address)
        if confirmed_share_name != share_name or not share_path:
            print(f"Share '{share_name}' not found correctly in volume config.")
            return []

        scanned_files = []

        try:
            for dirpath, dirnames, filenames in smbclient.walk(share_path):
                if any(b.lower() in dirpath.lower() for b in blacklist):
                    continue
                for file in filenames:
                    if file.endswith(".bat"):
                        continue
                    full_path = os.path.join(dirpath, file)

                    try:
                        creation_time = datetime.fromtimestamp(os.path.getctime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                        last_access_time = datetime.fromtimestamp(os.path.getatime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                        last_modified_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                        file_size = os.path.getsize(full_path)
                    except Exception as e:
                        print(f"Skipping unreadable file metadata: {full_path} → {e}")
                        continue

                    scanned_files.append({
                        'full_path': full_path,
                        'creation_time': creation_time,
                        'last_access_time': last_access_time,
                        'last_modified_time': last_modified_time,
                        'file_size': file_size
                    })
        except Exception as e:
            print(f"Error walking share {share_path}: {e}")

        return scanned_files




filter_parameters = {"blacklist", "creation_time_start", "creation_time_end", "last_access_time_start", "last_access_time_end", "last_modified_time_start", "last_modified_time_end", "file_size_min", "file_size_max"}



def convert_to_datetime(date_input):
    if isinstance(date_input, int):
        return datetime.fromtimestamp(date_input)
    elif isinstance(date_input, str):
        try:
            return datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                return datetime.strptime(date_input, '%Y-%m-%d')
            except ValueError:
                print(f"Invalid datetime format: {date_input}")
                return None
    return None


def is_blacklisted(file_path, blacklist):
    file_path_lower = file_path.lower()
    return any(blacklisted.lower() in file_path_lower for blacklisted in blacklist)


def filter_by_type(file_info, file_type):
    if not file_type:
        return True
    if isinstance(file_type, list):
        return file_info['full_path'].lower().endswith(tuple(ft.lower() for ft in file_type))
    return file_info['full_path'].lower().endswith(file_type.lower())

def filter_by_dates(file_info, date_filters):
    for date_type, date_range in date_filters.items():
        if date_range:  # Only process if a filter is set
            file_date = convert_to_datetime(file_info.get(date_type))
            start_date = convert_to_datetime(date_range.get('start_date'))
            end_date = convert_to_datetime(date_range.get('end_date'))

            if file_date is None:
                return False  # If the file is missing this date, exclude it
            
            if start_date and file_date < start_date:
                return False
            if end_date and file_date > end_date:
                return False

    return True  # Passes all date filters


def filter_by_size(file_info, min_size, max_size):
    file_size = file_info.get('file_size')

    if min_size is not None and file_size < min_size:
        return False
    if max_size is not None and max_size > 0 and file_size > max_size:
        return False
    return True



def filter_files(files, filters, blacklist, share_name):
    if share_name not in files:
        print(f"Share '{share_name}' not found in scanned results.")
        return {}

    filtered_files = {share_name: []}
    min_size = filters.get('min_size')
    max_size = filters.get('max_size')
    file_type = filters.get('file_type')
    date_filters = filters.get('date_filters', {})

    for file_info in files[share_name]:
        if is_blacklisted(file_info['full_path'], blacklist):
            continue
        if file_info['full_path'].endswith("_shortcut.bat"):
            continue
        if not filter_by_type(file_info, file_type):
            continue
        if not filter_by_dates(file_info, date_filters):
            continue
        if not filter_by_size(file_info, min_size, max_size):
            continue

        filtered_files[share_name].append(file_info)

    return {share_name: filtered_files[share_name]} if filtered_files[share_name] else {}




def normalize_path(file_path):
    file_path = file_path.replace("/", "\\")  
    if not file_path.startswith("\\\\"):
        file_path = "\\\\" + file_path.lstrip("\\")
    return file_path


def get_volume_name_by_share(share_name: str) -> str | None:
    svm_data = get_svm_data_volumes()
    for entry in svm_data.get("volumes", []):
        if entry["share_name"].lower() == share_name.lower():
            return entry["volume"]
    return None


def get_vol_uuid(vol_name):
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        volumes = Volume.get_collection(name=vol_name, fields="uuid,name")
        for vol in volumes:
            return vol.uuid
        return None 
    
def get_svm_uuid(svm_name):
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        svms = Svm.get_collection(name= svm_name, fields ="uuid,name")
        for svm in svms:
            return svm.uuid
        return None
    