import json
from netapp_ontap import HostConnection
from netapp_ontap.resources import Svm, IpInterface, CifsShare
import os
import smbclient
import tempfile
import shutil
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
    archive_ip = "192.168.16.15"  # ✅ Manually set the correct archive IP

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

    print(f"DEBUG: Updated Archive Map: {archive_map}")

    if "\\\\192.168.16.14\\data1\\" in file_path:
        archive_path = archive_map.get("data1", f"\\\\{archive_ip}\\archive1")
        print(f"DEBUG: Resolved archive path for data1: {archive_path}")
        return archive_path

    elif "\\\\192.168.16.14\\data2\\" in file_path:
        archive_path = archive_map.get("data2", f"\\\\{archive_ip}\\archive2")
        print(f"DEBUG: Resolved archive path for data2: {archive_path}")
        return archive_path

    print(f"Error: No matching archive path found for {file_path}")
    return None



def get_first_ip_address(svm_dict):
    ip_address = svm_dict.get('ip_addresses', [])[0]  # Use the first IP address
    if not ip_address:
        print("No IP address found in the SVM data.")
    return ip_address

def access_CIFS_share(share, ip_address):

    share_name = share.get('share_name')
    if not share_name or not ip_address:
        return None, None
    
    share_path = f"\\\\{ip_address}\\{share_name}"
    return share_path, share_name

def get_files_by_type(file_type):
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):


        # Retrieve the SVM dictionary
        svm_dict = get_svm_data_volumes()
        print(svm_dict)
        
        files = {}
        ip_address = get_first_ip_address(svm_dict)
        if not ip_address:
            return files

        # Access each CIFS share
        for share in svm_dict.get('volumes', []):
            share_path, share_name = access_CIFS_share(share, ip_address)
            if not share_name or not share_path:
                continue

            files[share_name] = []

            try:
                # Recursively walk the share
                for dirpath, _, filenames in smbclient.walk(share_path):
                    for file in filenames:
                        if file.endswith(file_type):
                            full_path = os.path.join(dirpath, file)
                            files[share_name].append(full_path)
            except OSError as e:
                print(f"Error accessing share {share_path}: {e}")

        return files


def scan_volume(volume):
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        files = {}
        ip_address = get_first_ip_address(volume)
        if not ip_address:
            return files

        # Access each CIFS share
        for share in volume.get('volumes', []):
            share_path, share_name = access_CIFS_share(share, ip_address)
            if not share_name or not share_path:
                continue

            files[share_name] = []

            try:
                # Recursively walk the share
                for dirpath, _, filenames in smbclient.walk(share_path):
                    for file in filenames:
                        if file:
                            full_path = os.path.join(dirpath, file)
                            creation_time = datetime.fromtimestamp(os.path.getctime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                            last_access_time = datetime.fromtimestamp(os.path.getatime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                            last_modified_time = datetime.fromtimestamp(os.path.getmtime(full_path)).strftime('%Y-%m-%d %H:%M:%S')
                            file_size = os.path.getsize(full_path)
                            files[share_name].append({
                                'full_path': full_path,
                                'creation_time': creation_time,
                                'last_access_time': last_access_time,
                                'last_modified_time': last_modified_time,
                                'file_size': file_size
                            })
            except OSError as e:
                print(f"Error accessing share {share_path}: {e}")

        return files


filter_parameters = {"blacklist", "creation_time_start", "creation_time_end", "last_access_time_start", "last_access_time_end", "last_modified_time_start", "last_modified_time_end", "file_size_min", "file_size_max"}




def convert_to_datetime(date_input):
    if isinstance(date_input, int):
        return datetime.fromtimestamp(date_input)
    elif isinstance(date_input, str):
        try:
            return datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return None



def is_blacklisted(file_path, blacklist):
    return any(blacklisted in file_path for blacklisted in blacklist)

def filter_by_type(file_info, file_type):
    return file_info['full_path'].endswith(file_type) if file_type else True

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
    file_size = file_info['file_size']
    if min_size is not None and file_size < min_size:
        return False
    if max_size is not None and file_size > max_size:
        return False
    return True

def filter_files(files, filters, blacklist, share_name):
    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        filtered_files = {}

        for share_name, file_list in files.items():
            filtered_files[share_name] = []

            for file_info in file_list:
                if is_blacklisted(file_info['full_path'], blacklist):
                    continue
                if not filter_by_type(file_info, filters.get('file_type')):
                    continue
                if not filter_by_dates(file_info, filters.get('date_filters', {})):  # Checks multiple date filters
                    continue
                if not filter_by_size(file_info, filters.get('min_size'), filters.get('max_size')):
                    continue

                filtered_files[share_name].append(file_info)

        return {k: v for k, v in filtered_files.items() if v}


def normalize_path(file_path):
    file_path = file_path.replace("/", "\\")  
    if not file_path.startswith("\\\\"):
        file_path = "\\\\" + file_path.lstrip("\\")
    return file_path


def save_metadata(dest_folder, filename, original_path):
    metadata_file = os.path.join(dest_folder, "metadata.json")

    # ✅ Ensure archive folder exists
    try:
        smbclient.stat(dest_folder)  # Check if folder exists
    except FileNotFoundError:
        print(f"WARNING: Archive folder {dest_folder} does not exist. Creating it.")
        try:
            smbclient.makedirs(dest_folder)
        except Exception as e:
            print(f"ERROR: Failed to create archive folder {dest_folder}: {e}")
            return False

    # ✅ Ensure metadata.json exists before opening
    try:
        smbclient.stat(metadata_file)  # Check if metadata.json exists
    except FileNotFoundError:
        print(f"WARNING: metadata.json not found, creating a new one at {metadata_file}")
        try:
            with smbclient.open_file(metadata_file, mode="w") as f:
                f.write("{}")  # ✅ Create an empty JSON file
        except Exception as e:
            print(f"ERROR: Failed to create metadata.json: {e}")
            return False

    # ✅ Load existing metadata
    try:
        with smbclient.open_file(metadata_file, mode="r") as f:
            metadata = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        print(f"WARNING: metadata.json is corrupted or missing. Resetting it.")
        metadata = {}

    # ✅ Save new metadata entry
    metadata[filename] = original_path

    try:
        with smbclient.open_file(metadata_file, mode="w") as f:
            json.dump(metadata, f, indent=4)
        print(f"Updated metadata.json: {metadata_file}")
    except Exception as e:
        print(f"ERROR: Failed to write metadata.json: {e}")
        return False

    return True


def move_file(file_info):
    src_path = normalize_path(file_info['full_path'])
    dest_folder = normalize_path(get_archive_path(src_path))

    print(f"DEBUG: Attempting to move file")
    print(f"  Source: {src_path}")
    print(f"  Destination Folder: {dest_folder}")

    if not dest_folder:
        print(f"Skipping file {src_path} (Invalid archive destination)")
        return False

    try:
        smbclient.stat(src_path)  
        print("File is accessible, proceeding with move...")

        filename = os.path.basename(src_path)
        dest_path = f"{dest_folder}\\{filename}"
        dest_path = normalize_path(dest_path)

        print(f"Final Destination Path: {dest_path}")

        # ✅ Ensure metadata is set before moving
        if not save_metadata(dest_folder, filename, src_path):
            print(f"ERROR: Failed to save metadata for {filename}. Aborting move.")
            return False

        # ✅ Download file to local temp
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(src_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)

        print(f"Successfully downloaded file to local temp: {temp_file_path}")

        # ✅ Upload file to archive
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(dest_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)

        print(f"Uploaded file to archive: {dest_path}")

        # ✅ Verify the file before deleting the original
        try:
            smbclient.stat(dest_path)
            smbclient.remove(src_path)
            print(f"Deleted original file: {src_path}")
        except FileNotFoundError:
            print(f"Failed to verify copied file at {dest_path}. Not deleting original.")
            return False

        # ✅ Restore timestamps
        os.utime(dest_path, (datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S').timestamp(),
                             datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S').timestamp()))

        # ✅ Create shortcut in the original location
        create_shortcut(src_path, dest_path)

        # ✅ Remove temp file
        os.remove(temp_file_path)

        return dest_path

    except FileNotFoundError:
        print(f"File not found: {src_path}")
        return False
    except PermissionError:
        print(f"Permission denied: {src_path}")
        return False
    except Exception as e:
        print(f"Failed to move {src_path}: {e}")
        return False


def create_shortcut(original_path, archive_path):
    shortcut_path = original_path + "_shortcut.bat"  # Create a .bat file instead of .lnk
    
    try:
        with open(shortcut_path, 'w') as shortcut:
            shortcut.write(f'@echo off\nstart "" "{archive_path}"\n')  # Opens the file when double-clicked
        
        print(f"Shortcut created: {shortcut_path} → {archive_path}")
        return True
    except Exception as e:
        print(f"Failed to create shortcut for {original_path}: {e}")
        return False


def process_files_for_archival(files):
    for file_list in files.values():
        for file_info in file_list:
            archive_path = move_file(file_info)
            if archive_path:
                create_shortcut(file_info['full_path'], archive_path)



def load_metadata(dest_folder):
    metadata_file = os.path.join(dest_folder, "metadata.json")

    try:
        with smbclient.open_file(metadata_file, mode="r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"No metadata file found at {metadata_file}")
        return {}

def restore_file(archive_folder, filename):
    metadata = load_metadata(archive_folder)

    if filename not in metadata:
        print(f"Original location not found for {filename}")
        return False

    original_path = metadata[filename]
    archive_path = os.path.join(archive_folder, filename)

    print(f"Restoring {filename}")
    print(f"  Source (Archive): {archive_path}")
    print(f"  Destination (Original): {original_path}")

    try:
        # Download from archive to local temp
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(archive_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)

        print(f"Downloaded file from archive to local temp: {temp_file_path}")

        # Upload file back to original location
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(original_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)

        print(f"Restored file to: {original_path}")

        # Verify and delete from archive
        try:
            smbclient.stat(original_path)
            smbclient.remove(archive_path)
            print(f"Deleted file from archive: {archive_path}")
        except FileNotFoundError:
            print(f"Failed to verify file at {original_path}. Not deleting archive copy.")
            return False

        # Restore timestamps
        os.utime(original_path, (datetime.strptime(metadata["last_access_time"], '%Y-%m-%d %H:%M:%S').timestamp(),
                                 datetime.strptime(metadata["last_modified_time"], '%Y-%m-%d %H:%M:%S').timestamp()))

        # Remove shortcut from original path
        shortcut_path = original_path + ".lnk"
        if os.path.exists(shortcut_path):
            os.remove(shortcut_path)
            print(f"Removed shortcut: {shortcut_path}")

        return original_path

    except FileNotFoundError:
        print(f"File not found in archive: {archive_path}")
        return False
    except PermissionError:
        print(f"Permission denied: {archive_path}")
        return False
    except Exception as e:
        print(f"Failed to restore {filename}: {e}")
        return False


#    print(scan_volume(get_svm_data_volumes()))
#    print(get_svm_data_volumes())



# Example usage:
filters = {
    'file_type': '.txt',
    'date_filters': {  # Allows independent date filters for different types
        'creation_time': {'start_date': '2023-01-01 00:00:00', 'end_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
        'last_access_time': {'start_date': None, 'end_date': None},  # If None, this filter is ignored
        'last_modified_time': {'start_date': None, 'end_date': None},
    },
    'min_size': 100,  # Minimum file size in bytes
    'max_size': 50000  # Maximum file size in bytes
}
blacklist = ['6Uh24TE', '3liOYfQA']

# Scan and filter files
#filtered_files = filter_files(scan_volume(get_svm_data_volumes()), filters, blacklist)
#print(filtered_files)

file1 = {
    "data2": [
    {
        'full_path': "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt",
        'creation_time': '2025-01-12 07:15:49',
        'last_access_time': '2024-08-25 15:15:49',
        'last_modified_time': '2024-08-25 15:15:49',
        'file_size': 32166
    }
    ]
}

file_path = "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt"

try:
    smbclient.stat(file_path)
    print("File exists and is accessible.")
except FileNotFoundError:
    print("File does not exist or cannot be accessed.")

process_files_for_archival(file1)
