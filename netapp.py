from netapp_ontap import HostConnection
from netapp_ontap.resources import Svm, IpInterface, CifsShare
import os
import smbclient
from datetime import datetime


with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):

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
        svm_data_dict = {}
        for svm_dict in get_svm_collection():
            if svm_dict.get('name') == "svm_data":
                svm_data_dict['svm_name'] = svm_dict['name']
                
                # Fetch LIF IPs
                svm_data_dict['ip_addresses'] = get_lif_ips()
                
                # Fetch CIFS Volumes
                svm_data_dict['volumes'] = get_cifs_volumes()

        return svm_data_dict

    def get_first_ip_address(svm_dict):
        """Retrieve the first IP address from the SVM dictionary."""
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


#    files_in_svm = get_files_by_type('.txt')
#    for share, files in files_in_svm.items():
#        print(f"Share: {share}")
#        for file in files:
#            print(f"  {file}")


    


    print(scan_volume(get_svm_data_volumes()))




