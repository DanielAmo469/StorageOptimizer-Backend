from netapp_ontap import config, HostConnection
from netapp_ontap.resources import Volume, Snapshot, Svm, IpInterface, CifsShare

with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):

    def get_data_svms():

        for svm in Svm.get_collection():
            svm_dict = svm.to_dict()
            if 'name' in svm_dict and svm_dict['name'] == "svm_data":
                print(f"{svm_dict['name']}")
            data_ip= list(IpInterface.get_collection(name="lif_data", fields="ip.address"))
            for interface in data_ip:
                ip_address = interface.to_dict().get('ip', {}).get('address')
                if ip_address:
                    print(ip_address)
            volumes = list(
                CifsShare.get_collection(
                    fields="name,volume"
                )
            )
            for volume in volumes:
                share = volume.to_dict().get('name')
                if '$' not in share:
                    print(share)


    def get_archive_svms():
        for svm in Svm.get_collection():
            svm_dict = svm.to_dict()
            if 'name' in svm_dict and svm_dict['name'] == "svm_archive":
                print(f"{svm_dict['name']}")
            data_ip= list(IpInterface.get_collection(name="lif_archive", fields="ip.address"))
            for interface in data_ip:
                ip_address = interface.to_dict().get('ip', {}).get('address')
                if ip_address:
                    print(ip_address)

    get_data_svms()
    get_archive_svms()
