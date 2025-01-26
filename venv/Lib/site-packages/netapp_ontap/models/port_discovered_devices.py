r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["PortDiscoveredDevices", "PortDiscoveredDevicesSchema"]
__pdoc__ = {
    "PortDiscoveredDevicesSchema.resource": False,
    "PortDiscoveredDevicesSchema.opts": False,
    "PortDiscoveredDevices": False,
}


class PortDiscoveredDevicesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PortDiscoveredDevices object"""

    capabilities = fields.List(fields.Str, data_key="capabilities")
    r""" The list of the capabilities of the discovered device. """

    chassis_id = fields.Str(data_key="chassis_id")
    r""" Identifier associated with this specific discovered device, useful for locating the device in a data center. """

    ip_addresses = fields.List(fields.Str, data_key="ip_addresses")
    r""" The IP addresses on the discovered device.

Example: ["192.168.100.24","192.168.100.26"] """

    name = fields.Str(data_key="name")
    r""" Name of the discovered device.

Example: ETY-R1S4-510Q13.datacenter.example.com """

    platform = fields.Str(data_key="platform")
    r""" Hardware platform of the discovered device.

Example: 93180YC-EX """

    protocol = fields.Str(data_key="protocol")
    r""" The protocol used to identify the discovered device. This can have a value of CDP or LLDP.

Valid choices:

* cdp
* lldp """

    remaining_hold_time = Size(data_key="remaining_hold_time")
    r""" The number of seconds until the discovered device entry expires and is removed. """

    remote_port = fields.Str(data_key="remote_port")
    r""" The name of the remote port on the discovered device. The format is dependent on the reporting device.

Example: FastEthernet0/12 """

    system_name = fields.Str(data_key="system_name")
    r""" Additional name used to identify a specific piece of equipment. """

    version = fields.Str(data_key="version")
    r""" The version of the software running on the discovered device.

Example: Cisco Nexus Operating System (NX-OS) Software, Version 8.1 """

    @property
    def resource(self):
        return PortDiscoveredDevices

    gettable_fields = [
        "capabilities",
        "chassis_id",
        "ip_addresses",
        "name",
        "platform",
        "protocol",
        "remaining_hold_time",
        "remote_port",
        "system_name",
        "version",
    ]
    """capabilities,chassis_id,ip_addresses,name,platform,protocol,remaining_hold_time,remote_port,system_name,version,"""

    patchable_fields = [
        "capabilities",
        "chassis_id",
        "ip_addresses",
        "name",
        "platform",
        "protocol",
        "remaining_hold_time",
        "remote_port",
        "system_name",
        "version",
    ]
    """capabilities,chassis_id,ip_addresses,name,platform,protocol,remaining_hold_time,remote_port,system_name,version,"""

    postable_fields = [
        "capabilities",
        "chassis_id",
        "ip_addresses",
        "name",
        "platform",
        "protocol",
        "remaining_hold_time",
        "remote_port",
        "system_name",
        "version",
    ]
    """capabilities,chassis_id,ip_addresses,name,platform,protocol,remaining_hold_time,remote_port,system_name,version,"""


class PortDiscoveredDevices(Resource):

    _schema = PortDiscoveredDevicesSchema
