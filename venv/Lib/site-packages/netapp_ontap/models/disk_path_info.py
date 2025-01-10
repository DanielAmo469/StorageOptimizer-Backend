r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["DiskPathInfo", "DiskPathInfoSchema"]
__pdoc__ = {
    "DiskPathInfoSchema.resource": False,
    "DiskPathInfoSchema.opts": False,
    "DiskPathInfo": False,
}


class DiskPathInfoSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the DiskPathInfo object"""

    initiator = fields.Str(data_key="initiator")
    r""" Initiator port.

Example: 3a """

    node_name = fields.Str(data_key="node.name")
    r""" Controller with the initiator port for this path.

Example: vsim4 """

    node_uuid = fields.Str(data_key="node.uuid")
    r""" Controller UUID, to identify node for this path.

Example: cf7fe057-526d-11ec-af4e-0050568e9df0 """

    port_name = fields.Str(data_key="port_name")
    r""" Name of the disk port.

Example: A """

    port_type = fields.Str(data_key="port_type")
    r""" Disk port type.

Valid choices:

* sas
* fc
* nvme """

    vmdisk_hypervisor_file_name = fields.Str(data_key="vmdisk_hypervisor_file_name")
    r""" Virtual disk hypervisor file name.

Example: xvds vol0a0567ae156ca59f6 """

    wwnn = fields.Str(data_key="wwnn")
    r""" Target device's World Wide Node Name.

Example: 5000c2971c1b2b8c """

    wwpn = fields.Str(data_key="wwpn")
    r""" Target device's World Wide Port Name.

Example: 5000c2971c1b2b8d """

    @property
    def resource(self):
        return DiskPathInfo

    gettable_fields = [
        "initiator",
        "node_name",
        "node_uuid",
        "port_name",
        "port_type",
        "vmdisk_hypervisor_file_name",
        "wwnn",
        "wwpn",
    ]
    """initiator,node_name,node_uuid,port_name,port_type,vmdisk_hypervisor_file_name,wwnn,wwpn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class DiskPathInfo(Resource):

    _schema = DiskPathInfoSchema
