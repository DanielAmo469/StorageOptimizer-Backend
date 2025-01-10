r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmMigrationIpInterfacePlacementIpInterfaces", "SvmMigrationIpInterfacePlacementIpInterfacesSchema"]
__pdoc__ = {
    "SvmMigrationIpInterfacePlacementIpInterfacesSchema.resource": False,
    "SvmMigrationIpInterfacePlacementIpInterfacesSchema.opts": False,
    "SvmMigrationIpInterfacePlacementIpInterfaces": False,
}


class SvmMigrationIpInterfacePlacementIpInterfacesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmMigrationIpInterfacePlacementIpInterfaces object"""

    interface = fields.Nested("netapp_ontap.resources.ip_interface.IpInterfaceSchema", unknown=EXCLUDE, data_key="interface")
    r""" The interface field of the svm_migration_ip_interface_placement_ip_interfaces. """

    port = fields.Nested("netapp_ontap.resources.port.PortSchema", unknown=EXCLUDE, data_key="port")
    r""" The port field of the svm_migration_ip_interface_placement_ip_interfaces. """

    @property
    def resource(self):
        return SvmMigrationIpInterfacePlacementIpInterfaces

    gettable_fields = [
        "interface.links",
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "port.links",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """interface.links,interface.ip,interface.name,interface.uuid,port.links,port.name,port.node,port.uuid,"""

    patchable_fields = [
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """interface.ip,interface.name,interface.uuid,port.name,port.node,port.uuid,"""

    postable_fields = [
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """interface.ip,interface.name,interface.uuid,port.name,port.node,port.uuid,"""


class SvmMigrationIpInterfacePlacementIpInterfaces(Resource):

    _schema = SvmMigrationIpInterfacePlacementIpInterfacesSchema
