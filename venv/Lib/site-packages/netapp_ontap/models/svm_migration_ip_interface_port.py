r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmMigrationIpInterfacePort", "SvmMigrationIpInterfacePortSchema"]
__pdoc__ = {
    "SvmMigrationIpInterfacePortSchema.resource": False,
    "SvmMigrationIpInterfacePortSchema.opts": False,
    "SvmMigrationIpInterfacePort": False,
}


class SvmMigrationIpInterfacePortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmMigrationIpInterfacePort object"""

    interface = fields.Nested("netapp_ontap.resources.ip_interface.IpInterfaceSchema", unknown=EXCLUDE, data_key="interface")
    r""" The interface field of the svm_migration_ip_interface_port. """

    port = fields.Nested("netapp_ontap.resources.port.PortSchema", unknown=EXCLUDE, data_key="port")
    r""" The port field of the svm_migration_ip_interface_port. """

    @property
    def resource(self):
        return SvmMigrationIpInterfacePort

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


class SvmMigrationIpInterfacePort(Resource):

    _schema = SvmMigrationIpInterfacePortSchema
