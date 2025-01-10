r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IpInterfaceLocation", "IpInterfaceLocationSchema"]
__pdoc__ = {
    "IpInterfaceLocationSchema.resource": False,
    "IpInterfaceLocationSchema.opts": False,
    "IpInterfaceLocation": False,
}


class IpInterfaceLocationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IpInterfaceLocation object"""

    auto_revert = fields.Boolean(data_key="auto_revert")
    r""" The auto_revert field of the ip_interface_location. """

    broadcast_domain = fields.Nested("netapp_ontap.models.broadcast_domain_svm.BroadcastDomainSvmSchema", unknown=EXCLUDE, data_key="broadcast_domain")
    r""" The broadcast_domain field of the ip_interface_location. """

    failover = fields.Str(data_key="failover")
    r""" The failover field of the ip_interface_location. """

    home_node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="home_node")
    r""" The home_node field of the ip_interface_location. """

    home_port = fields.Nested("netapp_ontap.resources.port.PortSchema", unknown=EXCLUDE, data_key="home_port")
    r""" The home_port field of the ip_interface_location. """

    is_home = fields.Boolean(data_key="is_home")
    r""" The is_home field of the ip_interface_location. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the ip_interface_location. """

    port = fields.Nested("netapp_ontap.resources.port.PortSchema", unknown=EXCLUDE, data_key="port")
    r""" The port field of the ip_interface_location. """

    @property
    def resource(self):
        return IpInterfaceLocation

    gettable_fields = [
        "auto_revert",
        "failover",
        "home_node.links",
        "home_node.name",
        "home_node.uuid",
        "home_port.links",
        "home_port.name",
        "home_port.node",
        "home_port.uuid",
        "is_home",
        "node.links",
        "node.name",
        "node.uuid",
        "port.links",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """auto_revert,failover,home_node.links,home_node.name,home_node.uuid,home_port.links,home_port.name,home_port.node,home_port.uuid,is_home,node.links,node.name,node.uuid,port.links,port.name,port.node,port.uuid,"""

    patchable_fields = [
        "auto_revert",
        "failover",
        "home_node.name",
        "home_node.uuid",
        "home_port.name",
        "home_port.node",
        "home_port.uuid",
        "is_home",
        "node.name",
        "node.uuid",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """auto_revert,failover,home_node.name,home_node.uuid,home_port.name,home_port.node,home_port.uuid,is_home,node.name,node.uuid,port.name,port.node,port.uuid,"""

    postable_fields = [
        "auto_revert",
        "broadcast_domain.links",
        "broadcast_domain.name",
        "broadcast_domain.uuid",
        "failover",
        "home_node.name",
        "home_node.uuid",
        "home_port.name",
        "home_port.node",
        "home_port.uuid",
    ]
    """auto_revert,broadcast_domain.links,broadcast_domain.name,broadcast_domain.uuid,failover,home_node.name,home_node.uuid,home_port.name,home_port.node,home_port.uuid,"""


class IpInterfaceLocation(Resource):

    _schema = IpInterfaceLocationSchema
