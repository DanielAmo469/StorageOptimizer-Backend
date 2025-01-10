r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IpInterfaceSvmLocation", "IpInterfaceSvmLocationSchema"]
__pdoc__ = {
    "IpInterfaceSvmLocationSchema.resource": False,
    "IpInterfaceSvmLocationSchema.opts": False,
    "IpInterfaceSvmLocation": False,
}


class IpInterfaceSvmLocationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IpInterfaceSvmLocation object"""

    broadcast_domain = fields.Nested("netapp_ontap.models.broadcast_domain_svm.BroadcastDomainSvmSchema", unknown=EXCLUDE, data_key="broadcast_domain")
    r""" The broadcast_domain field of the ip_interface_svm_location. """

    home_node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="home_node")
    r""" The home_node field of the ip_interface_svm_location. """

    @property
    def resource(self):
        return IpInterfaceSvmLocation

    gettable_fields = [
        "broadcast_domain.links",
        "broadcast_domain.name",
        "broadcast_domain.uuid",
        "home_node.links",
        "home_node.name",
        "home_node.uuid",
    ]
    """broadcast_domain.links,broadcast_domain.name,broadcast_domain.uuid,home_node.links,home_node.name,home_node.uuid,"""

    patchable_fields = [
        "broadcast_domain.links",
        "broadcast_domain.name",
        "broadcast_domain.uuid",
        "home_node.name",
        "home_node.uuid",
    ]
    """broadcast_domain.links,broadcast_domain.name,broadcast_domain.uuid,home_node.name,home_node.uuid,"""

    postable_fields = [
        "broadcast_domain.links",
        "broadcast_domain.name",
        "broadcast_domain.uuid",
        "home_node.name",
        "home_node.uuid",
    ]
    """broadcast_domain.links,broadcast_domain.name,broadcast_domain.uuid,home_node.name,home_node.uuid,"""


class IpInterfaceSvmLocation(Resource):

    _schema = IpInterfaceSvmLocationSchema
