r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesServiceProcessorAutoConfig", "ClusterNodesServiceProcessorAutoConfigSchema"]
__pdoc__ = {
    "ClusterNodesServiceProcessorAutoConfigSchema.resource": False,
    "ClusterNodesServiceProcessorAutoConfigSchema.opts": False,
    "ClusterNodesServiceProcessorAutoConfig": False,
}


class ClusterNodesServiceProcessorAutoConfigSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesServiceProcessorAutoConfig object"""

    ipv4_subnet = fields.Str(data_key="ipv4_subnet")
    r""" Indicates the service processor auto configuration IPv4 subnet name. To enable IPv4 auto-config give the subnet name, give the value as null or an empty string "" to disable auto-config.

Example: ipv4_mgmt """

    ipv6_subnet = fields.Str(data_key="ipv6_subnet")
    r""" Indicates the service processor auto configuration IPv6 subnet name. To enable IPv6 auto-config give the subnet name, give the value as null or an empty string "" to disable auto-config.

Example: ipv6_mgmt """

    @property
    def resource(self):
        return ClusterNodesServiceProcessorAutoConfig

    gettable_fields = [
        "ipv4_subnet",
        "ipv6_subnet",
    ]
    """ipv4_subnet,ipv6_subnet,"""

    patchable_fields = [
        "ipv4_subnet",
        "ipv6_subnet",
    ]
    """ipv4_subnet,ipv6_subnet,"""

    postable_fields = [
        "ipv4_subnet",
        "ipv6_subnet",
    ]
    """ipv4_subnet,ipv6_subnet,"""


class ClusterNodesServiceProcessorAutoConfig(Resource):

    _schema = ClusterNodesServiceProcessorAutoConfigSchema
