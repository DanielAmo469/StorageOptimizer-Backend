r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterPeerLocalNetworkInterfaces", "ClusterPeerLocalNetworkInterfacesSchema"]
__pdoc__ = {
    "ClusterPeerLocalNetworkInterfacesSchema.resource": False,
    "ClusterPeerLocalNetworkInterfacesSchema.opts": False,
    "ClusterPeerLocalNetworkInterfaces": False,
}


class ClusterPeerLocalNetworkInterfacesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterPeerLocalNetworkInterfaces object"""

    ip_address = fields.Str(data_key="ip_address")
    r""" List of local intercluster IP addresses. """

    @property
    def resource(self):
        return ClusterPeerLocalNetworkInterfaces

    gettable_fields = [
        "ip_address",
    ]
    """ip_address,"""

    patchable_fields = [
        "ip_address",
    ]
    """ip_address,"""

    postable_fields = [
        "ip_address",
    ]
    """ip_address,"""


class ClusterPeerLocalNetworkInterfaces(Resource):

    _schema = ClusterPeerLocalNetworkInterfacesSchema
