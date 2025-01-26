r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeSetupIp", "NodeSetupIpSchema"]
__pdoc__ = {
    "NodeSetupIpSchema.resource": False,
    "NodeSetupIpSchema.opts": False,
    "NodeSetupIp": False,
}


class NodeSetupIpSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeSetupIp object"""

    address = fields.Str(data_key="address")
    r""" The address field of the node_setup_ip. """

    @property
    def resource(self):
        return NodeSetupIp

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "address",
    ]
    """address,"""


class NodeSetupIp(Resource):

    _schema = NodeSetupIpSchema
