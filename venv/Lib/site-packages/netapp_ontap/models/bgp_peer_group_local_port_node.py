r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["BgpPeerGroupLocalPortNode", "BgpPeerGroupLocalPortNodeSchema"]
__pdoc__ = {
    "BgpPeerGroupLocalPortNodeSchema.resource": False,
    "BgpPeerGroupLocalPortNodeSchema.opts": False,
    "BgpPeerGroupLocalPortNode": False,
}


class BgpPeerGroupLocalPortNodeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the BgpPeerGroupLocalPortNode object"""

    name = fields.Str(data_key="name")
    r""" Name of node on which the port is located.

Example: node1 """

    @property
    def resource(self):
        return BgpPeerGroupLocalPortNode

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class BgpPeerGroupLocalPortNode(Resource):

    _schema = BgpPeerGroupLocalPortNodeSchema
