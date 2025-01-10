r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcInterfaceLocationHomePortNode", "FcInterfaceLocationHomePortNodeSchema"]
__pdoc__ = {
    "FcInterfaceLocationHomePortNodeSchema.resource": False,
    "FcInterfaceLocationHomePortNodeSchema.opts": False,
    "FcInterfaceLocationHomePortNode": False,
}


class FcInterfaceLocationHomePortNodeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcInterfaceLocationHomePortNode object"""

    name = fields.Str(data_key="name")
    r""" The name of the node on which the FC port is located.


Example: node1 """

    @property
    def resource(self):
        return FcInterfaceLocationHomePortNode

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


class FcInterfaceLocationHomePortNode(Resource):

    _schema = FcInterfaceLocationHomePortNodeSchema
