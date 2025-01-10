r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfBaysDrawer", "ShelfBaysDrawerSchema"]
__pdoc__ = {
    "ShelfBaysDrawerSchema.resource": False,
    "ShelfBaysDrawerSchema.opts": False,
    "ShelfBaysDrawer": False,
}


class ShelfBaysDrawerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfBaysDrawer object"""

    id = Size(data_key="id")
    r""" The drawer containing this bay

Example: 1 """

    slot = Size(data_key="slot")
    r""" The drawer slot for this bay

Example: 0 """

    @property
    def resource(self):
        return ShelfBaysDrawer

    gettable_fields = [
        "id",
        "slot",
    ]
    """id,slot,"""

    patchable_fields = [
        "id",
        "slot",
    ]
    """id,slot,"""

    postable_fields = [
        "id",
        "slot",
    ]
    """id,slot,"""


class ShelfBaysDrawer(Resource):

    _schema = ShelfBaysDrawerSchema
