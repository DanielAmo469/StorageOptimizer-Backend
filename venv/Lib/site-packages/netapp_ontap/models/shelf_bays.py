r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfBays", "ShelfBaysSchema"]
__pdoc__ = {
    "ShelfBaysSchema.resource": False,
    "ShelfBaysSchema.opts": False,
    "ShelfBays": False,
}


class ShelfBaysSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfBays object"""

    drawer = fields.Nested("netapp_ontap.models.shelf_bays_drawer.ShelfBaysDrawerSchema", unknown=EXCLUDE, data_key="drawer")
    r""" The drawer field of the shelf_bays. """

    has_disk = fields.Boolean(data_key="has_disk")
    r""" The has_disk field of the shelf_bays. """

    id = Size(data_key="id")
    r""" The id field of the shelf_bays.

Example: 0 """

    state = fields.Str(data_key="state")
    r""" The state field of the shelf_bays.

Valid choices:

* unknown
* ok
* error """

    type = fields.Str(data_key="type")
    r""" The type field of the shelf_bays.

Valid choices:

* unknown
* single_disk
* multi_lun """

    @property
    def resource(self):
        return ShelfBays

    gettable_fields = [
        "drawer",
        "has_disk",
        "id",
        "state",
        "type",
    ]
    """drawer,has_disk,id,state,type,"""

    patchable_fields = [
        "drawer",
        "has_disk",
        "id",
        "state",
        "type",
    ]
    """drawer,has_disk,id,state,type,"""

    postable_fields = [
        "drawer",
        "has_disk",
        "id",
        "state",
        "type",
    ]
    """drawer,has_disk,id,state,type,"""


class ShelfBays(Resource):

    _schema = ShelfBaysSchema
