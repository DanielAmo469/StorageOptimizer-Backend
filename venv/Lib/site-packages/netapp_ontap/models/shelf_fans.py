r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfFans", "ShelfFansSchema"]
__pdoc__ = {
    "ShelfFansSchema.resource": False,
    "ShelfFansSchema.opts": False,
    "ShelfFans": False,
}


class ShelfFansSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfFans object"""

    id = Size(data_key="id")
    r""" The id field of the shelf_fans.

Example: 1 """

    installed = fields.Boolean(data_key="installed")
    r""" The installed field of the shelf_fans.

Example: true """

    location = fields.Str(data_key="location")
    r""" The location field of the shelf_fans.

Example: rear of the shelf on the lower left power supply """

    rpm = Size(data_key="rpm")
    r""" The rpm field of the shelf_fans.

Example: 3020 """

    state = fields.Str(data_key="state")
    r""" The state field of the shelf_fans.

Valid choices:

* ok
* error """

    @property
    def resource(self):
        return ShelfFans

    gettable_fields = [
        "id",
        "installed",
        "location",
        "rpm",
        "state",
    ]
    """id,installed,location,rpm,state,"""

    patchable_fields = [
        "id",
        "installed",
        "location",
        "rpm",
        "state",
    ]
    """id,installed,location,rpm,state,"""

    postable_fields = [
        "id",
        "installed",
        "location",
        "rpm",
        "state",
    ]
    """id,installed,location,rpm,state,"""


class ShelfFans(Resource):

    _schema = ShelfFansSchema
