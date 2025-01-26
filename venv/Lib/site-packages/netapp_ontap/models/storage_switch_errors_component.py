r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchErrorsComponent", "StorageSwitchErrorsComponentSchema"]
__pdoc__ = {
    "StorageSwitchErrorsComponentSchema.resource": False,
    "StorageSwitchErrorsComponentSchema.opts": False,
    "StorageSwitchErrorsComponent": False,
}


class StorageSwitchErrorsComponentSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchErrorsComponent object"""

    id = Size(data_key="id")
    r""" Error component ID """

    name = fields.Str(data_key="name")
    r""" Error component name """

    @property
    def resource(self):
        return StorageSwitchErrorsComponent

    gettable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    patchable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    postable_fields = [
        "id",
        "name",
    ]
    """id,name,"""


class StorageSwitchErrorsComponent(Resource):

    _schema = StorageSwitchErrorsComponentSchema
