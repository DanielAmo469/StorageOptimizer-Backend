r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchZonesPort", "StorageSwitchZonesPortSchema"]
__pdoc__ = {
    "StorageSwitchZonesPortSchema.resource": False,
    "StorageSwitchZonesPortSchema.opts": False,
    "StorageSwitchZonesPort": False,
}


class StorageSwitchZonesPortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchZonesPort object"""

    id = fields.Str(data_key="id")
    r""" Storage switch zone port ID """

    name = fields.Str(data_key="name")
    r""" Storage switch zone port """

    @property
    def resource(self):
        return StorageSwitchZonesPort

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


class StorageSwitchZonesPort(Resource):

    _schema = StorageSwitchZonesPortSchema
