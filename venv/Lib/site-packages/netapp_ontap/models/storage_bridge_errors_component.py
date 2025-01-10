r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeErrorsComponent", "StorageBridgeErrorsComponentSchema"]
__pdoc__ = {
    "StorageBridgeErrorsComponentSchema.resource": False,
    "StorageBridgeErrorsComponentSchema.opts": False,
    "StorageBridgeErrorsComponent": False,
}


class StorageBridgeErrorsComponentSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeErrorsComponent object"""

    id = Size(data_key="id")
    r""" Bridge error component ID """

    name = fields.Str(data_key="name")
    r""" Bridge error component name """

    unique_id = fields.Str(data_key="unique_id")
    r""" Bridge error component unique ID """

    @property
    def resource(self):
        return StorageBridgeErrorsComponent

    gettable_fields = [
        "id",
        "name",
        "unique_id",
    ]
    """id,name,unique_id,"""

    patchable_fields = [
        "id",
        "name",
        "unique_id",
    ]
    """id,name,unique_id,"""

    postable_fields = [
        "id",
        "name",
        "unique_id",
    ]
    """id,name,unique_id,"""


class StorageBridgeErrorsComponent(Resource):

    _schema = StorageBridgeErrorsComponentSchema
