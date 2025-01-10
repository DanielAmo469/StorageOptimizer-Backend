r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationCifsPropertiesBackingStorage", "ApplicationCifsPropertiesBackingStorageSchema"]
__pdoc__ = {
    "ApplicationCifsPropertiesBackingStorageSchema.resource": False,
    "ApplicationCifsPropertiesBackingStorageSchema.opts": False,
    "ApplicationCifsPropertiesBackingStorage": False,
}


class ApplicationCifsPropertiesBackingStorageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationCifsPropertiesBackingStorage object"""

    type = fields.Str(data_key="type")
    r""" Backing storage type

Valid choices:

* volume """

    uuid = fields.Str(data_key="uuid")
    r""" Backing storage UUID """

    @property
    def resource(self):
        return ApplicationCifsPropertiesBackingStorage

    gettable_fields = [
        "type",
        "uuid",
    ]
    """type,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationCifsPropertiesBackingStorage(Resource):

    _schema = ApplicationCifsPropertiesBackingStorageSchema
