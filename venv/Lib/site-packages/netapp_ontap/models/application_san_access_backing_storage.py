r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationSanAccessBackingStorage", "ApplicationSanAccessBackingStorageSchema"]
__pdoc__ = {
    "ApplicationSanAccessBackingStorageSchema.resource": False,
    "ApplicationSanAccessBackingStorageSchema.opts": False,
    "ApplicationSanAccessBackingStorage": False,
}


class ApplicationSanAccessBackingStorageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationSanAccessBackingStorage object"""

    type = fields.Str(data_key="type")
    r""" Backing storage type

Valid choices:

* lun """

    uuid = fields.Str(data_key="uuid")
    r""" Backing storage UUID """

    @property
    def resource(self):
        return ApplicationSanAccessBackingStorage

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


class ApplicationSanAccessBackingStorage(Resource):

    _schema = ApplicationSanAccessBackingStorageSchema
