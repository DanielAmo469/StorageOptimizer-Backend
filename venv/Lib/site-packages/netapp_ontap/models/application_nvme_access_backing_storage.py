r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationNvmeAccessBackingStorage", "ApplicationNvmeAccessBackingStorageSchema"]
__pdoc__ = {
    "ApplicationNvmeAccessBackingStorageSchema.resource": False,
    "ApplicationNvmeAccessBackingStorageSchema.opts": False,
    "ApplicationNvmeAccessBackingStorage": False,
}


class ApplicationNvmeAccessBackingStorageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationNvmeAccessBackingStorage object"""

    type = fields.Str(data_key="type")
    r""" Backing storage type

Valid choices:

* namespace """

    uuid = fields.Str(data_key="uuid")
    r""" Backing storage UUID """

    @property
    def resource(self):
        return ApplicationNvmeAccessBackingStorage

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


class ApplicationNvmeAccessBackingStorage(Resource):

    _schema = ApplicationNvmeAccessBackingStorageSchema
