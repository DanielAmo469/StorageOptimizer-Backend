r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationComponentStorageService", "ApplicationComponentStorageServiceSchema"]
__pdoc__ = {
    "ApplicationComponentStorageServiceSchema.resource": False,
    "ApplicationComponentStorageServiceSchema.opts": False,
    "ApplicationComponentStorageService": False,
}


class ApplicationComponentStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationComponentStorageService object"""

    name = fields.Str(data_key="name")
    r""" Storage service name """

    uuid = fields.Str(data_key="uuid")
    r""" Storage service UUID """

    @property
    def resource(self):
        return ApplicationComponentStorageService

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationComponentStorageService(Resource):

    _schema = ApplicationComponentStorageServiceSchema
