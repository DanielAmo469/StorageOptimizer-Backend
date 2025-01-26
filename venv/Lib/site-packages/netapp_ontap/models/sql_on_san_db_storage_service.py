r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SqlOnSanDbStorageService", "SqlOnSanDbStorageServiceSchema"]
__pdoc__ = {
    "SqlOnSanDbStorageServiceSchema.resource": False,
    "SqlOnSanDbStorageServiceSchema.opts": False,
    "SqlOnSanDbStorageService": False,
}


class SqlOnSanDbStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SqlOnSanDbStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the DB.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return SqlOnSanDbStorageService

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class SqlOnSanDbStorageService(Resource):

    _schema = SqlOnSanDbStorageServiceSchema
