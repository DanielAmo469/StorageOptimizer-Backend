r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MongoDbOnSanDatasetStorageService", "MongoDbOnSanDatasetStorageServiceSchema"]
__pdoc__ = {
    "MongoDbOnSanDatasetStorageServiceSchema.resource": False,
    "MongoDbOnSanDatasetStorageServiceSchema.opts": False,
    "MongoDbOnSanDatasetStorageService": False,
}


class MongoDbOnSanDatasetStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MongoDbOnSanDatasetStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the database.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return MongoDbOnSanDatasetStorageService

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


class MongoDbOnSanDatasetStorageService(Resource):

    _schema = MongoDbOnSanDatasetStorageServiceSchema
