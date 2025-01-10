r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MongoDbOnSanSecondaryIgroups", "MongoDbOnSanSecondaryIgroupsSchema"]
__pdoc__ = {
    "MongoDbOnSanSecondaryIgroupsSchema.resource": False,
    "MongoDbOnSanSecondaryIgroupsSchema.opts": False,
    "MongoDbOnSanSecondaryIgroups": False,
}


class MongoDbOnSanSecondaryIgroupsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MongoDbOnSanSecondaryIgroups object"""

    name = fields.Str(data_key="name")
    r""" The name of the initiator group for each secondary. """

    @property
    def resource(self):
        return MongoDbOnSanSecondaryIgroups

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


class MongoDbOnSanSecondaryIgroups(Resource):

    _schema = MongoDbOnSanSecondaryIgroupsSchema
