r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MongoDbOnSanNewIgroupsInitiatorObjects", "MongoDbOnSanNewIgroupsInitiatorObjectsSchema"]
__pdoc__ = {
    "MongoDbOnSanNewIgroupsInitiatorObjectsSchema.resource": False,
    "MongoDbOnSanNewIgroupsInitiatorObjectsSchema.opts": False,
    "MongoDbOnSanNewIgroupsInitiatorObjects": False,
}


class MongoDbOnSanNewIgroupsInitiatorObjectsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MongoDbOnSanNewIgroupsInitiatorObjects object"""

    comment = fields.Str(data_key="comment")
    r""" A comment available for use by the administrator. """

    name = fields.Str(data_key="name")
    r""" The WWPN, IQN, or Alias of the initiator. Mutually exclusive with nested igroups and the initiators array. """

    @property
    def resource(self):
        return MongoDbOnSanNewIgroupsInitiatorObjects

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
        "comment",
        "name",
    ]
    """comment,name,"""

    postable_fields = [
        "comment",
        "name",
    ]
    """comment,name,"""


class MongoDbOnSanNewIgroupsInitiatorObjects(Resource):

    _schema = MongoDbOnSanNewIgroupsInitiatorObjectsSchema
