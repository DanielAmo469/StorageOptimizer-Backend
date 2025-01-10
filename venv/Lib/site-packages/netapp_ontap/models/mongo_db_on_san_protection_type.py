r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MongoDbOnSanProtectionType", "MongoDbOnSanProtectionTypeSchema"]
__pdoc__ = {
    "MongoDbOnSanProtectionTypeSchema.resource": False,
    "MongoDbOnSanProtectionTypeSchema.opts": False,
    "MongoDbOnSanProtectionType": False,
}


class MongoDbOnSanProtectionTypeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MongoDbOnSanProtectionType object"""

    local_rpo = fields.Str(data_key="local_rpo")
    r""" The local RPO of the application.

Valid choices:

* hourly
* none """

    remote_rpo = fields.Str(data_key="remote_rpo")
    r""" The remote RPO of the application.

Valid choices:

* none
* zero """

    @property
    def resource(self):
        return MongoDbOnSanProtectionType

    gettable_fields = [
        "local_rpo",
        "remote_rpo",
    ]
    """local_rpo,remote_rpo,"""

    patchable_fields = [
        "local_rpo",
    ]
    """local_rpo,"""

    postable_fields = [
        "local_rpo",
        "remote_rpo",
    ]
    """local_rpo,remote_rpo,"""


class MongoDbOnSanProtectionType(Resource):

    _schema = MongoDbOnSanProtectionTypeSchema
