r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["UnixGroupUsersRecords", "UnixGroupUsersRecordsSchema"]
__pdoc__ = {
    "UnixGroupUsersRecordsSchema.resource": False,
    "UnixGroupUsersRecordsSchema.opts": False,
    "UnixGroupUsersRecords": False,
}


class UnixGroupUsersRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the UnixGroupUsersRecords object"""

    name = fields.Str(data_key="name")
    r""" UNIX user who belongs to the specified UNIX group and the SVM. """

    @property
    def resource(self):
        return UnixGroupUsersRecords

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
    ]
    """name,"""


class UnixGroupUsersRecords(Resource):

    _schema = UnixGroupUsersRecordsSchema
