r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["UnixGroupUsersNoRecords", "UnixGroupUsersNoRecordsSchema"]
__pdoc__ = {
    "UnixGroupUsersNoRecordsSchema.resource": False,
    "UnixGroupUsersNoRecordsSchema.opts": False,
    "UnixGroupUsersNoRecords": False,
}


class UnixGroupUsersNoRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the UnixGroupUsersNoRecords object"""

    name = fields.Str(data_key="name")
    r""" UNIX user who belongs to the specified UNIX group and the SVM. """

    @property
    def resource(self):
        return UnixGroupUsersNoRecords

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


class UnixGroupUsersNoRecords(Resource):

    _schema = UnixGroupUsersNoRecordsSchema
