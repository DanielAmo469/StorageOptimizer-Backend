r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["UnixGroupUsers1", "UnixGroupUsers1Schema"]
__pdoc__ = {
    "UnixGroupUsers1Schema.resource": False,
    "UnixGroupUsers1Schema.opts": False,
    "UnixGroupUsers1": False,
}


class UnixGroupUsers1Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the UnixGroupUsers1 object"""

    name = fields.Str(data_key="name")
    r""" UNIX user who belongs to the specified UNIX group and the SVM. """

    @property
    def resource(self):
        return UnixGroupUsers1

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


class UnixGroupUsers1(Resource):

    _schema = UnixGroupUsers1Schema
