r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnaplockLitigationFileResponseRecords", "SnaplockLitigationFileResponseRecordsSchema"]
__pdoc__ = {
    "SnaplockLitigationFileResponseRecordsSchema.resource": False,
    "SnaplockLitigationFileResponseRecordsSchema.opts": False,
    "SnaplockLitigationFileResponseRecords": False,
}


class SnaplockLitigationFileResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnaplockLitigationFileResponseRecords object"""

    file = fields.List(fields.Str, data_key="file")
    r""" Name of the file including the path from the root. """

    sequence_index = Size(data_key="sequence_index")
    r""" Sequence index of files path list. """

    @property
    def resource(self):
        return SnaplockLitigationFileResponseRecords

    gettable_fields = [
        "file",
        "sequence_index",
    ]
    """file,sequence_index,"""

    patchable_fields = [
        "file",
        "sequence_index",
    ]
    """file,sequence_index,"""

    postable_fields = [
        "file",
        "sequence_index",
    ]
    """file,sequence_index,"""


class SnaplockLitigationFileResponseRecords(Resource):

    _schema = SnaplockLitigationFileResponseRecordsSchema
