r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FileMoveFilesToMove", "FileMoveFilesToMoveSchema"]
__pdoc__ = {
    "FileMoveFilesToMoveSchema.resource": False,
    "FileMoveFilesToMoveSchema.opts": False,
    "FileMoveFilesToMove": False,
}


class FileMoveFilesToMoveSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FileMoveFilesToMove object"""

    destinations = fields.List(fields.Nested("netapp_ontap.models.file_move_file.FileMoveFileSchema", unknown=EXCLUDE), data_key="destinations")
    r""" The destination file information. """

    sources = fields.List(fields.Nested("netapp_ontap.models.file_move_file.FileMoveFileSchema", unknown=EXCLUDE), data_key="sources")
    r""" The source file information. """

    @property
    def resource(self):
        return FileMoveFilesToMove

    gettable_fields = [
        "destinations",
        "sources",
    ]
    """destinations,sources,"""

    patchable_fields = [
        "destinations",
        "sources",
    ]
    """destinations,sources,"""

    postable_fields = [
        "destinations",
        "sources",
    ]
    """destinations,sources,"""


class FileMoveFilesToMove(Resource):

    _schema = FileMoveFilesToMoveSchema
