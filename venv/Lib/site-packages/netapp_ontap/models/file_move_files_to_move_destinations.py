r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FileMoveFilesToMoveDestinations", "FileMoveFilesToMoveDestinationsSchema"]
__pdoc__ = {
    "FileMoveFilesToMoveDestinationsSchema.resource": False,
    "FileMoveFilesToMoveDestinationsSchema.opts": False,
    "FileMoveFilesToMoveDestinations": False,
}


class FileMoveFilesToMoveDestinationsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FileMoveFilesToMoveDestinations object"""

    path = fields.Str(data_key="path")
    r""" The path field of the file_move_files_to_move_destinations.

Example: d1/d2/file1 """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the file_move_files_to_move_destinations. """

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE, data_key="volume")
    r""" The volume field of the file_move_files_to_move_destinations. """

    @property
    def resource(self):
        return FileMoveFilesToMoveDestinations

    gettable_fields = [
        "path",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """path,svm.links,svm.name,svm.uuid,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "path",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """path,svm.name,svm.uuid,volume.name,volume.uuid,"""

    postable_fields = [
        "path",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """path,svm.name,svm.uuid,volume.name,volume.uuid,"""


class FileMoveFilesToMoveDestinations(Resource):

    _schema = FileMoveFilesToMoveDestinationsSchema
