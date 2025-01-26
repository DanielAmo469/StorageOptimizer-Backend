r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ReferenceFileMoveFile", "ReferenceFileMoveFileSchema"]
__pdoc__ = {
    "ReferenceFileMoveFileSchema.resource": False,
    "ReferenceFileMoveFileSchema.opts": False,
    "ReferenceFileMoveFile": False,
}


class ReferenceFileMoveFileSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ReferenceFileMoveFile object"""

    max_cutover_time = Size(data_key="max_cutover_time")
    r""" The maximum amount of time, in seconds that the source reference file can be quiesced before the corresponding destination file must be made available for read-write traffic. Not supported in FlexGroup volume file move operations.


Example: 5 """

    path = fields.Str(data_key="path")
    r""" The source reference file. If a reference file is specified, data for other files being moved will be transferred as a difference from the reference file. This can save bandwidth and destination storage if the specified source files share blocks. If provided, this input must match one of the source file paths. This input need not be provided if only one source file is specified. Not supported in FlexGroup volume file move operations. """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the reference_file_move_file. """

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE, data_key="volume")
    r""" The volume field of the reference_file_move_file. """

    @property
    def resource(self):
        return ReferenceFileMoveFile

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "max_cutover_time",
        "path",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """max_cutover_time,path,svm.name,svm.uuid,volume.name,volume.uuid,"""


class ReferenceFileMoveFile(Resource):

    _schema = ReferenceFileMoveFileSchema
