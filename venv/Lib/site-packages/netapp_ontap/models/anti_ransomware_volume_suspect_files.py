r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeSuspectFiles", "AntiRansomwareVolumeSuspectFilesSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeSuspectFilesSchema.resource": False,
    "AntiRansomwareVolumeSuspectFilesSchema.opts": False,
    "AntiRansomwareVolumeSuspectFiles": False,
}


class AntiRansomwareVolumeSuspectFilesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeSuspectFiles object"""

    count = Size(data_key="count")
    r""" Total number of `suspect_files.format` files observed by the Anti-ransomware analytics engine on the volume. """

    entropy = fields.Str(data_key="entropy")
    r""" Indicates the entropy level of this file type. """

    format = fields.Str(data_key="format")
    r""" File formats observed by the Anti-ransomware analytics engine on the volume. """

    @property
    def resource(self):
        return AntiRansomwareVolumeSuspectFiles

    gettable_fields = [
        "count",
        "entropy",
        "format",
    ]
    """count,entropy,format,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AntiRansomwareVolumeSuspectFiles(Resource):

    _schema = AntiRansomwareVolumeSuspectFilesSchema
