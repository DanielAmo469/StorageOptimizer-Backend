r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeAsynchronousDirectoryDelete", "VolumeAsynchronousDirectoryDeleteSchema"]
__pdoc__ = {
    "VolumeAsynchronousDirectoryDeleteSchema.resource": False,
    "VolumeAsynchronousDirectoryDeleteSchema.opts": False,
    "VolumeAsynchronousDirectoryDelete": False,
}


class VolumeAsynchronousDirectoryDeleteSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeAsynchronousDirectoryDelete object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether asynchronous directory delete from the client is enabled on the volume. """

    trash_bin = fields.Str(data_key="trash_bin")
    r""" Name of the trash bin directory. If no "trash_bin" property is specified when enabling, the default trash bin name, "._ontaptrashbin", is used. """

    @property
    def resource(self):
        return VolumeAsynchronousDirectoryDelete

    gettable_fields = [
        "enabled",
        "trash_bin",
    ]
    """enabled,trash_bin,"""

    patchable_fields = [
        "enabled",
        "trash_bin",
    ]
    """enabled,trash_bin,"""

    postable_fields = [
        "enabled",
        "trash_bin",
    ]
    """enabled,trash_bin,"""


class VolumeAsynchronousDirectoryDelete(Resource):

    _schema = VolumeAsynchronousDirectoryDeleteSchema
