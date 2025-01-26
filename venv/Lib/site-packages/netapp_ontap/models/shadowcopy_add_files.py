r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShadowcopyAddFiles", "ShadowcopyAddFilesSchema"]
__pdoc__ = {
    "ShadowcopyAddFilesSchema.resource": False,
    "ShadowcopyAddFilesSchema.opts": False,
    "ShadowcopyAddFiles": False,
}


class ShadowcopyAddFilesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShadowcopyAddFiles object"""

    storage_shadowcopy_set_uuid = fields.Str(data_key="storage_shadowcopy_set_uuid")
    r""" The universally-unique identifier of the storage's shadow copy set.

Example: f8328660-00e6-11e6-80d9-005056bd65a9 """

    storage_shadowcopy_uuid = fields.Str(data_key="storage_shadowcopy_uuid")
    r""" The universally-unique identifier of the storage's shadow copy.

Example: fef32805-1f19-40ba-9b82-ebf277517e7e """

    @property
    def resource(self):
        return ShadowcopyAddFiles

    gettable_fields = [
        "storage_shadowcopy_set_uuid",
        "storage_shadowcopy_uuid",
    ]
    """storage_shadowcopy_set_uuid,storage_shadowcopy_uuid,"""

    patchable_fields = [
        "storage_shadowcopy_set_uuid",
        "storage_shadowcopy_uuid",
    ]
    """storage_shadowcopy_set_uuid,storage_shadowcopy_uuid,"""

    postable_fields = [
        "storage_shadowcopy_set_uuid",
        "storage_shadowcopy_uuid",
    ]
    """storage_shadowcopy_set_uuid,storage_shadowcopy_uuid,"""


class ShadowcopyAddFiles(Resource):

    _schema = ShadowcopyAddFilesSchema
