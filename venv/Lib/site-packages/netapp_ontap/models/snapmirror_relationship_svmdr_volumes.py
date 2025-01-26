r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapmirrorRelationshipSvmdrVolumes", "SnapmirrorRelationshipSvmdrVolumesSchema"]
__pdoc__ = {
    "SnapmirrorRelationshipSvmdrVolumesSchema.resource": False,
    "SnapmirrorRelationshipSvmdrVolumesSchema.opts": False,
    "SnapmirrorRelationshipSvmdrVolumes": False,
}


class SnapmirrorRelationshipSvmdrVolumesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapmirrorRelationshipSvmdrVolumes object"""

    name = fields.Str(data_key="name")
    r""" The name of the volume.

Example: volume1 """

    @property
    def resource(self):
        return SnapmirrorRelationshipSvmdrVolumes

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class SnapmirrorRelationshipSvmdrVolumes(Resource):

    _schema = SnapmirrorRelationshipSvmdrVolumesSchema
