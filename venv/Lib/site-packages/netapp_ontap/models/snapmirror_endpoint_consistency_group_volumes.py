r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapmirrorEndpointConsistencyGroupVolumes", "SnapmirrorEndpointConsistencyGroupVolumesSchema"]
__pdoc__ = {
    "SnapmirrorEndpointConsistencyGroupVolumesSchema.resource": False,
    "SnapmirrorEndpointConsistencyGroupVolumesSchema.opts": False,
    "SnapmirrorEndpointConsistencyGroupVolumes": False,
}


class SnapmirrorEndpointConsistencyGroupVolumesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapmirrorEndpointConsistencyGroupVolumes object"""

    name = fields.Str(data_key="name")
    r""" The name of the volume.

Example: volume1 """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier for the volume. This corresponds to the instance-uuid that is exposed in the CLI and ONTAPI. It does not change due to a volume move.

Example: 028baa66-41bd-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return SnapmirrorEndpointConsistencyGroupVolumes

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
        "uuid",
    ]
    """name,uuid,"""


class SnapmirrorEndpointConsistencyGroupVolumes(Resource):

    _schema = SnapmirrorEndpointConsistencyGroupVolumesSchema
