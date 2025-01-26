r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeSnapmirrorDestinations", "VolumeSnapmirrorDestinationsSchema"]
__pdoc__ = {
    "VolumeSnapmirrorDestinationsSchema.resource": False,
    "VolumeSnapmirrorDestinationsSchema.opts": False,
    "VolumeSnapmirrorDestinations": False,
}


class VolumeSnapmirrorDestinationsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeSnapmirrorDestinations object"""

    is_cloud = fields.Boolean(data_key="is_cloud")
    r""" Specifies whether a volume is a SnapMirror source volume, using SnapMirror to protect its data to a cloud destination. """

    is_ontap = fields.Boolean(data_key="is_ontap")
    r""" Specifies whether a volume is a SnapMirror source volume, using SnapMirror to protect its data to an ONTAP destination. """

    @property
    def resource(self):
        return VolumeSnapmirrorDestinations

    gettable_fields = [
        "is_cloud",
        "is_ontap",
    ]
    """is_cloud,is_ontap,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeSnapmirrorDestinations(Resource):

    _schema = VolumeSnapmirrorDestinationsSchema
