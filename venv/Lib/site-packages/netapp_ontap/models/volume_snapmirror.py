r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeSnapmirror", "VolumeSnapmirrorSchema"]
__pdoc__ = {
    "VolumeSnapmirrorSchema.resource": False,
    "VolumeSnapmirrorSchema.opts": False,
    "VolumeSnapmirror": False,
}


class VolumeSnapmirrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeSnapmirror object"""

    destinations = fields.Nested("netapp_ontap.models.volume_snapmirror_destinations.VolumeSnapmirrorDestinationsSchema", unknown=EXCLUDE, data_key="destinations")
    r""" The destinations field of the volume_snapmirror. """

    is_protected = fields.Boolean(data_key="is_protected")
    r""" Specifies whether a volume is a SnapMirror source volume, using SnapMirror to protect its data. """

    @property
    def resource(self):
        return VolumeSnapmirror

    gettable_fields = [
        "destinations",
        "is_protected",
    ]
    """destinations,is_protected,"""

    patchable_fields = [
        "destinations",
    ]
    """destinations,"""

    postable_fields = [
        "destinations",
    ]
    """destinations,"""


class VolumeSnapmirror(Resource):

    _schema = VolumeSnapmirrorSchema
