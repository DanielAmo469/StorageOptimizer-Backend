r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeConstituentsSpaceLogicalSpace", "VolumeConstituentsSpaceLogicalSpaceSchema"]
__pdoc__ = {
    "VolumeConstituentsSpaceLogicalSpaceSchema.resource": False,
    "VolumeConstituentsSpaceLogicalSpaceSchema.opts": False,
    "VolumeConstituentsSpaceLogicalSpace": False,
}


class VolumeConstituentsSpaceLogicalSpaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeConstituentsSpaceLogicalSpace object"""

    available = Size(data_key="available")
    r""" The amount of space available in this volume with storage efficiency space considered used, in bytes. """

    enforcement = fields.Boolean(data_key="enforcement")
    r""" Specifies whether space accounting for operations on the volume is done along with storage efficiency. """

    reporting = fields.Boolean(data_key="reporting")
    r""" Specifies whether space reporting on the volume is done along with storage efficiency. """

    used_by_afs = Size(data_key="used_by_afs")
    r""" The virtual space used by AFS alone (includes volume reserves) and along with storage efficiency, in bytes. """

    @property
    def resource(self):
        return VolumeConstituentsSpaceLogicalSpace

    gettable_fields = [
        "available",
        "enforcement",
        "reporting",
        "used_by_afs",
    ]
    """available,enforcement,reporting,used_by_afs,"""

    patchable_fields = [
        "enforcement",
        "reporting",
    ]
    """enforcement,reporting,"""

    postable_fields = [
        "enforcement",
        "reporting",
    ]
    """enforcement,reporting,"""


class VolumeConstituentsSpaceLogicalSpace(Resource):

    _schema = VolumeConstituentsSpaceLogicalSpaceSchema
