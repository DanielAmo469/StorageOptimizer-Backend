r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupVolumeSpace", "ConsistencyGroupVolumeSpaceSchema"]
__pdoc__ = {
    "ConsistencyGroupVolumeSpaceSchema.resource": False,
    "ConsistencyGroupVolumeSpaceSchema.opts": False,
    "ConsistencyGroupVolumeSpace": False,
}


class ConsistencyGroupVolumeSpaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupVolumeSpace object"""

    available = Size(data_key="available")
    r""" The available space, in bytes. """

    size = Size(data_key="size")
    r""" Total provisioned size, in bytes. """

    used = Size(data_key="used")
    r""" The virtual space used (includes volume reserves) before storage efficiency, in bytes. """

    @property
    def resource(self):
        return ConsistencyGroupVolumeSpace

    gettable_fields = [
        "available",
        "size",
        "used",
    ]
    """available,size,used,"""

    patchable_fields = [
        "size",
    ]
    """size,"""

    postable_fields = [
        "size",
    ]
    """size,"""


class ConsistencyGroupVolumeSpace(Resource):

    _schema = ConsistencyGroupVolumeSpaceSchema
