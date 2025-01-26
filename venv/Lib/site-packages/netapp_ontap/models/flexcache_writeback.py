r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FlexcacheWriteback", "FlexcacheWritebackSchema"]
__pdoc__ = {
    "FlexcacheWritebackSchema.resource": False,
    "FlexcacheWritebackSchema.opts": False,
    "FlexcacheWriteback": False,
}


class FlexcacheWritebackSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FlexcacheWriteback object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Indicates whether or not writeback is enabled for the FlexCache volume. Writeback is a storage method where data is first written to the FlexCache volume and then written to the origin of a FlexCache volume. """

    @property
    def resource(self):
        return FlexcacheWriteback

    gettable_fields = [
        "enabled",
    ]
    """enabled,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
        "enabled",
    ]
    """enabled,"""


class FlexcacheWriteback(Resource):

    _schema = FlexcacheWritebackSchema
