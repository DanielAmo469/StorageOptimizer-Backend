r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FlexcacheRelativeSize", "FlexcacheRelativeSizeSchema"]
__pdoc__ = {
    "FlexcacheRelativeSizeSchema.resource": False,
    "FlexcacheRelativeSizeSchema.opts": False,
    "FlexcacheRelativeSize": False,
}


class FlexcacheRelativeSizeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FlexcacheRelativeSize object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether the relative sizing is enabled for the FlexCache volume. Relative sizing is introduced as a part of follow the origin feature. When relative sizing is enabled, it blocks any modifications done manually in the absolute size of the FlexCache volume. The size of the FlexCache volume is calculated and entered automatically based on the size of the origin volume. """

    percentage = Size(data_key="percentage")
    r""" Specifies the percent size the FlexCache volume should have relative to the total size of the origin volume. This property is only relevant to a FlexCache volume that has the relative size property enabled. """

    @property
    def resource(self):
        return FlexcacheRelativeSize

    gettable_fields = [
        "enabled",
        "percentage",
    ]
    """enabled,percentage,"""

    patchable_fields = [
        "enabled",
        "percentage",
    ]
    """enabled,percentage,"""

    postable_fields = [
        "enabled",
        "percentage",
    ]
    """enabled,percentage,"""


class FlexcacheRelativeSize(Resource):

    _schema = FlexcacheRelativeSizeSchema
