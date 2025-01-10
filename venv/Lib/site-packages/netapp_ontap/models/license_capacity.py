r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LicenseCapacity", "LicenseCapacitySchema"]
__pdoc__ = {
    "LicenseCapacitySchema.resource": False,
    "LicenseCapacitySchema.opts": False,
    "LicenseCapacity": False,
}


class LicenseCapacitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LicenseCapacity object"""

    maximum_size = Size(data_key="maximum_size")
    r""" Licensed capacity size (in bytes) that can be used. """

    used_size = Size(data_key="used_size")
    r""" Capacity that is currently used (in bytes). """

    @property
    def resource(self):
        return LicenseCapacity

    gettable_fields = [
        "maximum_size",
        "used_size",
    ]
    """maximum_size,used_size,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LicenseCapacity(Resource):

    _schema = LicenseCapacitySchema
