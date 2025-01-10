r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunCopy", "LunCopySchema"]
__pdoc__ = {
    "LunCopySchema.resource": False,
    "LunCopySchema.opts": False,
    "LunCopy": False,
}


class LunCopySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunCopy object"""

    destinations = fields.List(fields.Nested("netapp_ontap.models.lun_copy_destinations.LunCopyDestinationsSchema", unknown=EXCLUDE), data_key="destinations")
    r""" An array of destination LUNs of LUN copy operations in which the containing LUN is the source of the copy. """

    source = fields.Nested("netapp_ontap.models.lun_copy_source.LunCopySourceSchema", unknown=EXCLUDE, data_key="source")
    r""" The source field of the lun_copy. """

    @property
    def resource(self):
        return LunCopy

    gettable_fields = [
        "destinations",
        "source",
    ]
    """destinations,source,"""

    patchable_fields = [
        "source",
    ]
    """source,"""

    postable_fields = [
        "source",
    ]
    """source,"""


class LunCopy(Resource):

    _schema = LunCopySchema
