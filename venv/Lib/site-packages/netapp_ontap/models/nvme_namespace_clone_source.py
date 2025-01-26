r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeNamespaceCloneSource", "NvmeNamespaceCloneSourceSchema"]
__pdoc__ = {
    "NvmeNamespaceCloneSourceSchema.resource": False,
    "NvmeNamespaceCloneSourceSchema.opts": False,
    "NvmeNamespaceCloneSource": False,
}


class NvmeNamespaceCloneSourceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeNamespaceCloneSource object"""

    name = fields.Str(data_key="name")
    r""" The fully qualified path name of the clone source NVMe namespace composed of a "/vol" prefix, the volume name, the (optional) qtree name and base name of the namespace. Valid in POST and PATCH.


Example: /vol/volume1/namespace1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the clone source NVMe namespace. Valid in POST and PATCH.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return NvmeNamespaceCloneSource

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class NvmeNamespaceCloneSource(Resource):

    _schema = NvmeNamespaceCloneSourceSchema
