r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeNamespaceClone", "NvmeNamespaceCloneSchema"]
__pdoc__ = {
    "NvmeNamespaceCloneSchema.resource": False,
    "NvmeNamespaceCloneSchema.opts": False,
    "NvmeNamespaceClone": False,
}


class NvmeNamespaceCloneSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeNamespaceClone object"""

    source = fields.Nested("netapp_ontap.models.nvme_namespace_clone_source.NvmeNamespaceCloneSourceSchema", unknown=EXCLUDE, data_key="source")
    r""" The source field of the nvme_namespace_clone. """

    @property
    def resource(self):
        return NvmeNamespaceClone

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
        "source",
    ]
    """source,"""

    postable_fields = [
        "source",
    ]
    """source,"""


class NvmeNamespaceClone(Resource):

    _schema = NvmeNamespaceCloneSchema
