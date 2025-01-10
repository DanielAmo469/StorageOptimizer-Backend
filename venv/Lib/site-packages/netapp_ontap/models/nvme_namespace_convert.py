r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeNamespaceConvert", "NvmeNamespaceConvertSchema"]
__pdoc__ = {
    "NvmeNamespaceConvertSchema.resource": False,
    "NvmeNamespaceConvertSchema.opts": False,
    "NvmeNamespaceConvert": False,
}


class NvmeNamespaceConvertSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeNamespaceConvert object"""

    lun = fields.Nested("netapp_ontap.models.nvme_namespace_convert_lun.NvmeNamespaceConvertLunSchema", unknown=EXCLUDE, data_key="lun")
    r""" The lun field of the nvme_namespace_convert. """

    @property
    def resource(self):
        return NvmeNamespaceConvert

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "lun",
    ]
    """lun,"""


class NvmeNamespaceConvert(Resource):

    _schema = NvmeNamespaceConvertSchema
