r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeNamespaceConvertLun", "NvmeNamespaceConvertLunSchema"]
__pdoc__ = {
    "NvmeNamespaceConvertLunSchema.resource": False,
    "NvmeNamespaceConvertLunSchema.opts": False,
    "NvmeNamespaceConvertLun": False,
}


class NvmeNamespaceConvertLunSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeNamespaceConvertLun object"""

    name = fields.Str(data_key="name")
    r""" The fully qualified path name of the source LUN composed of a "/vol" prefix, the volume name, the (optional) qtree name and base name of the LUN. Valid in POST.


Example: /vol/volume1/lun1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the source LUN. Valid in POST.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return NvmeNamespaceConvertLun

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class NvmeNamespaceConvertLun(Resource):

    _schema = NvmeNamespaceConvertLunSchema
