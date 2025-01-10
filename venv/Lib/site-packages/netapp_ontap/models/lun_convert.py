r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunConvert", "LunConvertSchema"]
__pdoc__ = {
    "LunConvertSchema.resource": False,
    "LunConvertSchema.opts": False,
    "LunConvert": False,
}


class LunConvertSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunConvert object"""

    namespace = fields.Nested("netapp_ontap.models.lun_convert_namespace.LunConvertNamespaceSchema", unknown=EXCLUDE, data_key="namespace")
    r""" The namespace field of the lun_convert. """

    @property
    def resource(self):
        return LunConvert

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "namespace",
    ]
    """namespace,"""


class LunConvert(Resource):

    _schema = LunConvertSchema
