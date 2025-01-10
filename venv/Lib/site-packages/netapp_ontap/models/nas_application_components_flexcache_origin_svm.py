r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsFlexcacheOriginSvm", "NasApplicationComponentsFlexcacheOriginSvmSchema"]
__pdoc__ = {
    "NasApplicationComponentsFlexcacheOriginSvmSchema.resource": False,
    "NasApplicationComponentsFlexcacheOriginSvmSchema.opts": False,
    "NasApplicationComponentsFlexcacheOriginSvm": False,
}


class NasApplicationComponentsFlexcacheOriginSvmSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsFlexcacheOriginSvm object"""

    name = fields.Str(data_key="name")
    r""" Name of the source SVM. """

    @property
    def resource(self):
        return NasApplicationComponentsFlexcacheOriginSvm

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
    ]
    """name,"""


class NasApplicationComponentsFlexcacheOriginSvm(Resource):

    _schema = NasApplicationComponentsFlexcacheOriginSvmSchema
