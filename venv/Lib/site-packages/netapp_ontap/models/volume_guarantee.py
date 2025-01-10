r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeGuarantee", "VolumeGuaranteeSchema"]
__pdoc__ = {
    "VolumeGuaranteeSchema.resource": False,
    "VolumeGuaranteeSchema.opts": False,
    "VolumeGuarantee": False,
}


class VolumeGuaranteeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeGuarantee object"""

    honored = fields.Boolean(data_key="honored")
    r""" Is the space guarantee of this volume honored in the aggregate? """

    type = fields.Str(data_key="type")
    r""" The type of space guarantee of this volume in the aggregate.

Valid choices:

* volume
* none """

    @property
    def resource(self):
        return VolumeGuarantee

    gettable_fields = [
        "honored",
        "type",
    ]
    """honored,type,"""

    patchable_fields = [
        "type",
    ]
    """type,"""

    postable_fields = [
        "type",
    ]
    """type,"""


class VolumeGuarantee(Resource):

    _schema = VolumeGuaranteeSchema
