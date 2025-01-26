r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventParameters", "EmsEventParametersSchema"]
__pdoc__ = {
    "EmsEventParametersSchema.resource": False,
    "EmsEventParametersSchema.opts": False,
    "EmsEventParameters": False,
}


class EmsEventParametersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventParameters object"""

    name = fields.Str(data_key="name")
    r""" Name of parameter

Example: numOps """

    value = fields.Str(data_key="value")
    r""" Value of parameter

Example: 123 """

    @property
    def resource(self):
        return EmsEventParameters

    gettable_fields = [
        "name",
        "value",
    ]
    """name,value,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsEventParameters(Resource):

    _schema = EmsEventParametersSchema
