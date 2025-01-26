r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventParameter", "EmsEventParameterSchema"]
__pdoc__ = {
    "EmsEventParameterSchema.resource": False,
    "EmsEventParameterSchema.opts": False,
    "EmsEventParameter": False,
}


class EmsEventParameterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventParameter object"""

    name = fields.Str(data_key="name")
    r""" Name of parameter

Example: numOps """

    value = fields.Str(data_key="value")
    r""" Value of parameter

Example: 123 """

    @property
    def resource(self):
        return EmsEventParameter

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


class EmsEventParameter(Resource):

    _schema = EmsEventParameterSchema
