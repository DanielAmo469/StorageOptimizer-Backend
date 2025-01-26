r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventResponseRecordsParameters", "EmsEventResponseRecordsParametersSchema"]
__pdoc__ = {
    "EmsEventResponseRecordsParametersSchema.resource": False,
    "EmsEventResponseRecordsParametersSchema.opts": False,
    "EmsEventResponseRecordsParameters": False,
}


class EmsEventResponseRecordsParametersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventResponseRecordsParameters object"""

    name = fields.Str(data_key="name")
    r""" Name of parameter

Example: numOps """

    value = fields.Str(data_key="value")
    r""" Value of parameter

Example: 123 """

    @property
    def resource(self):
        return EmsEventResponseRecordsParameters

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


class EmsEventResponseRecordsParameters(Resource):

    _schema = EmsEventResponseRecordsParametersSchema
