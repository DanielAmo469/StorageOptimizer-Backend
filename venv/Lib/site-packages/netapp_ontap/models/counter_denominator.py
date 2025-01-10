r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CounterDenominator", "CounterDenominatorSchema"]
__pdoc__ = {
    "CounterDenominatorSchema.resource": False,
    "CounterDenominatorSchema.opts": False,
    "CounterDenominator": False,
}


class CounterDenominatorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CounterDenominator object"""

    name = fields.Str(data_key="name")
    r""" Counter name. """

    @property
    def resource(self):
        return CounterDenominator

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class CounterDenominator(Resource):

    _schema = CounterDenominatorSchema
