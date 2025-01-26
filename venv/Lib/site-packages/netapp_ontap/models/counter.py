r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["Counter", "CounterSchema"]
__pdoc__ = {
    "CounterSchema.resource": False,
    "CounterSchema.opts": False,
    "Counter": False,
}


class CounterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Counter object"""

    counters = fields.List(fields.Nested("netapp_ontap.models.counter2d.Counter2dSchema", unknown=EXCLUDE), data_key="counters")
    r""" List of labels and values for the second dimension. """

    labels = fields.List(fields.Str, data_key="labels")
    r""" List of labels for the first dimension. """

    name = fields.Str(data_key="name")
    r""" Counter name. """

    value = Size(data_key="value")
    r""" Scalar value. """

    values = fields.List(Size, data_key="values")
    r""" List of values in a one-dimensional counter. """

    @property
    def resource(self):
        return Counter

    gettable_fields = [
        "counters",
        "labels",
        "name",
        "value",
        "values",
    ]
    """counters,labels,name,value,values,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class Counter(Resource):

    _schema = CounterSchema
