r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CounterSchema", "CounterSchemaSchema"]
__pdoc__ = {
    "CounterSchemaSchema.resource": False,
    "CounterSchemaSchema.opts": False,
    "CounterSchema": False,
}


class CounterSchemaSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CounterSchema object"""

    denominator = fields.Nested("netapp_ontap.models.counter_denominator.CounterDenominatorSchema", unknown=EXCLUDE, data_key="denominator")
    r""" The denominator field of the counter_schema. """

    description = fields.Str(data_key="description")
    r""" Counter or property description. """

    name = fields.Str(data_key="name")
    r""" Counter or property name. """

    type = fields.Str(data_key="type")
    r""" Type of counter or property. Properties will always set this field to 'string'.


Valid choices:

* average
* rate
* raw
* delta
* percent
* string """

    unit = fields.Str(data_key="unit")
    r""" Counter unit.

Valid choices:

* per_sec
* b_per_sec
* kb_per_sec
* mb_per_sec
* percent
* millisec
* microsec
* nanosec
* sec
* none """

    @property
    def resource(self):
        return CounterSchema

    gettable_fields = [
        "denominator",
        "description",
        "name",
        "type",
        "unit",
    ]
    """denominator,description,name,type,unit,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class CounterSchema(Resource):

    _schema = CounterSchemaSchema
