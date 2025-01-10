r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["InstanceCounterAggregation", "InstanceCounterAggregationSchema"]
__pdoc__ = {
    "InstanceCounterAggregationSchema.resource": False,
    "InstanceCounterAggregationSchema.opts": False,
    "InstanceCounterAggregation": False,
}


class InstanceCounterAggregationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the InstanceCounterAggregation object"""

    complete = fields.Boolean(data_key="complete")
    r""" The aggregation state for this row.
For non-aggregated tables:
  Not present
For aggregated tables:
  If all requests to remote nodes for counter data are successful, then this value will be 'true'.
  If any requests to remote nodes fail, then this value will be 'false'. """

    count = Size(data_key="count")
    r""" Number of nodes included in the aggregation of this counter. """

    @property
    def resource(self):
        return InstanceCounterAggregation

    gettable_fields = [
        "complete",
        "count",
    ]
    """complete,count,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class InstanceCounterAggregation(Resource):

    _schema = InstanceCounterAggregationSchema
