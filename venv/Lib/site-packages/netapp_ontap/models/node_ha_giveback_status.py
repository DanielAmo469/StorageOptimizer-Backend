r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeHaGivebackStatus", "NodeHaGivebackStatusSchema"]
__pdoc__ = {
    "NodeHaGivebackStatusSchema.resource": False,
    "NodeHaGivebackStatusSchema.opts": False,
    "NodeHaGivebackStatus": False,
}


class NodeHaGivebackStatusSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeHaGivebackStatus object"""

    aggregate = fields.Nested("netapp_ontap.resources.aggregate.AggregateSchema", unknown=EXCLUDE, data_key="aggregate")
    r""" The aggregate field of the node_ha_giveback_status. """

    error = fields.Nested("netapp_ontap.models.cluster_nodes_ha_giveback_status_error.ClusterNodesHaGivebackStatusErrorSchema", unknown=EXCLUDE, data_key="error")
    r""" The error field of the node_ha_giveback_status. """

    state = fields.Str(data_key="state")
    r""" Giveback state of the aggregate. <br/>
Possible values include no aggregates to giveback(nothing_to_giveback), failed to disable background disk firmware update(BDFU) on source node(failed_bdfu_source), <br/>
giveback delayed as disk firmware update is in progress on source node(delayed_bdfu_source), performing veto checks(running_checks). <br/>


Valid choices:

* done
* failed
* in_progress
* not_started
* nothing_to_giveback
* failed_bdfu_source
* failed_bdfu_dest
* delayed_bdfu_source
* delayed_bdfu_dest
* running_checks """

    @property
    def resource(self):
        return NodeHaGivebackStatus

    gettable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "error",
        "state",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,error,state,"""

    patchable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "error",
        "state",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,error,state,"""

    postable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "error",
        "state",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,error,state,"""


class NodeHaGivebackStatus(Resource):

    _schema = NodeHaGivebackStatusSchema
