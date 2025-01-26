r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeResponseRecordsHaGiveback", "NodeResponseRecordsHaGivebackSchema"]
__pdoc__ = {
    "NodeResponseRecordsHaGivebackSchema.resource": False,
    "NodeResponseRecordsHaGivebackSchema.opts": False,
    "NodeResponseRecordsHaGiveback": False,
}


class NodeResponseRecordsHaGivebackSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeResponseRecordsHaGiveback object"""

    failure = fields.Nested("netapp_ontap.models.cluster_nodes_ha_giveback_failure.ClusterNodesHaGivebackFailureSchema", unknown=EXCLUDE, data_key="failure")
    r""" The failure field of the node_response_records_ha_giveback. """

    state = fields.Str(data_key="state")
    r""" The state field of the node_response_records_ha_giveback.

Valid choices:

* nothing_to_giveback
* not_attempted
* in_progress
* failed """

    status = fields.List(fields.Nested("netapp_ontap.models.node_ha_giveback_status.NodeHaGivebackStatusSchema", unknown=EXCLUDE), data_key="status")
    r""" Giveback status of each aggregate. """

    @property
    def resource(self):
        return NodeResponseRecordsHaGiveback

    gettable_fields = [
        "failure",
        "state",
        "status",
    ]
    """failure,state,status,"""

    patchable_fields = [
        "failure",
        "state",
    ]
    """failure,state,"""

    postable_fields = [
        "failure",
        "state",
    ]
    """failure,state,"""


class NodeResponseRecordsHaGiveback(Resource):

    _schema = NodeResponseRecordsHaGivebackSchema
