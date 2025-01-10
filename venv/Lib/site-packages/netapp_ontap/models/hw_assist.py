r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["HwAssist", "HwAssistSchema"]
__pdoc__ = {
    "HwAssistSchema.resource": False,
    "HwAssistSchema.opts": False,
    "HwAssist": False,
}


class HwAssistSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the HwAssist object"""

    status = fields.Nested("netapp_ontap.models.cluster_nodes_hw_assist_status.ClusterNodesHwAssistStatusSchema", unknown=EXCLUDE, data_key="status")
    r""" The status field of the hw_assist. """

    @property
    def resource(self):
        return HwAssist

    gettable_fields = [
        "status",
    ]
    """status,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class HwAssist(Resource):

    _schema = HwAssistSchema
