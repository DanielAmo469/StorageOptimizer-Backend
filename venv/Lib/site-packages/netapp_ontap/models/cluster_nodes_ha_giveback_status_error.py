r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaGivebackStatusError", "ClusterNodesHaGivebackStatusErrorSchema"]
__pdoc__ = {
    "ClusterNodesHaGivebackStatusErrorSchema.resource": False,
    "ClusterNodesHaGivebackStatusErrorSchema.opts": False,
    "ClusterNodesHaGivebackStatusError": False,
}


class ClusterNodesHaGivebackStatusErrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaGivebackStatusError object"""

    code = fields.Str(data_key="code")
    r""" Message code.

Example: 852126 """

    message = fields.Str(data_key="message")
    r""" Detailed message based on the state.

Valid choices:

* shutdown
* not_homes_partner
* not_sfo
* failed_limbo
* offline_failed
* migrating
* veto
* communication_err
* online_timeout
* online_failed
* hdd_to_aff_dest """

    @property
    def resource(self):
        return ClusterNodesHaGivebackStatusError

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesHaGivebackStatusError(Resource):

    _schema = ClusterNodesHaGivebackStatusErrorSchema
