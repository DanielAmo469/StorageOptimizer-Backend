r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaTakeoverFailure", "ClusterNodesHaTakeoverFailureSchema"]
__pdoc__ = {
    "ClusterNodesHaTakeoverFailureSchema.resource": False,
    "ClusterNodesHaTakeoverFailureSchema.opts": False,
    "ClusterNodesHaTakeoverFailure": False,
}


class ClusterNodesHaTakeoverFailureSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaTakeoverFailure object"""

    code = Size(data_key="code")
    r""" Message code

Example: 852130 """

    message = fields.Str(data_key="message")
    r""" Detailed message based on the state.

Example: Failed to initiate takeover. Run the "storage failover show-takeover" command for more information. """

    @property
    def resource(self):
        return ClusterNodesHaTakeoverFailure

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    postable_fields = [
        "code",
        "message",
    ]
    """code,message,"""


class ClusterNodesHaTakeoverFailure(Resource):

    _schema = ClusterNodesHaTakeoverFailureSchema
