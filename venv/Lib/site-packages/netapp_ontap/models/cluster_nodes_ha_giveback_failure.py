r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaGivebackFailure", "ClusterNodesHaGivebackFailureSchema"]
__pdoc__ = {
    "ClusterNodesHaGivebackFailureSchema.resource": False,
    "ClusterNodesHaGivebackFailureSchema.opts": False,
    "ClusterNodesHaGivebackFailure": False,
}


class ClusterNodesHaGivebackFailureSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaGivebackFailure object"""

    code = Size(data_key="code")
    r""" Message code

Example: 852126 """

    message = fields.Str(data_key="message")
    r""" Detailed message based on the state.

Example: Failed to initiate giveback. Run the "storage failover show-giveback" command for more information. """

    @property
    def resource(self):
        return ClusterNodesHaGivebackFailure

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


class ClusterNodesHaGivebackFailure(Resource):

    _schema = ClusterNodesHaGivebackFailureSchema
