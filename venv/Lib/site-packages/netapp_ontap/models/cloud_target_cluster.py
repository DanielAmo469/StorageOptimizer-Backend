r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CloudTargetCluster", "CloudTargetClusterSchema"]
__pdoc__ = {
    "CloudTargetClusterSchema.resource": False,
    "CloudTargetClusterSchema.opts": False,
    "CloudTargetCluster": False,
}


class CloudTargetClusterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CloudTargetCluster object"""

    name = fields.Str(data_key="name")
    r""" The name of the cluster that owns the cloud target. For POST, this accepts the name of the peer cluster only if the cluster is in switchover state. """

    uuid = fields.Str(data_key="uuid")
    r""" The UUID of the cluster that owns the cloud target. For POST, this accepts the UUID of the peer cluster only if the cluster is in switchover state. """

    @property
    def resource(self):
        return CloudTargetCluster

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class CloudTargetCluster(Resource):

    _schema = CloudTargetClusterSchema
