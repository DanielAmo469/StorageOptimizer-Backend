r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesServiceProcessorApiService", "ClusterNodesServiceProcessorApiServiceSchema"]
__pdoc__ = {
    "ClusterNodesServiceProcessorApiServiceSchema.resource": False,
    "ClusterNodesServiceProcessorApiServiceSchema.opts": False,
    "ClusterNodesServiceProcessorApiService": False,
}


class ClusterNodesServiceProcessorApiServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesServiceProcessorApiService object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Indicates whether the service processor API service is enabled. """

    limit_access = fields.Boolean(data_key="limit_access")
    r""" Indicates whether the service processor API service limit access is enabled. """

    port = Size(data_key="port")
    r""" Indicates the port number of service processor API service. """

    @property
    def resource(self):
        return ClusterNodesServiceProcessorApiService

    gettable_fields = [
        "enabled",
        "limit_access",
        "port",
    ]
    """enabled,limit_access,port,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesServiceProcessorApiService(Resource):

    _schema = ClusterNodesServiceProcessorApiServiceSchema
