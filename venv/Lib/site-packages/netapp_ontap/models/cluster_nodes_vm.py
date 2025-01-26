r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesVm", "ClusterNodesVmSchema"]
__pdoc__ = {
    "ClusterNodesVmSchema.resource": False,
    "ClusterNodesVmSchema.opts": False,
    "ClusterNodesVm": False,
}


class ClusterNodesVmSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesVm object"""

    provider_type = fields.Str(data_key="provider_type")
    r""" Cloud provider where the VM is hosted.

Valid choices:

* GoogleCloud
* AWS_S3
* Azure_Cloud """

    @property
    def resource(self):
        return ClusterNodesVm

    gettable_fields = [
        "provider_type",
    ]
    """provider_type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesVm(Resource):

    _schema = ClusterNodesVmSchema
