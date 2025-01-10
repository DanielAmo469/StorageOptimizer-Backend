r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterRemote", "MetroclusterRemoteSchema"]
__pdoc__ = {
    "MetroclusterRemoteSchema.resource": False,
    "MetroclusterRemoteSchema.opts": False,
    "MetroclusterRemote": False,
}


class MetroclusterRemoteSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterRemote object"""

    cluster = fields.Nested("netapp_ontap.resources.cluster.ClusterSchema", unknown=EXCLUDE, data_key="cluster")
    r""" The cluster field of the metrocluster_remote. """

    configuration_state = fields.Str(data_key="configuration_state")
    r""" Indicates the state of the remote cluster configuration.

Valid choices:

* configuration_error
* configured
* not_configured
* not_reachable
* partially_configured
* unknown """

    mode = fields.Str(data_key="mode")
    r""" Specifies the mode of operation of the remote cluster.

Valid choices:

* normal
* not_configured
* not_reachable
* partial_switchback
* partial_switchover
* switchover
* unknown
* waiting_for_switchback """

    periodic_check_enabled = fields.Boolean(data_key="periodic_check_enabled")
    r""" Indicates whether or not a periodic check is enabled on the remote cluster. """

    @property
    def resource(self):
        return MetroclusterRemote

    gettable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "configuration_state",
        "mode",
        "periodic_check_enabled",
    ]
    """cluster.links,cluster.name,cluster.uuid,configuration_state,mode,periodic_check_enabled,"""

    patchable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,"""

    postable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,"""


class MetroclusterRemote(Resource):

    _schema = MetroclusterRemoteSchema
