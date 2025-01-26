r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FabricConnections", "FabricConnectionsSchema"]
__pdoc__ = {
    "FabricConnectionsSchema.resource": False,
    "FabricConnectionsSchema.opts": False,
    "FabricConnections": False,
}


class FabricConnectionsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FabricConnections object"""

    cluster_port = fields.Nested("netapp_ontap.models.fabric_connections_cluster_port.FabricConnectionsClusterPortSchema", unknown=EXCLUDE, data_key="cluster_port")
    r""" The cluster_port field of the fabric_connections. """

    switch = fields.Nested("netapp_ontap.models.fabric_connections_switch.FabricConnectionsSwitchSchema", unknown=EXCLUDE, data_key="switch")
    r""" The switch field of the fabric_connections. """

    @property
    def resource(self):
        return FabricConnections

    gettable_fields = [
        "cluster_port",
        "switch",
    ]
    """cluster_port,switch,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FabricConnections(Resource):

    _schema = FabricConnectionsSchema
