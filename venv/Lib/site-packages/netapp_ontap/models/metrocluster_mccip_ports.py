r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterMccipPorts", "MetroclusterMccipPortsSchema"]
__pdoc__ = {
    "MetroclusterMccipPortsSchema.resource": False,
    "MetroclusterMccipPortsSchema.opts": False,
    "MetroclusterMccipPorts": False,
}


class MetroclusterMccipPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterMccipPorts object"""

    l3_config = fields.Nested("netapp_ontap.models.mccip_port_l3_config.MccipPortL3ConfigSchema", unknown=EXCLUDE, data_key="l3_config")
    r""" The l3_config field of the metrocluster_mccip_ports. """

    name = fields.Str(data_key="name")
    r""" Port name

Example: e1b """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the metrocluster_mccip_ports. """

    uuid = fields.Str(data_key="uuid")
    r""" Port UUID """

    vlan_id = Size(data_key="vlan_id")
    r""" VLAN ID

Example: 200 """

    @property
    def resource(self):
        return MetroclusterMccipPorts

    gettable_fields = [
        "l3_config",
        "name",
        "node.links",
        "node.name",
        "node.uuid",
        "uuid",
        "vlan_id",
    ]
    """l3_config,name,node.links,node.name,node.uuid,uuid,vlan_id,"""

    patchable_fields = [
        "l3_config",
        "name",
        "node.name",
        "node.uuid",
        "uuid",
        "vlan_id",
    ]
    """l3_config,name,node.name,node.uuid,uuid,vlan_id,"""

    postable_fields = [
        "l3_config",
        "name",
        "node.name",
        "node.uuid",
        "uuid",
        "vlan_id",
    ]
    """l3_config,name,node.name,node.uuid,uuid,vlan_id,"""


class MetroclusterMccipPorts(Resource):

    _schema = MetroclusterMccipPortsSchema
