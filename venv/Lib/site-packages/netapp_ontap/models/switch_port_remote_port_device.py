r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SwitchPortRemotePortDevice", "SwitchPortRemotePortDeviceSchema"]
__pdoc__ = {
    "SwitchPortRemotePortDeviceSchema.resource": False,
    "SwitchPortRemotePortDeviceSchema.opts": False,
    "SwitchPortRemotePortDevice": False,
}


class SwitchPortRemotePortDeviceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SwitchPortRemotePortDevice object"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the switch_port_remote_port_device. """

    shelf = fields.Nested("netapp_ontap.models.switch_port_remote_port_device_shelf.SwitchPortRemotePortDeviceShelfSchema", unknown=EXCLUDE, data_key="shelf")
    r""" The shelf field of the switch_port_remote_port_device. """

    @property
    def resource(self):
        return SwitchPortRemotePortDevice

    gettable_fields = [
        "node.links",
        "node.name",
        "node.uuid",
        "shelf",
    ]
    """node.links,node.name,node.uuid,shelf,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
        "shelf",
    ]
    """node.name,node.uuid,shelf,"""

    postable_fields = [
        "node.name",
        "node.uuid",
        "shelf",
    ]
    """node.name,node.uuid,shelf,"""


class SwitchPortRemotePortDevice(Resource):

    _schema = SwitchPortRemotePortDeviceSchema
