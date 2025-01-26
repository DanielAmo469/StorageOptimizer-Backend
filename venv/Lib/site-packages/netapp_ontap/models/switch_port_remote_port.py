r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SwitchPortRemotePort", "SwitchPortRemotePortSchema"]
__pdoc__ = {
    "SwitchPortRemotePortSchema.resource": False,
    "SwitchPortRemotePortSchema.opts": False,
    "SwitchPortRemotePort": False,
}


class SwitchPortRemotePortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SwitchPortRemotePort object"""

    device = fields.Nested("netapp_ontap.models.switch_port_remote_port_device.SwitchPortRemotePortDeviceSchema", unknown=EXCLUDE, data_key="device")
    r""" The device field of the switch_port_remote_port. """

    mtu = Size(data_key="mtu")
    r""" MTU in octets. """

    name = fields.Str(data_key="name")
    r""" Port Name. """

    @property
    def resource(self):
        return SwitchPortRemotePort

    gettable_fields = [
        "device",
        "mtu",
        "name",
    ]
    """device,mtu,name,"""

    patchable_fields = [
        "device",
    ]
    """device,"""

    postable_fields = [
        "device",
    ]
    """device,"""


class SwitchPortRemotePort(Resource):

    _schema = SwitchPortRemotePortSchema
