r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcSwitchPorts", "FcSwitchPortsSchema"]
__pdoc__ = {
    "FcSwitchPortsSchema.resource": False,
    "FcSwitchPortsSchema.opts": False,
    "FcSwitchPorts": False,
}


class FcSwitchPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcSwitchPorts object"""

    attached_device = fields.Nested("netapp_ontap.models.fc_switch_ports_attached_device.FcSwitchPortsAttachedDeviceSchema", unknown=EXCLUDE, data_key="attached_device")
    r""" The attached_device field of the fc_switch_ports. """

    slot = fields.Str(data_key="slot")
    r""" The slot of the Fibre Channel switch port.


Example: 1 """

    state = fields.Str(data_key="state")
    r""" The state of the Fibre Channel switch port.


Valid choices:

* unknown
* online
* offline
* testing
* fault """

    type = fields.Str(data_key="type")
    r""" The type of the Fibre Channel switch port.


Valid choices:

* b_port
* e_port
* f_port
* fl_port
* fnl_port
* fv_port
* n_port
* nl_port
* nv_port
* nx_port
* sd_port
* te_port
* tf_port
* tl_port
* tnp_port
* none """

    wwpn = fields.Str(data_key="wwpn")
    r""" The world wide port name (WWPN) of the Fibre Channel switch port.


Example: 50:0a:31:32:33:34:35:36 """

    @property
    def resource(self):
        return FcSwitchPorts

    gettable_fields = [
        "attached_device",
        "slot",
        "state",
        "type",
        "wwpn",
    ]
    """attached_device,slot,state,type,wwpn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FcSwitchPorts(Resource):

    _schema = FcSwitchPortsSchema
