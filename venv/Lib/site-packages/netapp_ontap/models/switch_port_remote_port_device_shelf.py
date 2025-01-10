r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SwitchPortRemotePortDeviceShelf", "SwitchPortRemotePortDeviceShelfSchema"]
__pdoc__ = {
    "SwitchPortRemotePortDeviceShelfSchema.resource": False,
    "SwitchPortRemotePortDeviceShelfSchema.opts": False,
    "SwitchPortRemotePortDeviceShelf": False,
}


class SwitchPortRemotePortDeviceShelfSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SwitchPortRemotePortDeviceShelf object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the switch_port_remote_port_device_shelf. """

    module = fields.Str(data_key="module")
    r""" Shelf module connected to this port.

Valid choices:

* A
* B """

    name = fields.Str(data_key="name")
    r""" The name field of the switch_port_remote_port_device_shelf.

Example: 1.1 """

    uid = fields.Str(data_key="uid")
    r""" The uid field of the switch_port_remote_port_device_shelf.

Example: 12439000444923584512 """

    @property
    def resource(self):
        return SwitchPortRemotePortDeviceShelf

    gettable_fields = [
        "links",
        "module",
        "name",
        "uid",
    ]
    """links,module,name,uid,"""

    patchable_fields = [
        "module",
        "name",
        "uid",
    ]
    """module,name,uid,"""

    postable_fields = [
        "module",
        "name",
        "uid",
    ]
    """module,name,uid,"""


class SwitchPortRemotePortDeviceShelf(Resource):

    _schema = SwitchPortRemotePortDeviceShelfSchema
