r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FabricConnectionsSwitch", "FabricConnectionsSwitchSchema"]
__pdoc__ = {
    "FabricConnectionsSwitchSchema.resource": False,
    "FabricConnectionsSwitchSchema.opts": False,
    "FabricConnectionsSwitch": False,
}


class FabricConnectionsSwitchSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FabricConnectionsSwitch object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the fabric_connections_switch. """

    port = fields.Nested("netapp_ontap.models.fabric_connections_switch_port.FabricConnectionsSwitchPortSchema", unknown=EXCLUDE, data_key="port")
    r""" The port field of the fabric_connections_switch. """

    wwn = fields.Str(data_key="wwn")
    r""" The world-wide name (WWN) of the Fibre Channel switch to which the cluster node port is attached.


Example: 10:00:b1:b2:b3:b4:b4:b6 """

    @property
    def resource(self):
        return FabricConnectionsSwitch

    gettable_fields = [
        "links",
        "port",
        "wwn",
    ]
    """links,port,wwn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FabricConnectionsSwitch(Resource):

    _schema = FabricConnectionsSwitchSchema
