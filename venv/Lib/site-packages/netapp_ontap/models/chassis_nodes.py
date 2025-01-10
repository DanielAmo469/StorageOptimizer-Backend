r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ChassisNodes", "ChassisNodesSchema"]
__pdoc__ = {
    "ChassisNodesSchema.resource": False,
    "ChassisNodesSchema.opts": False,
    "ChassisNodes": False,
}


class ChassisNodesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ChassisNodes object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the chassis_nodes. """

    name = fields.Str(data_key="name")
    r""" The name field of the chassis_nodes.

Example: node1 """

    pcis = fields.Nested("netapp_ontap.models.chassis_nodes_pcis.ChassisNodesPcisSchema", unknown=EXCLUDE, data_key="pcis")
    r""" The pcis field of the chassis_nodes. """

    position = fields.Str(data_key="position")
    r""" The position of the node in the chassis, when viewed from the rear of the system.

Valid choices:

* top
* bottom
* left
* right
* unknown """

    usbs = fields.Nested("netapp_ontap.models.chassis_nodes_usbs.ChassisNodesUsbsSchema", unknown=EXCLUDE, data_key="usbs")
    r""" The usbs field of the chassis_nodes. """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the chassis_nodes.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return ChassisNodes

    gettable_fields = [
        "links",
        "name",
        "pcis",
        "position",
        "usbs",
        "uuid",
    ]
    """links,name,pcis,position,usbs,uuid,"""

    patchable_fields = [
        "name",
        "pcis",
        "position",
        "usbs",
        "uuid",
    ]
    """name,pcis,position,usbs,uuid,"""

    postable_fields = [
        "name",
        "pcis",
        "position",
        "usbs",
        "uuid",
    ]
    """name,pcis,position,usbs,uuid,"""


class ChassisNodes(Resource):

    _schema = ChassisNodesSchema
