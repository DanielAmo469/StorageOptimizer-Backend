r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcZoneMember", "FcZoneMemberSchema"]
__pdoc__ = {
    "FcZoneMemberSchema.resource": False,
    "FcZoneMemberSchema.opts": False,
    "FcZoneMember": False,
}


class FcZoneMemberSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcZoneMember object"""

    name = fields.Str(data_key="name")
    r""" The identifying property value of the zone member. The format of this value depends on the member type:

  * `port_id`: A zero-filled 6-digit hexadecimal value with a 0x prefer. Example: 0x0000A0.
  * `port_name`: A world-wide name. Example: 10:00:12:34:56:78:9a:bc.
  * `domain_id_port`: A domain ID and a port ID as decimal integers separated by a slash. Example: 1/2.
  * `node_name`: A world-wide name. Example: 10:00:11:22:33:44:55:66.
  * `fabric_port_name`: A world-wide name. Example: 10:00:ab:cd:ef:12:34:56.
#####
The following types might not report a name:

  * `interface`
  * `domain_interface`
  * `ip_address`
  * `symbolic_node_name`
  * `device_alias`


Example: 10:00:12:34:56:78:9a:bc """

    type = fields.Str(data_key="type")
    r""" The type of Fibre Channel zone member. This value should be used to interpret the contents of the `name` property.


Valid choices:

* port_id
* port_name
* domain_id_port
* node_name
* fabric_port_name
* interface
* domain_interface
* ip_address
* symbolic_node_name
* device_alias """

    @property
    def resource(self):
        return FcZoneMember

    gettable_fields = [
        "name",
        "type",
    ]
    """name,type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FcZoneMember(Resource):

    _schema = FcZoneMemberSchema
