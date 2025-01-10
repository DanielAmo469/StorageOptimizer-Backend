r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfPortsCable", "ShelfPortsCableSchema"]
__pdoc__ = {
    "ShelfPortsCableSchema.resource": False,
    "ShelfPortsCableSchema.opts": False,
    "ShelfPortsCable": False,
}


class ShelfPortsCableSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfPortsCable object"""

    identifier = fields.Str(data_key="identifier")
    r""" The identifier field of the shelf_ports_cable.

Example: 500a0980000b6c3f-50000d1703544b80 """

    length = fields.Str(data_key="length")
    r""" The length field of the shelf_ports_cable.

Example: 2m """

    part_number = fields.Str(data_key="part_number")
    r""" The part_number field of the shelf_ports_cable.

Example: 112-00431+A0 """

    serial_number = fields.Str(data_key="serial_number")
    r""" The serial_number field of the shelf_ports_cable.

Example: 616930439 """

    @property
    def resource(self):
        return ShelfPortsCable

    gettable_fields = [
        "identifier",
        "length",
        "part_number",
        "serial_number",
    ]
    """identifier,length,part_number,serial_number,"""

    patchable_fields = [
        "identifier",
        "length",
        "part_number",
        "serial_number",
    ]
    """identifier,length,part_number,serial_number,"""

    postable_fields = [
        "identifier",
        "length",
        "part_number",
        "serial_number",
    ]
    """identifier,length,part_number,serial_number,"""


class ShelfPortsCable(Resource):

    _schema = ShelfPortsCableSchema
