r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeFcPortsSfp", "StorageBridgeFcPortsSfpSchema"]
__pdoc__ = {
    "StorageBridgeFcPortsSfpSchema.resource": False,
    "StorageBridgeFcPortsSfpSchema.opts": False,
    "StorageBridgeFcPortsSfp": False,
}


class StorageBridgeFcPortsSfpSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeFcPortsSfp object"""

    data_rate_capability = fields.Number(data_key="data_rate_capability")
    r""" Bridge FC port SFP data rate capability, in Gbps """

    part_number = fields.Str(data_key="part_number")
    r""" The part_number field of the storage_bridge_fc_ports_sfp. """

    serial_number = fields.Str(data_key="serial_number")
    r""" Bridge FC port SFP serial number """

    vendor = fields.Str(data_key="vendor")
    r""" Bridge FC port SFP vendor """

    @property
    def resource(self):
        return StorageBridgeFcPortsSfp

    gettable_fields = [
        "data_rate_capability",
        "part_number",
        "serial_number",
        "vendor",
    ]
    """data_rate_capability,part_number,serial_number,vendor,"""

    patchable_fields = [
        "data_rate_capability",
        "part_number",
        "serial_number",
        "vendor",
    ]
    """data_rate_capability,part_number,serial_number,vendor,"""

    postable_fields = [
        "data_rate_capability",
        "part_number",
        "serial_number",
        "vendor",
    ]
    """data_rate_capability,part_number,serial_number,vendor,"""


class StorageBridgeFcPortsSfp(Resource):

    _schema = StorageBridgeFcPortsSfpSchema
