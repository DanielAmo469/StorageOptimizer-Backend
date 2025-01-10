r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FabricConnectionsSwitchPort", "FabricConnectionsSwitchPortSchema"]
__pdoc__ = {
    "FabricConnectionsSwitchPortSchema.resource": False,
    "FabricConnectionsSwitchPortSchema.opts": False,
    "FabricConnectionsSwitchPort": False,
}


class FabricConnectionsSwitchPortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FabricConnectionsSwitchPort object"""

    wwpn = fields.Str(data_key="wwpn")
    r""" The world wide port name (WWPN) of the Fibre Channel switch port.


Example: 50:0a:a1:a2:a3:a4:a5:a6 """

    @property
    def resource(self):
        return FabricConnectionsSwitchPort

    gettable_fields = [
        "wwpn",
    ]
    """wwpn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FabricConnectionsSwitchPort(Resource):

    _schema = FabricConnectionsSwitchPortSchema
