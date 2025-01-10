r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgePowerSupplyUnits", "StorageBridgePowerSupplyUnitsSchema"]
__pdoc__ = {
    "StorageBridgePowerSupplyUnitsSchema.resource": False,
    "StorageBridgePowerSupplyUnitsSchema.opts": False,
    "StorageBridgePowerSupplyUnits": False,
}


class StorageBridgePowerSupplyUnitsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgePowerSupplyUnits object"""

    name = fields.Str(data_key="name")
    r""" Power supply unit name """

    state = fields.Str(data_key="state")
    r""" Power supply unit state

Valid choices:

* ok
* error """

    @property
    def resource(self):
        return StorageBridgePowerSupplyUnits

    gettable_fields = [
        "name",
        "state",
    ]
    """name,state,"""

    patchable_fields = [
        "name",
        "state",
    ]
    """name,state,"""

    postable_fields = [
        "name",
        "state",
    ]
    """name,state,"""


class StorageBridgePowerSupplyUnits(Resource):

    _schema = StorageBridgePowerSupplyUnitsSchema
