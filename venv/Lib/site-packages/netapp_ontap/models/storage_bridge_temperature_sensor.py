r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeTemperatureSensor", "StorageBridgeTemperatureSensorSchema"]
__pdoc__ = {
    "StorageBridgeTemperatureSensorSchema.resource": False,
    "StorageBridgeTemperatureSensorSchema.opts": False,
    "StorageBridgeTemperatureSensor": False,
}


class StorageBridgeTemperatureSensorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeTemperatureSensor object"""

    maximum = Size(data_key="maximum")
    r""" Maximum safe operating temperature, in degrees Celsius. """

    minimum = Size(data_key="minimum")
    r""" Minimum safe operating temperature, in degrees Celsius. """

    name = fields.Str(data_key="name")
    r""" Temperature sensor name

Example: Chassis temperature sensor """

    reading = Size(data_key="reading")
    r""" Chassis temperature sensor reading, in degrees Celsius. """

    state = fields.Str(data_key="state")
    r""" The state field of the storage_bridge_temperature_sensor.

Valid choices:

* ok
* warning
* error """

    @property
    def resource(self):
        return StorageBridgeTemperatureSensor

    gettable_fields = [
        "maximum",
        "minimum",
        "name",
        "reading",
        "state",
    ]
    """maximum,minimum,name,reading,state,"""

    patchable_fields = [
        "maximum",
        "minimum",
        "name",
        "reading",
        "state",
    ]
    """maximum,minimum,name,reading,state,"""

    postable_fields = [
        "maximum",
        "minimum",
        "name",
        "reading",
        "state",
    ]
    """maximum,minimum,name,reading,state,"""


class StorageBridgeTemperatureSensor(Resource):

    _schema = StorageBridgeTemperatureSensorSchema
