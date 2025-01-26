r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchTemperatureSensors", "StorageSwitchTemperatureSensorsSchema"]
__pdoc__ = {
    "StorageSwitchTemperatureSensorsSchema.resource": False,
    "StorageSwitchTemperatureSensorsSchema.opts": False,
    "StorageSwitchTemperatureSensors": False,
}


class StorageSwitchTemperatureSensorsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchTemperatureSensors object"""

    name = fields.Str(data_key="name")
    r""" Temperature sensor name """

    reading = Size(data_key="reading")
    r""" Temperature sensor reading, in degrees celsius. """

    state = fields.Str(data_key="state")
    r""" Temperature sensor state

Valid choices:

* error
* ok """

    @property
    def resource(self):
        return StorageSwitchTemperatureSensors

    gettable_fields = [
        "name",
        "reading",
        "state",
    ]
    """name,reading,state,"""

    patchable_fields = [
        "name",
        "reading",
        "state",
    ]
    """name,reading,state,"""

    postable_fields = [
        "name",
        "reading",
        "state",
    ]
    """name,reading,state,"""


class StorageSwitchTemperatureSensors(Resource):

    _schema = StorageSwitchTemperatureSensorsSchema
