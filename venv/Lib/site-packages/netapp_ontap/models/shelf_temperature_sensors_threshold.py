r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfTemperatureSensorsThreshold", "ShelfTemperatureSensorsThresholdSchema"]
__pdoc__ = {
    "ShelfTemperatureSensorsThresholdSchema.resource": False,
    "ShelfTemperatureSensorsThresholdSchema.opts": False,
    "ShelfTemperatureSensorsThreshold": False,
}


class ShelfTemperatureSensorsThresholdSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfTemperatureSensorsThreshold object"""

    high = fields.Nested("netapp_ontap.models.shelf_temperature_sensors_threshold_high.ShelfTemperatureSensorsThresholdHighSchema", unknown=EXCLUDE, data_key="high")
    r""" The high field of the shelf_temperature_sensors_threshold. """

    low = fields.Nested("netapp_ontap.models.shelf_temperature_sensors_threshold_low.ShelfTemperatureSensorsThresholdLowSchema", unknown=EXCLUDE, data_key="low")
    r""" The low field of the shelf_temperature_sensors_threshold. """

    @property
    def resource(self):
        return ShelfTemperatureSensorsThreshold

    gettable_fields = [
        "high",
        "low",
    ]
    """high,low,"""

    patchable_fields = [
        "high",
        "low",
    ]
    """high,low,"""

    postable_fields = [
        "high",
        "low",
    ]
    """high,low,"""


class ShelfTemperatureSensorsThreshold(Resource):

    _schema = ShelfTemperatureSensorsThresholdSchema
