r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfTemperatureSensorsThresholdHigh", "ShelfTemperatureSensorsThresholdHighSchema"]
__pdoc__ = {
    "ShelfTemperatureSensorsThresholdHighSchema.resource": False,
    "ShelfTemperatureSensorsThresholdHighSchema.opts": False,
    "ShelfTemperatureSensorsThresholdHigh": False,
}


class ShelfTemperatureSensorsThresholdHighSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfTemperatureSensorsThresholdHigh object"""

    critical = Size(data_key="critical")
    r""" High critical threshold, in degrees Celsius

Example: 60 """

    warning = Size(data_key="warning")
    r""" High warning threshold, in degrees Celsius

Example: 55 """

    @property
    def resource(self):
        return ShelfTemperatureSensorsThresholdHigh

    gettable_fields = [
        "critical",
        "warning",
    ]
    """critical,warning,"""

    patchable_fields = [
        "critical",
        "warning",
    ]
    """critical,warning,"""

    postable_fields = [
        "critical",
        "warning",
    ]
    """critical,warning,"""


class ShelfTemperatureSensorsThresholdHigh(Resource):

    _schema = ShelfTemperatureSensorsThresholdHighSchema
