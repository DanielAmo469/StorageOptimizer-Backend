r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfTemperatureSensorsThresholdLow", "ShelfTemperatureSensorsThresholdLowSchema"]
__pdoc__ = {
    "ShelfTemperatureSensorsThresholdLowSchema.resource": False,
    "ShelfTemperatureSensorsThresholdLowSchema.opts": False,
    "ShelfTemperatureSensorsThresholdLow": False,
}


class ShelfTemperatureSensorsThresholdLowSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfTemperatureSensorsThresholdLow object"""

    critical = Size(data_key="critical")
    r""" Low critical threshold, in degrees Celsius

Example: 0 """

    warning = Size(data_key="warning")
    r""" Low warning threshold, in degrees Celsius

Example: 5 """

    @property
    def resource(self):
        return ShelfTemperatureSensorsThresholdLow

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


class ShelfTemperatureSensorsThresholdLow(Resource):

    _schema = ShelfTemperatureSensorsThresholdLowSchema
