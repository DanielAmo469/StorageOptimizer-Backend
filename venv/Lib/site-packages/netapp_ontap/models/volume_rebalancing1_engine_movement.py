r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeRebalancing1EngineMovement", "VolumeRebalancing1EngineMovementSchema"]
__pdoc__ = {
    "VolumeRebalancing1EngineMovementSchema.resource": False,
    "VolumeRebalancing1EngineMovementSchema.opts": False,
    "VolumeRebalancing1EngineMovement": False,
}


class VolumeRebalancing1EngineMovementSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeRebalancing1EngineMovement object"""

    file_moves_started = Size(data_key="file_moves_started")
    r""" Number of file moves started on this constituent. """

    last_error = fields.Nested("netapp_ontap.models.volume_rebalancing1_engine_movement_last_error.VolumeRebalancing1EngineMovementLastErrorSchema", unknown=EXCLUDE, data_key="last_error")
    r""" The last_error field of the volume_rebalancing1_engine_movement. """

    most_recent_start_time = ImpreciseDateTime(data_key="most_recent_start_time")
    r""" Start time of the most recent file move on the constiutent.

Example: 2018-06-04T19:00:00.000+0000 """

    @property
    def resource(self):
        return VolumeRebalancing1EngineMovement

    gettable_fields = [
        "file_moves_started",
        "last_error",
        "most_recent_start_time",
    ]
    """file_moves_started,last_error,most_recent_start_time,"""

    patchable_fields = [
        "last_error",
    ]
    """last_error,"""

    postable_fields = [
        "last_error",
    ]
    """last_error,"""


class VolumeRebalancing1EngineMovement(Resource):

    _schema = VolumeRebalancing1EngineMovementSchema
