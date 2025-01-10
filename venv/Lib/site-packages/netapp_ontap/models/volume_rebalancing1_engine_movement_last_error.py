r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeRebalancing1EngineMovementLastError", "VolumeRebalancing1EngineMovementLastErrorSchema"]
__pdoc__ = {
    "VolumeRebalancing1EngineMovementLastErrorSchema.resource": False,
    "VolumeRebalancing1EngineMovementLastErrorSchema.opts": False,
    "VolumeRebalancing1EngineMovementLastError": False,
}


class VolumeRebalancing1EngineMovementLastErrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeRebalancing1EngineMovementLastError object"""

    code = Size(data_key="code")
    r""" Error code of the last file move error on the constiutent. """

    destination = Size(data_key="destination")
    r""" DSID of the destination constituent of the last file move error on the constiutent. """

    file_id = Size(data_key="file_id")
    r""" File ID of the last file move error on the constiutent. """

    time = ImpreciseDateTime(data_key="time")
    r""" Time of the last file move error on the constiutent.

Example: 2018-06-04T19:00:00.000+0000 """

    @property
    def resource(self):
        return VolumeRebalancing1EngineMovementLastError

    gettable_fields = [
        "code",
        "destination",
        "file_id",
        "time",
    ]
    """code,destination,file_id,time,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeRebalancing1EngineMovementLastError(Resource):

    _schema = VolumeRebalancing1EngineMovementLastErrorSchema
