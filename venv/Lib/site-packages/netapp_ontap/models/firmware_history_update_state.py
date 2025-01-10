r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FirmwareHistoryUpdateState", "FirmwareHistoryUpdateStateSchema"]
__pdoc__ = {
    "FirmwareHistoryUpdateStateSchema.resource": False,
    "FirmwareHistoryUpdateStateSchema.opts": False,
    "FirmwareHistoryUpdateState": False,
}


class FirmwareHistoryUpdateStateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FirmwareHistoryUpdateState object"""

    worker = fields.Nested("netapp_ontap.models.firmware_history_update_state_worker.FirmwareHistoryUpdateStateWorkerSchema", unknown=EXCLUDE, data_key="worker")
    r""" The worker field of the firmware_history_update_state. """

    @property
    def resource(self):
        return FirmwareHistoryUpdateState

    gettable_fields = [
        "worker",
    ]
    """worker,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FirmwareHistoryUpdateState(Resource):

    _schema = FirmwareHistoryUpdateStateSchema
