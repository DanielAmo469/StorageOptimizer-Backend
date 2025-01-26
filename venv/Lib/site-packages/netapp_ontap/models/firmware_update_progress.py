r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FirmwareUpdateProgress", "FirmwareUpdateProgressSchema"]
__pdoc__ = {
    "FirmwareUpdateProgressSchema.resource": False,
    "FirmwareUpdateProgressSchema.opts": False,
    "FirmwareUpdateProgress": False,
}


class FirmwareUpdateProgressSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FirmwareUpdateProgress object"""

    job = fields.Nested("netapp_ontap.models.job_link.JobLinkSchema", unknown=EXCLUDE, data_key="job")
    r""" The job field of the firmware_update_progress. """

    update_state = fields.List(fields.Nested("netapp_ontap.models.firmware_update_progress_state.FirmwareUpdateProgressStateSchema", unknown=EXCLUDE), data_key="update_state")
    r""" The update_state field of the firmware_update_progress. """

    update_type = fields.Str(data_key="update_type")
    r""" Specifies the type of update.

Valid choices:

* manual_update
* automatic_update """

    zip_file_name = fields.Str(data_key="zip_file_name")
    r""" The zip_file_name field of the firmware_update_progress.

Example: disk_firmware.zip """

    @property
    def resource(self):
        return FirmwareUpdateProgress

    gettable_fields = [
        "job",
        "update_state",
        "update_type",
        "zip_file_name",
    ]
    """job,update_state,update_type,zip_file_name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FirmwareUpdateProgress(Resource):

    _schema = FirmwareUpdateProgressSchema
