r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemHostRecords", "NvmeSubsystemHostRecordsSchema"]
__pdoc__ = {
    "NvmeSubsystemHostRecordsSchema.resource": False,
    "NvmeSubsystemHostRecordsSchema.opts": False,
    "NvmeSubsystemHostRecords": False,
}


class NvmeSubsystemHostRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemHostRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the nvme_subsystem_host_records. """

    dh_hmac_chap = fields.Nested("netapp_ontap.models.nvme_dh_hmac_chap_authentication.NvmeDhHmacChapAuthenticationSchema", unknown=EXCLUDE, data_key="dh_hmac_chap")
    r""" The dh_hmac_chap field of the nvme_subsystem_host_records. """

    io_queue = fields.Nested("netapp_ontap.models.nvme_subsystem_host_records_io_queue.NvmeSubsystemHostRecordsIoQueueSchema", unknown=EXCLUDE, data_key="io_queue")
    r""" The io_queue field of the nvme_subsystem_host_records. """

    nqn = fields.Str(data_key="nqn")
    r""" The NVMe qualified name (NQN) used to identify the NVMe storage target. Not allowed in POST when the `records` property is used.


Example: nqn.1992-01.example.com:string """

    subsystem = fields.Nested("netapp_ontap.models.nvme_subsystem_host_records_subsystem.NvmeSubsystemHostRecordsSubsystemSchema", unknown=EXCLUDE, data_key="subsystem")
    r""" The subsystem field of the nvme_subsystem_host_records. """

    @property
    def resource(self):
        return NvmeSubsystemHostRecords

    gettable_fields = [
        "links",
        "dh_hmac_chap",
        "io_queue",
        "nqn",
        "subsystem",
    ]
    """links,dh_hmac_chap,io_queue,nqn,subsystem,"""

    patchable_fields = [
        "dh_hmac_chap",
        "io_queue",
        "subsystem",
    ]
    """dh_hmac_chap,io_queue,subsystem,"""

    postable_fields = [
        "dh_hmac_chap",
        "io_queue",
        "nqn",
        "subsystem",
    ]
    """dh_hmac_chap,io_queue,nqn,subsystem,"""


class NvmeSubsystemHostRecords(Resource):

    _schema = NvmeSubsystemHostRecordsSchema
