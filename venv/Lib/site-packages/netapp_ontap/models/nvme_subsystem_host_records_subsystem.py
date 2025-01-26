r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemHostRecordsSubsystem", "NvmeSubsystemHostRecordsSubsystemSchema"]
__pdoc__ = {
    "NvmeSubsystemHostRecordsSubsystemSchema.resource": False,
    "NvmeSubsystemHostRecordsSubsystemSchema.opts": False,
    "NvmeSubsystemHostRecordsSubsystem": False,
}


class NvmeSubsystemHostRecordsSubsystemSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemHostRecordsSubsystem object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the nvme_subsystem_host_records_subsystem. """

    name = fields.Str(data_key="name")
    r""" The name of the NVMe subsystem.


Example: subsystem1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the NVMe subsystem.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return NvmeSubsystemHostRecordsSubsystem

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class NvmeSubsystemHostRecordsSubsystem(Resource):

    _schema = NvmeSubsystemHostRecordsSubsystemSchema
