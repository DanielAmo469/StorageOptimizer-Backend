r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationNvmeAccessSubsystemMapSubsystem", "ApplicationNvmeAccessSubsystemMapSubsystemSchema"]
__pdoc__ = {
    "ApplicationNvmeAccessSubsystemMapSubsystemSchema.resource": False,
    "ApplicationNvmeAccessSubsystemMapSubsystemSchema.opts": False,
    "ApplicationNvmeAccessSubsystemMapSubsystem": False,
}


class ApplicationNvmeAccessSubsystemMapSubsystemSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationNvmeAccessSubsystemMapSubsystem object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the application_nvme_access_subsystem_map_subsystem. """

    hosts = fields.List(fields.Nested("netapp_ontap.models.application_nvme_access_subsystem_map_subsystem_hosts.ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema", unknown=EXCLUDE), data_key="hosts")
    r""" The hosts field of the application_nvme_access_subsystem_map_subsystem. """

    name = fields.Str(data_key="name")
    r""" Subsystem name """

    uuid = fields.Str(data_key="uuid")
    r""" Subsystem UUID """

    @property
    def resource(self):
        return ApplicationNvmeAccessSubsystemMapSubsystem

    gettable_fields = [
        "links",
        "hosts",
        "name",
        "uuid",
    ]
    """links,hosts,name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationNvmeAccessSubsystemMapSubsystem(Resource):

    _schema = ApplicationNvmeAccessSubsystemMapSubsystemSchema
