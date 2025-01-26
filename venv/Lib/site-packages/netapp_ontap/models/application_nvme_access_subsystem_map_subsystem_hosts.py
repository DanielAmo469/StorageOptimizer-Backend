r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationNvmeAccessSubsystemMapSubsystemHosts", "ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema"]
__pdoc__ = {
    "ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema.resource": False,
    "ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema.opts": False,
    "ApplicationNvmeAccessSubsystemMapSubsystemHosts": False,
}


class ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationNvmeAccessSubsystemMapSubsystemHosts object"""

    links = fields.Nested("netapp_ontap.models.application_nvme_access_subsystem_map_subsystem_hosts_links.ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the application_nvme_access_subsystem_map_subsystem_hosts. """

    nqn = fields.Str(data_key="nqn")
    r""" Host """

    @property
    def resource(self):
        return ApplicationNvmeAccessSubsystemMapSubsystemHosts

    gettable_fields = [
        "links",
        "nqn",
    ]
    """links,nqn,"""

    patchable_fields = [
        "links",
    ]
    """links,"""

    postable_fields = [
        "links",
    ]
    """links,"""


class ApplicationNvmeAccessSubsystemMapSubsystemHosts(Resource):

    _schema = ApplicationNvmeAccessSubsystemMapSubsystemHostsSchema
