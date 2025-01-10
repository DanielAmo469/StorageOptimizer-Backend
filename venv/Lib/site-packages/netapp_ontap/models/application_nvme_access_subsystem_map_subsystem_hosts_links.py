r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationNvmeAccessSubsystemMapSubsystemHostsLinks", "ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema"]
__pdoc__ = {
    "ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema.resource": False,
    "ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema.opts": False,
    "ApplicationNvmeAccessSubsystemMapSubsystemHostsLinks": False,
}


class ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationNvmeAccessSubsystemMapSubsystemHostsLinks object"""

    self_ = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="self")
    r""" The self_ field of the application_nvme_access_subsystem_map_subsystem_hosts_links. """

    @property
    def resource(self):
        return ApplicationNvmeAccessSubsystemMapSubsystemHostsLinks

    gettable_fields = [
        "self_",
    ]
    """self_,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationNvmeAccessSubsystemMapSubsystemHostsLinks(Resource):

    _schema = ApplicationNvmeAccessSubsystemMapSubsystemHostsLinksSchema
