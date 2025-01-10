r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemSubsystemMaps", "NvmeSubsystemSubsystemMapsSchema"]
__pdoc__ = {
    "NvmeSubsystemSubsystemMapsSchema.resource": False,
    "NvmeSubsystemSubsystemMapsSchema.opts": False,
    "NvmeSubsystemSubsystemMaps": False,
}


class NvmeSubsystemSubsystemMapsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemSubsystemMaps object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the nvme_subsystem_subsystem_maps. """

    anagrpid = fields.Str(data_key="anagrpid")
    r""" The Asymmetric Namespace Access Group ID (ANAGRPID) of the NVMe namespace.<br/>
The format for an ANAGRPIP is 8 hexadecimal digits (zero-filled) followed by a lower case "h".


Example: 00103050h """

    namespace = fields.Nested("netapp_ontap.models.nvme_subsystem_subsystem_maps_namespace.NvmeSubsystemSubsystemMapsNamespaceSchema", unknown=EXCLUDE, data_key="namespace")
    r""" The namespace field of the nvme_subsystem_subsystem_maps. """

    nsid = fields.Str(data_key="nsid")
    r""" The NVMe namespace identifier. This is an identifier used by an NVMe controller to provide access to the NVMe namespace.<br/>
The format for an NVMe namespace identifier is 8 hexadecimal digits (zero-filled) followed by a lower case "h".


Example: 00000001h """

    @property
    def resource(self):
        return NvmeSubsystemSubsystemMaps

    gettable_fields = [
        "links",
        "anagrpid",
        "namespace",
        "nsid",
    ]
    """links,anagrpid,namespace,nsid,"""

    patchable_fields = [
        "namespace",
    ]
    """namespace,"""

    postable_fields = [
        "namespace",
    ]
    """namespace,"""


class NvmeSubsystemSubsystemMaps(Resource):

    _schema = NvmeSubsystemSubsystemMapsSchema
