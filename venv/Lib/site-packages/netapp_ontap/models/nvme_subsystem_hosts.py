r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemHosts", "NvmeSubsystemHostsSchema"]
__pdoc__ = {
    "NvmeSubsystemHostsSchema.resource": False,
    "NvmeSubsystemHostsSchema.opts": False,
    "NvmeSubsystemHosts": False,
}


class NvmeSubsystemHostsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemHosts object"""

    dh_hmac_chap = fields.Nested("netapp_ontap.models.nvme_dh_hmac_chap_authentication.NvmeDhHmacChapAuthenticationSchema", unknown=EXCLUDE, data_key="dh_hmac_chap")
    r""" The dh_hmac_chap field of the nvme_subsystem_hosts. """

    nqn = fields.Str(data_key="nqn")
    r""" The NVMe qualified name (NQN) used to identify the NVMe storage target.


Example: nqn.1992-01.example.com:string """

    @property
    def resource(self):
        return NvmeSubsystemHosts

    gettable_fields = [
        "dh_hmac_chap",
        "nqn",
    ]
    """dh_hmac_chap,nqn,"""

    patchable_fields = [
        "dh_hmac_chap",
    ]
    """dh_hmac_chap,"""

    postable_fields = [
        "dh_hmac_chap",
        "nqn",
    ]
    """dh_hmac_chap,nqn,"""


class NvmeSubsystemHosts(Resource):

    _schema = NvmeSubsystemHostsSchema
