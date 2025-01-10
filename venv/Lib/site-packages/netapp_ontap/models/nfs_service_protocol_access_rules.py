r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceProtocolAccessRules", "NfsServiceProtocolAccessRulesSchema"]
__pdoc__ = {
    "NfsServiceProtocolAccessRulesSchema.resource": False,
    "NfsServiceProtocolAccessRulesSchema.opts": False,
    "NfsServiceProtocolAccessRules": False,
}


class NfsServiceProtocolAccessRulesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceProtocolAccessRules object"""

    cifs_access_type = fields.Str(data_key="cifs_access_type")
    r""" Access available for the CIFS protocol.

Valid choices:

* read
* read_write
* denied """

    nfs3_access_type = fields.Str(data_key="nfs3_access_type")
    r""" Access available for the NFSv3 protocol.

Valid choices:

* read
* read_write
* denied """

    nfs4_access_type = fields.Str(data_key="nfs4_access_type")
    r""" Access available for the NFSv4 protocol.

Valid choices:

* read
* read_write
* denied """

    @property
    def resource(self):
        return NfsServiceProtocolAccessRules

    gettable_fields = [
        "cifs_access_type",
        "nfs3_access_type",
        "nfs4_access_type",
    ]
    """cifs_access_type,nfs3_access_type,nfs4_access_type,"""

    patchable_fields = [
        "cifs_access_type",
        "nfs3_access_type",
        "nfs4_access_type",
    ]
    """cifs_access_type,nfs3_access_type,nfs4_access_type,"""

    postable_fields = [
        "cifs_access_type",
        "nfs3_access_type",
        "nfs4_access_type",
    ]
    """cifs_access_type,nfs3_access_type,nfs4_access_type,"""


class NfsServiceProtocolAccessRules(Resource):

    _schema = NfsServiceProtocolAccessRulesSchema
