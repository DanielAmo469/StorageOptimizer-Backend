r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IpsecPolicyResponseRecords", "IpsecPolicyResponseRecordsSchema"]
__pdoc__ = {
    "IpsecPolicyResponseRecordsSchema.resource": False,
    "IpsecPolicyResponseRecordsSchema.opts": False,
    "IpsecPolicyResponseRecords": False,
}


class IpsecPolicyResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IpsecPolicyResponseRecords object"""

    action = fields.Str(data_key="action")
    r""" Action for the IPsec policy.

Valid choices:

* bypass
* discard
* esp_transport
* esp_udp """

    authentication_method = fields.Str(data_key="authentication_method")
    r""" Authentication method for the IPsec policy.

Valid choices:

* none
* psk
* pki """

    certificate = fields.Nested("netapp_ontap.resources.security_certificate.SecurityCertificateSchema", unknown=EXCLUDE, data_key="certificate")
    r""" The certificate field of the ipsec_policy_response_records. """

    enabled = fields.Boolean(data_key="enabled")
    r""" Indicates whether or not the policy is enabled. """

    ipspace = fields.Nested("netapp_ontap.resources.ipspace.IpspaceSchema", unknown=EXCLUDE, data_key="ipspace")
    r""" The ipspace field of the ipsec_policy_response_records. """

    local_endpoint = fields.Nested("netapp_ontap.models.ipsec_endpoint.IpsecEndpointSchema", unknown=EXCLUDE, data_key="local_endpoint")
    r""" The local_endpoint field of the ipsec_policy_response_records. """

    local_identity = fields.Str(data_key="local_identity")
    r""" Local Identity """

    name = fields.Str(data_key="name")
    r""" IPsec policy name. """

    protocol = fields.Str(data_key="protocol")
    r""" Lower layer protocol to be covered by the IPsec policy.

Example: 17 """

    remote_endpoint = fields.Nested("netapp_ontap.models.ipsec_endpoint.IpsecEndpointSchema", unknown=EXCLUDE, data_key="remote_endpoint")
    r""" The remote_endpoint field of the ipsec_policy_response_records. """

    remote_identity = fields.Str(data_key="remote_identity")
    r""" Remote Identity """

    scope = fields.Str(data_key="scope")
    r""" The scope field of the ipsec_policy_response_records. """

    secret_key = fields.Str(data_key="secret_key")
    r""" Pre-shared key for IKE negotiation. """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the ipsec_policy_response_records. """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier of the IPsec policy.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return IpsecPolicyResponseRecords

    gettable_fields = [
        "authentication_method",
        "certificate.links",
        "certificate.name",
        "certificate.uuid",
        "enabled",
        "ipspace.links",
        "ipspace.name",
        "ipspace.uuid",
        "local_endpoint",
        "local_identity",
        "name",
        "protocol",
        "remote_endpoint",
        "remote_identity",
        "scope",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
    ]
    """authentication_method,certificate.links,certificate.name,certificate.uuid,enabled,ipspace.links,ipspace.name,ipspace.uuid,local_endpoint,local_identity,name,protocol,remote_endpoint,remote_identity,scope,svm.links,svm.name,svm.uuid,uuid,"""

    patchable_fields = [
        "authentication_method",
        "certificate.name",
        "certificate.uuid",
        "enabled",
        "ipspace.name",
        "ipspace.uuid",
        "local_endpoint",
        "local_identity",
        "name",
        "protocol",
        "remote_endpoint",
        "remote_identity",
        "scope",
    ]
    """authentication_method,certificate.name,certificate.uuid,enabled,ipspace.name,ipspace.uuid,local_endpoint,local_identity,name,protocol,remote_endpoint,remote_identity,scope,"""

    postable_fields = [
        "action",
        "authentication_method",
        "certificate.name",
        "certificate.uuid",
        "enabled",
        "ipspace.name",
        "ipspace.uuid",
        "local_endpoint",
        "local_identity",
        "name",
        "protocol",
        "remote_endpoint",
        "remote_identity",
        "scope",
        "secret_key",
        "svm.name",
        "svm.uuid",
    ]
    """action,authentication_method,certificate.name,certificate.uuid,enabled,ipspace.name,ipspace.uuid,local_endpoint,local_identity,name,protocol,remote_endpoint,remote_identity,scope,secret_key,svm.name,svm.uuid,"""


class IpsecPolicyResponseRecords(Resource):

    _schema = IpsecPolicyResponseRecordsSchema
