r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SecurityAssociationResponseRecords", "SecurityAssociationResponseRecordsSchema"]
__pdoc__ = {
    "SecurityAssociationResponseRecordsSchema.resource": False,
    "SecurityAssociationResponseRecordsSchema.opts": False,
    "SecurityAssociationResponseRecords": False,
}


class SecurityAssociationResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SecurityAssociationResponseRecords object"""

    cipher_suite = fields.Str(data_key="cipher_suite")
    r""" Cipher suite for the security association.

Valid choices:

* suite_aescbc
* suiteb_gcm256
* suiteb_gmac256 """

    ike = fields.Nested("netapp_ontap.models.security_association_ike.SecurityAssociationIkeSchema", unknown=EXCLUDE, data_key="ike")
    r""" The ike field of the security_association_response_records. """

    ipsec = fields.Nested("netapp_ontap.models.security_association_ipsec.SecurityAssociationIpsecSchema", unknown=EXCLUDE, data_key="ipsec")
    r""" The ipsec field of the security_association_response_records. """

    lifetime = Size(data_key="lifetime")
    r""" Lifetime for the security association in seconds. """

    local_address = fields.Str(data_key="local_address")
    r""" Local address of the security association. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the security_association_response_records. """

    policy_name = fields.Str(data_key="policy_name")
    r""" Policy name for the security association. """

    remote_address = fields.Str(data_key="remote_address")
    r""" Remote address of the security association. """

    scope = fields.Str(data_key="scope")
    r""" The scope field of the security_association_response_records. """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the security_association_response_records. """

    type = fields.Str(data_key="type")
    r""" Type of security association, it can be IPsec or IKE (Internet Key Exchange).

Valid choices:

* ipsec
* ike """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier of the security association. """

    @property
    def resource(self):
        return SecurityAssociationResponseRecords

    gettable_fields = [
        "cipher_suite",
        "ike",
        "ipsec",
        "lifetime",
        "local_address",
        "node.links",
        "node.name",
        "node.uuid",
        "policy_name",
        "remote_address",
        "scope",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "type",
        "uuid",
    ]
    """cipher_suite,ike,ipsec,lifetime,local_address,node.links,node.name,node.uuid,policy_name,remote_address,scope,svm.links,svm.name,svm.uuid,type,uuid,"""

    patchable_fields = [
        "cipher_suite",
        "ike",
        "ipsec",
        "lifetime",
        "local_address",
        "node.name",
        "node.uuid",
        "policy_name",
        "remote_address",
        "scope",
        "svm.name",
        "svm.uuid",
        "type",
        "uuid",
    ]
    """cipher_suite,ike,ipsec,lifetime,local_address,node.name,node.uuid,policy_name,remote_address,scope,svm.name,svm.uuid,type,uuid,"""

    postable_fields = [
        "cipher_suite",
        "ike",
        "ipsec",
        "lifetime",
        "local_address",
        "node.name",
        "node.uuid",
        "policy_name",
        "remote_address",
        "scope",
        "svm.name",
        "svm.uuid",
        "type",
        "uuid",
    ]
    """cipher_suite,ike,ipsec,lifetime,local_address,node.name,node.uuid,policy_name,remote_address,scope,svm.name,svm.uuid,type,uuid,"""


class SecurityAssociationResponseRecords(Resource):

    _schema = SecurityAssociationResponseRecordsSchema
