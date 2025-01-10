r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TotpPost", "TotpPostSchema"]
__pdoc__ = {
    "TotpPostSchema.resource": False,
    "TotpPostSchema.opts": False,
    "TotpPost": False,
}


class TotpPostSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TotpPost object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the totp_post. """

    account = fields.Nested("netapp_ontap.resources.account.AccountSchema", unknown=EXCLUDE, data_key="account")
    r""" The account field of the totp_post. """

    comment = fields.Str(data_key="comment")
    r""" Optional comment for the TOTP profile. """

    emergency_codes = fields.List(fields.Str, data_key="emergency_codes")
    r""" TOTP profile emergency codes for a user. These codes are for emergency use when a user cannot access 2FA codes through other means.

Example: "17503785" """

    enabled = fields.Boolean(data_key="enabled")
    r""" Status of the TOTP profile.

Example: true """

    install_url = fields.Str(data_key="install_url")
    r""" TOTP profile installation URL for a user. """

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="owner")
    r""" The owner field of the totp_post. """

    scope = fields.Str(data_key="scope")
    r""" Scope of the entity. Set to "cluster" for cluster owned objects and to "svm" for SVM owned objects.

Valid choices:

* cluster
* svm """

    secret_key = fields.Str(data_key="secret_key")
    r""" TOTP profile secret key for a user. """

    sha_fingerprint = fields.Str(data_key="sha_fingerprint")
    r""" SHA fingerprint for the TOTP secret key. """

    verification_code = fields.Str(data_key="verification_code")
    r""" TOTP profile verification code for a user. """

    @property
    def resource(self):
        return TotpPost

    gettable_fields = [
        "links",
        "account.links",
        "account.name",
        "comment",
        "emergency_codes",
        "enabled",
        "install_url",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "scope",
        "secret_key",
        "sha_fingerprint",
        "verification_code",
    ]
    """links,account.links,account.name,comment,emergency_codes,enabled,install_url,owner.links,owner.name,owner.uuid,scope,secret_key,sha_fingerprint,verification_code,"""

    patchable_fields = [
        "account.links",
        "account.name",
        "comment",
        "enabled",
    ]
    """account.links,account.name,comment,enabled,"""

    postable_fields = [
        "account.links",
        "account.name",
        "comment",
        "owner.name",
        "owner.uuid",
    ]
    """account.links,account.name,comment,owner.name,owner.uuid,"""


class TotpPost(Resource):

    _schema = TotpPostSchema
