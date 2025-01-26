r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AccountApplication", "AccountApplicationSchema"]
__pdoc__ = {
    "AccountApplicationSchema.resource": False,
    "AccountApplicationSchema.opts": False,
    "AccountApplication": False,
}


class AccountApplicationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AccountApplication object"""

    application = fields.Str(data_key="application")
    r""" Applications

Valid choices:

* amqp
* console
* http
* ontapi
* service_processor
* ssh """

    authentication_methods = fields.List(fields.Str, data_key="authentication_methods")
    r""" The authentication_methods field of the account_application. """

    second_authentication_method = fields.Str(data_key="second_authentication_method")
    r""" An optional additional authentication method for multifactor authentication (MFA). This is only supported with SSH as the application. Time-based One-Time Passwords (TOTPs) are only supported with the authentication method password or public key. It is ignored for all other applications.

Valid choices:

* none
* password
* publickey
* nsswitch
* domain
* totp """

    @property
    def resource(self):
        return AccountApplication

    gettable_fields = [
        "application",
        "authentication_methods",
        "second_authentication_method",
    ]
    """application,authentication_methods,second_authentication_method,"""

    patchable_fields = [
        "application",
        "authentication_methods",
        "second_authentication_method",
    ]
    """application,authentication_methods,second_authentication_method,"""

    postable_fields = [
        "application",
        "authentication_methods",
        "second_authentication_method",
    ]
    """application,authentication_methods,second_authentication_method,"""


class AccountApplication(Resource):

    _schema = AccountApplicationSchema
