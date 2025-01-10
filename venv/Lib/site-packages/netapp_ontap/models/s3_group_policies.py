r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3GroupPolicies", "S3GroupPoliciesSchema"]
__pdoc__ = {
    "S3GroupPoliciesSchema.resource": False,
    "S3GroupPoliciesSchema.opts": False,
    "S3GroupPolicies": False,
}


class S3GroupPoliciesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3GroupPolicies object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_group_policies. """

    name = fields.Str(data_key="name")
    r""" Specifies the name of the policy. A policy name length can range from 1 to 128 characters and can only contain the following combination of characters 0-9, A-Z, a-z, "_", "+", "=", ",", ".","@", and "-".

Example: Policy1 """

    @property
    def resource(self):
        return S3GroupPolicies

    gettable_fields = [
        "links",
        "name",
    ]
    """links,name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class S3GroupPolicies(Resource):

    _schema = S3GroupPoliciesSchema
