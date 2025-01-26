r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceCredentialCache", "NfsServiceCredentialCacheSchema"]
__pdoc__ = {
    "NfsServiceCredentialCacheSchema.resource": False,
    "NfsServiceCredentialCacheSchema.opts": False,
    "NfsServiceCredentialCache": False,
}


class NfsServiceCredentialCacheSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceCredentialCache object"""

    negative_ttl = Size(data_key="negative_ttl")
    r""" Specifies the age in milliseconds, of the negative cached credentials after which they are cleared from the cache.

Example: 7200000 """

    positive_ttl = Size(data_key="positive_ttl")
    r""" Specifies the age in milliseconds, of the positive cached credentials after which they are cleared from the cache.

Example: 7200000 """

    transient_error_ttl = Size(data_key="transient_error_ttl")
    r""" Specifies the age in milliseconds, of the cached entries during a transient error situation.

Example: 72000 """

    @property
    def resource(self):
        return NfsServiceCredentialCache

    gettable_fields = [
        "negative_ttl",
        "positive_ttl",
        "transient_error_ttl",
    ]
    """negative_ttl,positive_ttl,transient_error_ttl,"""

    patchable_fields = [
        "negative_ttl",
        "positive_ttl",
        "transient_error_ttl",
    ]
    """negative_ttl,positive_ttl,transient_error_ttl,"""

    postable_fields = [
        "negative_ttl",
        "positive_ttl",
        "transient_error_ttl",
    ]
    """negative_ttl,positive_ttl,transient_error_ttl,"""


class NfsServiceCredentialCache(Resource):

    _schema = NfsServiceCredentialCacheSchema
