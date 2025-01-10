r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AwsConnectivity", "AwsConnectivitySchema"]
__pdoc__ = {
    "AwsConnectivitySchema.resource": False,
    "AwsConnectivitySchema.opts": False,
    "AwsConnectivity": False,
}


class AwsConnectivitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AwsConnectivity object"""

    code = fields.Str(data_key="code")
    r""" Code corresponding to the error message. Returns a 0 if Amazon KMS is reachable from all nodes in the cluster.

Example: 346758 """

    message = fields.Str(data_key="message")
    r""" Error message returned when 'reachable' is false.

Example: Amazon KMS is not reachable from all nodes - <reason>. """

    reachable = fields.Boolean(data_key="reachable")
    r""" Set to true if the Amazon KMS is reachable from all nodes of the cluster. """

    @property
    def resource(self):
        return AwsConnectivity

    gettable_fields = [
        "code",
        "message",
        "reachable",
    ]
    """code,message,reachable,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AwsConnectivity(Resource):

    _schema = AwsConnectivitySchema
