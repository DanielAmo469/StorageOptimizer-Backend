r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IkpConnectivity", "IkpConnectivitySchema"]
__pdoc__ = {
    "IkpConnectivitySchema.resource": False,
    "IkpConnectivitySchema.opts": False,
    "IkpConnectivity": False,
}


class IkpConnectivitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IkpConnectivity object"""

    code = fields.Str(data_key="code")
    r""" Code corresponding to the error message. Returns a 0 if IBM Key Protect KMS is reachable from all nodes in the cluster.

Example: 346758 """

    message = fields.Str(data_key="message")
    r""" Set to the error message when 'reachable' is false.

Example: IBM Key Protect KMS is not reachable from all nodes - <reason>. """

    reachable = fields.Boolean(data_key="reachable")
    r""" Set to true if the IBM Key Protect KMS is reachable from all nodes of the cluster. """

    @property
    def resource(self):
        return IkpConnectivity

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


class IkpConnectivity(Resource):

    _schema = IkpConnectivitySchema
