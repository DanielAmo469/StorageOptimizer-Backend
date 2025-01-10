r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["DetailedStatusCodeMessage", "DetailedStatusCodeMessageSchema"]
__pdoc__ = {
    "DetailedStatusCodeMessageSchema.resource": False,
    "DetailedStatusCodeMessageSchema.opts": False,
    "DetailedStatusCodeMessage": False,
}


class DetailedStatusCodeMessageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the DetailedStatusCodeMessage object"""

    code = fields.Str(data_key="code")
    r""" Code corresponding to the import status failure.


Example: 6684732 """

    message = fields.Str(data_key="message")
    r""" Detailed description of the import status. """

    @property
    def resource(self):
        return DetailedStatusCodeMessage

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class DetailedStatusCodeMessage(Resource):

    _schema = DetailedStatusCodeMessageSchema
