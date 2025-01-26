r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SoftwareValidationReferenceAction", "SoftwareValidationReferenceActionSchema"]
__pdoc__ = {
    "SoftwareValidationReferenceActionSchema.resource": False,
    "SoftwareValidationReferenceActionSchema.opts": False,
    "SoftwareValidationReferenceAction": False,
}


class SoftwareValidationReferenceActionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SoftwareValidationReferenceAction object"""

    message = fields.Str(data_key="message")
    r""" Specifies the corrective action to take to resolve an error.

Example: Use NFS hard mounts, if possible. """

    @property
    def resource(self):
        return SoftwareValidationReferenceAction

    gettable_fields = [
        "message",
    ]
    """message,"""

    patchable_fields = [
        "message",
    ]
    """message,"""

    postable_fields = [
        "message",
    ]
    """message,"""


class SoftwareValidationReferenceAction(Resource):

    _schema = SoftwareValidationReferenceActionSchema
