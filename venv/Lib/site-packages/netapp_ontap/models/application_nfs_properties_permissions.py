r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationNfsPropertiesPermissions", "ApplicationNfsPropertiesPermissionsSchema"]
__pdoc__ = {
    "ApplicationNfsPropertiesPermissionsSchema.resource": False,
    "ApplicationNfsPropertiesPermissionsSchema.opts": False,
    "ApplicationNfsPropertiesPermissions": False,
}


class ApplicationNfsPropertiesPermissionsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationNfsPropertiesPermissions object"""

    access = fields.Str(data_key="access")
    r""" Access granted to the host """

    host = fields.Str(data_key="host")
    r""" Host granted access """

    @property
    def resource(self):
        return ApplicationNfsPropertiesPermissions

    gettable_fields = [
        "access",
        "host",
    ]
    """access,host,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationNfsPropertiesPermissions(Resource):

    _schema = ApplicationNfsPropertiesPermissionsSchema
