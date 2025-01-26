r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationProtectionGroupsRpoLocal", "ApplicationProtectionGroupsRpoLocalSchema"]
__pdoc__ = {
    "ApplicationProtectionGroupsRpoLocalSchema.resource": False,
    "ApplicationProtectionGroupsRpoLocalSchema.opts": False,
    "ApplicationProtectionGroupsRpoLocal": False,
}


class ApplicationProtectionGroupsRpoLocalSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationProtectionGroupsRpoLocal object"""

    description = fields.Str(data_key="description")
    r""" A detailed description of the local RPO. This includes details on the Snapshot copy schedule. """

    name = fields.Str(data_key="name")
    r""" The local RPO of the component. This indicates how often component Snapshot copies are automatically created.

Valid choices:

* none
* hourly
* 6_hourly
* 15_minutely """

    @property
    def resource(self):
        return ApplicationProtectionGroupsRpoLocal

    gettable_fields = [
        "description",
        "name",
    ]
    """description,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationProtectionGroupsRpoLocal(Resource):

    _schema = ApplicationProtectionGroupsRpoLocalSchema
