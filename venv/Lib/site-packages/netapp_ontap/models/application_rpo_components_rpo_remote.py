r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationRpoComponentsRpoRemote", "ApplicationRpoComponentsRpoRemoteSchema"]
__pdoc__ = {
    "ApplicationRpoComponentsRpoRemoteSchema.resource": False,
    "ApplicationRpoComponentsRpoRemoteSchema.opts": False,
    "ApplicationRpoComponentsRpoRemote": False,
}


class ApplicationRpoComponentsRpoRemoteSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationRpoComponentsRpoRemote object"""

    description = fields.Str(data_key="description")
    r""" A detailed description of the remote RPO. """

    name = fields.Str(data_key="name")
    r""" The remote RPO of the component. A remote RPO of zero indicates that the component is synchronously replicated to another cluster.

Valid choices:

* 6_hourly
* 15_minutely
* hourly
* none
* zero """

    @property
    def resource(self):
        return ApplicationRpoComponentsRpoRemote

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


class ApplicationRpoComponentsRpoRemote(Resource):

    _schema = ApplicationRpoComponentsRpoRemoteSchema
