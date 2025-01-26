r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationRpoComponentsRpoLocal", "ApplicationRpoComponentsRpoLocalSchema"]
__pdoc__ = {
    "ApplicationRpoComponentsRpoLocalSchema.resource": False,
    "ApplicationRpoComponentsRpoLocalSchema.opts": False,
    "ApplicationRpoComponentsRpoLocal": False,
}


class ApplicationRpoComponentsRpoLocalSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationRpoComponentsRpoLocal object"""

    description = fields.Str(data_key="description")
    r""" A detailed description of the local RPO. This will include details about the Snapshot copy schedule. """

    name = fields.Str(data_key="name")
    r""" The local RPO of the component. This indicates how often component Snapshot copies are automatically created.

Valid choices:

* 6_hourly
* 15_minutely
* hourly
* none """

    @property
    def resource(self):
        return ApplicationRpoComponentsRpoLocal

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


class ApplicationRpoComponentsRpoLocal(Resource):

    _schema = ApplicationRpoComponentsRpoLocalSchema
