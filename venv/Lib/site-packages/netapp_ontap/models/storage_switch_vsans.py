r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchVsans", "StorageSwitchVsansSchema"]
__pdoc__ = {
    "StorageSwitchVsansSchema.resource": False,
    "StorageSwitchVsansSchema.opts": False,
    "StorageSwitchVsans": False,
}


class StorageSwitchVsansSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchVsans object"""

    id = Size(data_key="id")
    r""" Storage switch VSAN ID """

    iod = fields.Boolean(data_key="iod")
    r""" Indicates whether in-order delivery is set for a zone. """

    load_balancing_types = fields.Str(data_key="load_balancing_types")
    r""" Storage switch VSAN load balancing type """

    name = fields.Str(data_key="name")
    r""" Storage switch VSAN name """

    state = fields.Str(data_key="state")
    r""" Storage switch VSAN Port state

Valid choices:

* ok
* error """

    @property
    def resource(self):
        return StorageSwitchVsans

    gettable_fields = [
        "id",
        "iod",
        "load_balancing_types",
        "name",
        "state",
    ]
    """id,iod,load_balancing_types,name,state,"""

    patchable_fields = [
        "id",
        "iod",
        "load_balancing_types",
        "name",
        "state",
    ]
    """id,iod,load_balancing_types,name,state,"""

    postable_fields = [
        "id",
        "iod",
        "load_balancing_types",
        "name",
        "state",
    ]
    """id,iod,load_balancing_types,name,state,"""


class StorageSwitchVsans(Resource):

    _schema = StorageSwitchVsansSchema
