r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SanApplicationComponentsTiering", "SanApplicationComponentsTieringSchema"]
__pdoc__ = {
    "SanApplicationComponentsTieringSchema.resource": False,
    "SanApplicationComponentsTieringSchema.opts": False,
    "SanApplicationComponentsTiering": False,
}


class SanApplicationComponentsTieringSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SanApplicationComponentsTiering object"""

    control = fields.Str(data_key="control")
    r""" Storage tiering placement rules for the container(s)

Valid choices:

* required
* best_effort
* disallowed """

    object_stores = fields.List(fields.Nested("netapp_ontap.models.nas_application_components_tiering_object_stores.NasApplicationComponentsTieringObjectStoresSchema", unknown=EXCLUDE), data_key="object_stores")
    r""" The object_stores field of the san_application_components_tiering. """

    policy = fields.Str(data_key="policy")
    r""" The storage tiering type of the application component.

Valid choices:

* all
* auto
* none
* snapshot_only """

    @property
    def resource(self):
        return SanApplicationComponentsTiering

    gettable_fields = [
        "policy",
    ]
    """policy,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "control",
        "object_stores",
        "policy",
    ]
    """control,object_stores,policy,"""


class SanApplicationComponentsTiering(Resource):

    _schema = SanApplicationComponentsTieringSchema
