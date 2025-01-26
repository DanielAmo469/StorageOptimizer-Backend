r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasExcludeAggregates", "NasExcludeAggregatesSchema"]
__pdoc__ = {
    "NasExcludeAggregatesSchema.resource": False,
    "NasExcludeAggregatesSchema.opts": False,
    "NasExcludeAggregates": False,
}


class NasExcludeAggregatesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasExcludeAggregates object"""

    name = fields.Str(data_key="name")
    r""" The name of the aggregate to exclude. Usage: &lt;aggregate name&gt; """

    uuid = fields.Str(data_key="uuid")
    r""" The ID of the aggregate to exclude. Usage: &lt;UUID&gt; """

    @property
    def resource(self):
        return NasExcludeAggregates

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class NasExcludeAggregates(Resource):

    _schema = NasExcludeAggregatesSchema
