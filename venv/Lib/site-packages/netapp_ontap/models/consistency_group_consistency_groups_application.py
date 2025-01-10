r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsApplication", "ConsistencyGroupConsistencyGroupsApplicationSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsApplicationSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsApplicationSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsApplication": False,
}


class ConsistencyGroupConsistencyGroupsApplicationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsApplication object"""

    component_type = fields.Str(data_key="component_type")
    r""" Nested consistency group tag.

Valid choices:

* data
* logs
* other """

    type = fields.Str(data_key="type")
    r""" Top level consistency group tag.

Valid choices:

* oracle
* other
* exchange
* sql_server
* sap_hana
* vmware
* mongodb
* db2
* mysql
* sap_maxdb
* postgresql
* sap_ase_sybase """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsApplication

    gettable_fields = [
        "component_type",
        "type",
    ]
    """component_type,type,"""

    patchable_fields = [
        "component_type",
        "type",
    ]
    """component_type,type,"""

    postable_fields = [
        "component_type",
        "type",
    ]
    """component_type,type,"""


class ConsistencyGroupConsistencyGroupsApplication(Resource):

    _schema = ConsistencyGroupConsistencyGroupsApplicationSchema
