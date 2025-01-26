r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageService", "ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageServiceSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageServiceSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageServiceSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageService": False,
}


class ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageService object"""

    name = fields.Str(data_key="name")
    r""" Storage service name. If not specified, the default value is the most performant for the platform.


Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageService

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageService(Resource):

    _schema = ConsistencyGroupConsistencyGroupsProvisioningOptionsStorageServiceSchema
