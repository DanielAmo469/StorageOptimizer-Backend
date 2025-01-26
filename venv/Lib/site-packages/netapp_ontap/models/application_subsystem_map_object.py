r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationSubsystemMapObject", "ApplicationSubsystemMapObjectSchema"]
__pdoc__ = {
    "ApplicationSubsystemMapObjectSchema.resource": False,
    "ApplicationSubsystemMapObjectSchema.opts": False,
    "ApplicationSubsystemMapObject": False,
}


class ApplicationSubsystemMapObjectSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationSubsystemMapObject object"""

    anagrpid = fields.Str(data_key="anagrpid")
    r""" Subsystem ANA group ID """

    nsid = fields.Str(data_key="nsid")
    r""" Subsystem namespace ID """

    subsystem = fields.Nested("netapp_ontap.models.application_nvme_access_subsystem_map_subsystem.ApplicationNvmeAccessSubsystemMapSubsystemSchema", unknown=EXCLUDE, data_key="subsystem")
    r""" The subsystem field of the application_subsystem_map_object. """

    @property
    def resource(self):
        return ApplicationSubsystemMapObject

    gettable_fields = [
        "anagrpid",
        "nsid",
        "subsystem",
    ]
    """anagrpid,nsid,subsystem,"""

    patchable_fields = [
        "subsystem",
    ]
    """subsystem,"""

    postable_fields = [
        "subsystem",
    ]
    """subsystem,"""


class ApplicationSubsystemMapObject(Resource):

    _schema = ApplicationSubsystemMapObjectSchema
