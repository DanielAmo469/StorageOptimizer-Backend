r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplyTo", "ApplyToSchema"]
__pdoc__ = {
    "ApplyToSchema.resource": False,
    "ApplyToSchema.opts": False,
    "ApplyTo": False,
}


class ApplyToSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplyTo object"""

    files = fields.Boolean(data_key="files")
    r""" Apply to Files """

    sub_folders = fields.Boolean(data_key="sub_folders")
    r""" Apply to all sub-folders """

    this_folder = fields.Boolean(data_key="this_folder")
    r""" Apply only to this folder """

    @property
    def resource(self):
        return ApplyTo

    gettable_fields = [
        "files",
        "sub_folders",
        "this_folder",
    ]
    """files,sub_folders,this_folder,"""

    patchable_fields = [
        "files",
        "sub_folders",
        "this_folder",
    ]
    """files,sub_folders,this_folder,"""

    postable_fields = [
        "files",
        "sub_folders",
        "this_folder",
    ]
    """files,sub_folders,this_folder,"""


class ApplyTo(Resource):

    _schema = ApplyToSchema
