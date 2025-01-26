r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterDiagCheck", "MetroclusterDiagCheckSchema"]
__pdoc__ = {
    "MetroclusterDiagCheckSchema.resource": False,
    "MetroclusterDiagCheckSchema.opts": False,
    "MetroclusterDiagCheck": False,
}


class MetroclusterDiagCheckSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterDiagCheck object"""

    additional_info = fields.Nested("netapp_ontap.models.error_arguments.ErrorArgumentsSchema", unknown=EXCLUDE, data_key="additional_info")
    r""" The additional_info field of the metrocluster_diag_check. """

    name = fields.Str(data_key="name")
    r""" Name of type of diagnostic operation run for the component.

Example: mirrror_status """

    result = fields.Str(data_key="result")
    r""" Result of the diagnostic operation on this component.

Valid choices:

* ok
* warning
* not_run
* not_applicable """

    @property
    def resource(self):
        return MetroclusterDiagCheck

    gettable_fields = [
        "additional_info",
        "name",
        "result",
    ]
    """additional_info,name,result,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class MetroclusterDiagCheck(Resource):

    _schema = MetroclusterDiagCheckSchema
