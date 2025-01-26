r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FpolicyPoliciesScope", "FpolicyPoliciesScopeSchema"]
__pdoc__ = {
    "FpolicyPoliciesScopeSchema.resource": False,
    "FpolicyPoliciesScopeSchema.opts": False,
    "FpolicyPoliciesScope": False,
}


class FpolicyPoliciesScopeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyPoliciesScope object"""

    check_extensions_on_directories = fields.Boolean(data_key="check_extensions_on_directories")
    r""" Specifies whether the file name extension checks also apply to directory objects. If this parameter is set to true,
the directory objects are subjected to the same extension checks as regular files. If this parameter is set to false,
the directory names are not matched for extensions and notifications are sent for directories even if their name
extensions do not match. Default is false. """

    exclude_export_policies = fields.List(fields.Str, data_key="exclude_export_policies")
    r""" The exclude_export_policies field of the fpolicy_policies_scope. """

    exclude_extension = fields.List(fields.Str, data_key="exclude_extension")
    r""" The exclude_extension field of the fpolicy_policies_scope. """

    exclude_shares = fields.List(fields.Str, data_key="exclude_shares")
    r""" The exclude_shares field of the fpolicy_policies_scope. """

    exclude_volumes = fields.List(fields.Str, data_key="exclude_volumes")
    r""" The exclude_volumes field of the fpolicy_policies_scope.

Example: ["vol1","vol_svm1","*"] """

    include_export_policies = fields.List(fields.Str, data_key="include_export_policies")
    r""" The include_export_policies field of the fpolicy_policies_scope. """

    include_extension = fields.List(fields.Str, data_key="include_extension")
    r""" The include_extension field of the fpolicy_policies_scope. """

    include_shares = fields.List(fields.Str, data_key="include_shares")
    r""" The include_shares field of the fpolicy_policies_scope.

Example: ["sh1","share_cifs"] """

    include_volumes = fields.List(fields.Str, data_key="include_volumes")
    r""" The include_volumes field of the fpolicy_policies_scope.

Example: ["vol1","vol_svm1"] """

    object_monitoring_with_no_extension = fields.Boolean(data_key="object_monitoring_with_no_extension")
    r""" Specifies whether the extension checks also apply to objects with no extension. If this parameter is set to true,
all objects with or without extensions are monitored. Default is false. """

    @property
    def resource(self):
        return FpolicyPoliciesScope

    gettable_fields = [
        "check_extensions_on_directories",
        "exclude_export_policies",
        "exclude_extension",
        "exclude_shares",
        "exclude_volumes",
        "include_export_policies",
        "include_extension",
        "include_shares",
        "include_volumes",
        "object_monitoring_with_no_extension",
    ]
    """check_extensions_on_directories,exclude_export_policies,exclude_extension,exclude_shares,exclude_volumes,include_export_policies,include_extension,include_shares,include_volumes,object_monitoring_with_no_extension,"""

    patchable_fields = [
        "check_extensions_on_directories",
        "exclude_export_policies",
        "exclude_extension",
        "exclude_shares",
        "exclude_volumes",
        "include_export_policies",
        "include_extension",
        "include_shares",
        "include_volumes",
        "object_monitoring_with_no_extension",
    ]
    """check_extensions_on_directories,exclude_export_policies,exclude_extension,exclude_shares,exclude_volumes,include_export_policies,include_extension,include_shares,include_volumes,object_monitoring_with_no_extension,"""

    postable_fields = [
        "check_extensions_on_directories",
        "exclude_export_policies",
        "exclude_extension",
        "exclude_shares",
        "exclude_volumes",
        "include_export_policies",
        "include_extension",
        "include_shares",
        "include_volumes",
        "object_monitoring_with_no_extension",
    ]
    """check_extensions_on_directories,exclude_export_policies,exclude_extension,exclude_shares,exclude_volumes,include_export_policies,include_extension,include_shares,include_volumes,object_monitoring_with_no_extension,"""


class FpolicyPoliciesScope(Resource):

    _schema = FpolicyPoliciesScopeSchema
