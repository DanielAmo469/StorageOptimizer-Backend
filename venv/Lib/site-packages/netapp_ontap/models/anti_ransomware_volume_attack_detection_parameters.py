r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeAttackDetectionParameters", "AntiRansomwareVolumeAttackDetectionParametersSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeAttackDetectionParametersSchema.resource": False,
    "AntiRansomwareVolumeAttackDetectionParametersSchema.opts": False,
    "AntiRansomwareVolumeAttackDetectionParameters": False,
}


class AntiRansomwareVolumeAttackDetectionParametersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeAttackDetectionParameters object"""

    based_on_file_create_op_rate = fields.Boolean(data_key="based_on_file_create_op_rate")
    r""" Specifies whether attack detection is based on the file create operations rate. This parameter is valid only for NAS volumes. """

    based_on_file_delete_op_rate = fields.Boolean(data_key="based_on_file_delete_op_rate")
    r""" Specifies whether attack detection is based on the file delete operations rate. This parameter is valid only for NAS volumes. """

    based_on_file_rename_op_rate = fields.Boolean(data_key="based_on_file_rename_op_rate")
    r""" Specifies whether attack detection is based on the file rename operations rate. This parameter is valid only for NAS volumes. """

    based_on_high_entropy_data_rate = fields.Boolean(data_key="based_on_high_entropy_data_rate")
    r""" Specifies whether a high entropy data rate should be considered for attack detection. """

    based_on_never_seen_before_file_extension = fields.Boolean(data_key="based_on_never_seen_before_file_extension")
    r""" Specifies whether file extensions never seen before should be considered for attack detection. """

    file_create_op_rate_surge_notify_percent = Size(data_key="file_create_op_rate_surge_notify_percent")
    r""" Specifies the percentage of surge in the file create rate up to which it is considered normal behavior.

Example: 100 """

    file_delete_op_rate_surge_notify_percent = Size(data_key="file_delete_op_rate_surge_notify_percent")
    r""" Specifies the percentage of surge in the file delete rate up to which it is considered normal behavior. """

    file_rename_op_rate_surge_notify_percent = Size(data_key="file_rename_op_rate_surge_notify_percent")
    r""" Specifies the percent of surge in the file rename rate up to which it is considered normal behavior. """

    high_entropy_data_surge_notify_percent = Size(data_key="high_entropy_data_surge_notify_percent")
    r""" Specifies the percentage of surge in high entropy data up to which it is considered as normal behavior. For example, if the usual high entropy data rate in the volume is 5% and if this parameter is set to 100%, it will be considered as an unusual surge if the high entropy data rate of the volume exceeds 10% at any time. Similarly, if this parameter is set to 400%, it will be considered as an unusual surge if the high entropy data rate of the volume exceeds 25%, and so on. """

    never_seen_before_file_extension_count_notify_threshold = Size(data_key="never_seen_before_file_extension_count_notify_threshold")
    r""" Specifies the number of files found with a never seen before file extension up to which it is considered normal behavior. """

    never_seen_before_file_extension_duration_in_hours = Size(data_key="never_seen_before_file_extension_duration_in_hours")
    r""" Specifies the duration within which the specified number of files found with never seen before file extensions is considered normal behavior. """

    relaxing_popular_file_extensions = fields.Boolean(data_key="relaxing_popular_file_extensions")
    r""" Specifies whether popular file extensions should be relaxed from being treated as a suspect for the attack. Some popular file extensions are .txt, .pdf, and so on. """

    @property
    def resource(self):
        return AntiRansomwareVolumeAttackDetectionParameters

    gettable_fields = [
        "based_on_file_create_op_rate",
        "based_on_file_delete_op_rate",
        "based_on_file_rename_op_rate",
        "based_on_high_entropy_data_rate",
        "based_on_never_seen_before_file_extension",
        "file_create_op_rate_surge_notify_percent",
        "file_delete_op_rate_surge_notify_percent",
        "file_rename_op_rate_surge_notify_percent",
        "high_entropy_data_surge_notify_percent",
        "never_seen_before_file_extension_count_notify_threshold",
        "never_seen_before_file_extension_duration_in_hours",
        "relaxing_popular_file_extensions",
    ]
    """based_on_file_create_op_rate,based_on_file_delete_op_rate,based_on_file_rename_op_rate,based_on_high_entropy_data_rate,based_on_never_seen_before_file_extension,file_create_op_rate_surge_notify_percent,file_delete_op_rate_surge_notify_percent,file_rename_op_rate_surge_notify_percent,high_entropy_data_surge_notify_percent,never_seen_before_file_extension_count_notify_threshold,never_seen_before_file_extension_duration_in_hours,relaxing_popular_file_extensions,"""

    patchable_fields = [
        "based_on_file_create_op_rate",
        "based_on_file_delete_op_rate",
        "based_on_file_rename_op_rate",
        "based_on_high_entropy_data_rate",
        "based_on_never_seen_before_file_extension",
        "file_create_op_rate_surge_notify_percent",
        "file_delete_op_rate_surge_notify_percent",
        "file_rename_op_rate_surge_notify_percent",
        "high_entropy_data_surge_notify_percent",
        "never_seen_before_file_extension_count_notify_threshold",
        "never_seen_before_file_extension_duration_in_hours",
        "relaxing_popular_file_extensions",
    ]
    """based_on_file_create_op_rate,based_on_file_delete_op_rate,based_on_file_rename_op_rate,based_on_high_entropy_data_rate,based_on_never_seen_before_file_extension,file_create_op_rate_surge_notify_percent,file_delete_op_rate_surge_notify_percent,file_rename_op_rate_surge_notify_percent,high_entropy_data_surge_notify_percent,never_seen_before_file_extension_count_notify_threshold,never_seen_before_file_extension_duration_in_hours,relaxing_popular_file_extensions,"""

    postable_fields = [
        "based_on_file_create_op_rate",
        "based_on_file_delete_op_rate",
        "based_on_file_rename_op_rate",
        "based_on_high_entropy_data_rate",
        "based_on_never_seen_before_file_extension",
        "file_create_op_rate_surge_notify_percent",
        "file_delete_op_rate_surge_notify_percent",
        "file_rename_op_rate_surge_notify_percent",
        "high_entropy_data_surge_notify_percent",
        "never_seen_before_file_extension_count_notify_threshold",
        "never_seen_before_file_extension_duration_in_hours",
        "relaxing_popular_file_extensions",
    ]
    """based_on_file_create_op_rate,based_on_file_delete_op_rate,based_on_file_rename_op_rate,based_on_high_entropy_data_rate,based_on_never_seen_before_file_extension,file_create_op_rate_surge_notify_percent,file_delete_op_rate_surge_notify_percent,file_rename_op_rate_surge_notify_percent,high_entropy_data_surge_notify_percent,never_seen_before_file_extension_count_notify_threshold,never_seen_before_file_extension_duration_in_hours,relaxing_popular_file_extensions,"""


class AntiRansomwareVolumeAttackDetectionParameters(Resource):

    _schema = AntiRansomwareVolumeAttackDetectionParametersSchema
