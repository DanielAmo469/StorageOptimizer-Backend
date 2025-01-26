r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsSnaplock", "NasApplicationComponentsSnaplockSchema"]
__pdoc__ = {
    "NasApplicationComponentsSnaplockSchema.resource": False,
    "NasApplicationComponentsSnaplockSchema.opts": False,
    "NasApplicationComponentsSnaplock": False,
}


class NasApplicationComponentsSnaplockSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsSnaplock object"""

    append_mode_enabled = fields.Boolean(data_key="append_mode_enabled")
    r""" Specifies if the volume append mode is enabled or disabled. When it is enabled, all the files created with write permissions on the volume are, by default, WORM appendable files. The user can append the data to a WORM appendable file but cannot modify the existing contents of the file nor delete the file until it expires. """

    autocommit_period = fields.Str(data_key="autocommit_period")
    r""" Specifies the autocommit period for SnapLock volume. All files which are not modified for a period greater than the autocommit period of the volume are committed to the WORM state. The autocommit period value represents a duration and must be specified in the ISO-8601 duration format. The autocommit period can be in years, months, days, hours, and minutes. A period specified for years, months, and days is represented in the ISO-8601 format as &quot;P&lt;num&gt;Y&quot;, &quot;P&lt;num&gt;M&quot;, &quot;P&lt;num&gt;D&quot; respectively, for example &quot;P10Y&quot; represents a duration of 10 years. A duration in hours and minutes is represented by &quot;PT&lt;num&gt;H&quot; and &quot;PT&lt;num&gt;M&quot; respectively. The period string must contain only a single time element that is, either years, months, days, hours, or minutes. A duration which combines different periods is not supported, for example &quot;P1Y10M&quot; is not supported. Apart from the duration specified in the ISO-8601 format, the autocommit field also accepts the string &quot;none&quot;. """

    retention = fields.Nested("netapp_ontap.models.nas_application_components_snaplock_retention.NasApplicationComponentsSnaplockRetentionSchema", unknown=EXCLUDE, data_key="retention")
    r""" The retention field of the nas_application_components_snaplock. """

    snaplock_type = fields.Str(data_key="snaplock_type")
    r""" The SnapLock type of the smart container.

Valid choices:

* compliance
* enterprise
* non_snaplock """

    @property
    def resource(self):
        return NasApplicationComponentsSnaplock

    gettable_fields = [
        "append_mode_enabled",
        "autocommit_period",
        "retention",
        "snaplock_type",
    ]
    """append_mode_enabled,autocommit_period,retention,snaplock_type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "append_mode_enabled",
        "autocommit_period",
        "retention",
        "snaplock_type",
    ]
    """append_mode_enabled,autocommit_period,retention,snaplock_type,"""


class NasApplicationComponentsSnaplock(Resource):

    _schema = NasApplicationComponentsSnaplockSchema
