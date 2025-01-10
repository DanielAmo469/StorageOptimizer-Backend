r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsSnaplockRetention", "NasApplicationComponentsSnaplockRetentionSchema"]
__pdoc__ = {
    "NasApplicationComponentsSnaplockRetentionSchema.resource": False,
    "NasApplicationComponentsSnaplockRetentionSchema.opts": False,
    "NasApplicationComponentsSnaplockRetention": False,
}


class NasApplicationComponentsSnaplockRetentionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsSnaplockRetention object"""

    default = fields.Str(data_key="default")
    r""" Specifies the default retention period that is applied to files while committing them to the WORM state without an associated retention period. The retention value represents a duration and must be specified in the ISO-8601 duration format. The retention period can be in years, months, days, hours, and minutes. A duration specified for years, months, and days is represented in the ISO-8601 format as quot;Plt;num&gt;Y&quot;, &quot;P&lt;num&gt;M&quot;, &quot;P&lt;num&gt;D&quot; respectively, for example &quot;P10Y&quot; represents a duration of 10 years. A duration in hours and minutes is represented by &quot;PT&lt;num&gt;H&quot; and &quot;PT&lt;num&gt;M&quot; respectively. The retention string must contain only a single time element that is, either years, months, days, hours, or minutes. A duration which combines different periods is not supported, for example &quot;P1Y10M&quot; is not supported. Apart from the duration specified in the ISO-8601 format, the duration field also accepts the string &quot;infinite&quot; to set an infinite retention period and the string &quot;unspecified&quot; to set an unspecified retention period. """

    maximum = fields.Str(data_key="maximum")
    r""" Specifies the maximum allowed retention period for files committed to the WORM state on the volume. The retention value represents a duration and must be specified in the ISO-8601 duration format. The retention period can be in years, months, days, hours, and minutes. A duration specified for years, months, and days is represented in the ISO-8601 format as &quot;P&lt;num&gt;Y&quot;, &quot;P&lt;num&gt;M&quot;, &quot;P&lt;num&gt;D&quot; respectively, for example &quot;P10Y&quot; represents a duration of 10 years. A duration in hours and minutes is represented by &quot;PT&lt;num&gt;H&quot; and &quot;PT&lt;num&gt;M&quot; respectively. The retention string must contain only a single time element that is, either years, months, days, hours, or minutes. A duration which combines different periods is not supported, for example &quot;P1Y10M&quot; is not supported. Apart from the duration specified in the ISO-8601 format, the duration field also accepts the string &quot;infinite&quot; to set an infinite retention period. """

    minimum = fields.Str(data_key="minimum")
    r""" Specifies the minimum allowed retention period for files committed to the WORM state on the volume. The retention value represents a duration and must be specified in the ISO-8601 duration format. The retention period can be in years, months, days, hours, and minutes. A duration specified for years, month,s and days is represented in the ISO-8601 format as &quot;P&lt;num&gt;Y&quot;, &quot;P&lt;num&gt;M&quot;, &quot;P&lt;num&gt;D&quot; respectively, for example &quot;P10Y&quot; represents a duration of 10 years. A duration in hours and minutes is represented by &quot;PT&lt;num&gt;H&quot; and &quot;PT&lt;num&gt;M&quot; respectively. The retention string must contain only a single time element that is, either years, months, days, hours, or minutes. A duration which combines different periods is not supported, for example &quot;P1Y10M&quot; is not supported. Apart from the duration specified in the ISO-8601 format, the duration field also accepts the string &quot;infinite&quot; to set an infinite retention period. """

    @property
    def resource(self):
        return NasApplicationComponentsSnaplockRetention

    gettable_fields = [
        "default",
        "maximum",
        "minimum",
    ]
    """default,maximum,minimum,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "default",
        "maximum",
        "minimum",
    ]
    """default,maximum,minimum,"""


class NasApplicationComponentsSnaplockRetention(Resource):

    _schema = NasApplicationComponentsSnaplockRetentionSchema
