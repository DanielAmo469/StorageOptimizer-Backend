r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsSyslogFormat", "EmsSyslogFormatSchema"]
__pdoc__ = {
    "EmsSyslogFormatSchema.resource": False,
    "EmsSyslogFormatSchema.opts": False,
    "EmsSyslogFormat": False,
}


class EmsSyslogFormatSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsSyslogFormat object"""

    hostname_override = fields.Str(data_key="hostname_override")
    r""" Syslog Hostname Format Override. The supported hostname formats are no_override (hostname format based on the syslog.format.message property i.e. fqdn if syslog.format.message is rfc_5424, hostname_only if syslog.format.message is legacy_netapp), fqdn (Fully Qualified Domain Name) and hostname_only.


Valid choices:

* no_override
* fqdn
* hostname_only """

    message = fields.Str(data_key="message")
    r""" Syslog Message Format. The supported message formats are legacy_netapp (format: &lt;PRIVAL&gt;TIMESTAMP [HOSTNAME:Event-name:Event-severity]: MSG) and rfc_5424 (format: &lt;PRIVAL&gt;VERSION TIMESTAMP HOSTNAME Event-source - Event-name - MSG).


Valid choices:

* legacy_netapp
* rfc_5424 """

    timestamp_override = fields.Str(data_key="timestamp_override")
    r""" Syslog Timestamp Format Override. The supported timestamp formats are no_override (timestamp format based on the syslog.format.message property i.e. rfc_3164 if syslog.format.message is legacy_netapp, iso_8601_local_time if syslog.format.message is rfc_5424), rfc_3164 (format: Mmm dd hh:mm:ss), iso_8601_local_time (format: YYYY-MM-DDThh:mm:ss+/-hh:mm) and iso_8601_utc (format: YYYY-MM-DDThh:mm:ssZ).


Valid choices:

* no_override
* rfc_3164
* iso_8601_local_time
* iso_8601_utc """

    @property
    def resource(self):
        return EmsSyslogFormat

    gettable_fields = [
        "hostname_override",
        "message",
        "timestamp_override",
    ]
    """hostname_override,message,timestamp_override,"""

    patchable_fields = [
        "hostname_override",
        "message",
        "timestamp_override",
    ]
    """hostname_override,message,timestamp_override,"""

    postable_fields = [
        "hostname_override",
        "message",
        "timestamp_override",
    ]
    """hostname_override,message,timestamp_override,"""


class EmsSyslogFormat(Resource):

    _schema = EmsSyslogFormatSchema
