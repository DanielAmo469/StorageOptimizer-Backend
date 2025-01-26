r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventAction", "EmsEventActionSchema"]
__pdoc__ = {
    "EmsEventActionSchema.resource": False,
    "EmsEventActionSchema.opts": False,
    "EmsEventAction": False,
}


class EmsEventActionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventAction object"""

    confirmation_message = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="confirmation_message")
    r""" The confirmation_message field of the ems_event_action. """

    description = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="description")
    r""" The description field of the ems_event_action. """

    href = fields.Str(data_key="href")
    r""" URI on which to perform the action, using the HTTP method specified in the method property.

Example: /api/resourcelink """

    method = fields.Str(data_key="method")
    r""" HTTP verb, such as PATCH, POST, used to perform the action.

Example: PATCH """

    name = fields.Str(data_key="name")
    r""" Name of the action.

Example: schedule """

    parameters = fields.List(fields.Nested("netapp_ontap.models.ems_action_parameter.EmsActionParameterSchema", unknown=EXCLUDE), data_key="parameters")
    r""" Parameter list for the action. """

    request_body_template = fields.Dict(data_key="request_body_template")
    r""" JSON object describing the structure of the request body if arguments must be
provided in the body when invoking the action.
A JSON string value that takes the form of {parameter-name} must be substituted by user input
values encoded as the appropriate JSON type.
The following table gives examples where the parameter type is a string, an integer, and
an array:
| request_body_template value | parameter value              | request body                    |
| --------------------------- | ---------------------------- | ------------------------------- |
| {"name": "{user-name}"}     | user-name="Joe"              | {"name": "Joe"}                 |
| {"retry count": "{count}"}  | count=10                     | {"retry_count": 10}             |
| {"domains": "{dns_names}"}  | dns_names=["dns-1", "dns-2"] | {"domains": ["dns-1", "dns-2"]} |
Only JSON string values that start with a '{' and end with a '}' should be considered
for parameter substitutions. A JSON string value such as "{user-name} is the syntax" should
be treated as a literal string with no parameter substitution to be performed.
Double curly braces '{{' and '}}' are used to escape parameter substitutions, therefore double
curly braces must be converted to single curly braces.
For example, "{{user-name}}" is converted to the JSON string value "{user-name}".
Note that this rule only applies if a JSON string starts with '{{' and ends with '}}'.
The following table provides examples of when parameter substitutions must not be performed:
| request_body_template value       | request body                     |
| --------------------------------- | -------------------------------- |
| {"name": "{user-name} is bad"}    | {"name": "{user-name} is bad"}   |
| {"name": "{{user-name}}"}         | {"name": "{user-name}"}          |
| {"name": "{{user-name}} is bad"}  | {"name": "{{user-name}} is bad"} |
| {"name": "{{{{user-name}}}}"}     | {"name": "{{user-name}}"}        | """

    title = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="title")
    r""" The title field of the ems_event_action. """

    @property
    def resource(self):
        return EmsEventAction

    gettable_fields = [
        "confirmation_message",
        "description",
        "href",
        "method",
        "name",
        "parameters",
        "request_body_template",
        "title",
    ]
    """confirmation_message,description,href,method,name,parameters,request_body_template,title,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsEventAction(Resource):

    _schema = EmsEventActionSchema
