r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsActionParameter", "EmsActionParameterSchema"]
__pdoc__ = {
    "EmsActionParameterSchema.resource": False,
    "EmsActionParameterSchema.opts": False,
    "EmsActionParameter": False,
}


class EmsActionParameterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsActionParameter object"""

    description = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="description")
    r""" The description field of the ems_action_parameter. """

    enum = fields.List(fields.Str, data_key="enum")
    r""" Specifies the possible values of the parameter.

Example: ["value-1","value-2"] """

    exclusive_maximum = fields.Boolean(data_key="exclusiveMaximum")
    r""" Specifies whether the "maximum" value is excluded in the parameter value range. """

    exclusive_minimum = fields.Boolean(data_key="exclusiveMinimum")
    r""" Specifies whether the "minimum" value is excluded in the parameter value range. """

    format = fields.Str(data_key="format")
    r""" An optional modifier that serves as a hint at the content and format of the parameter.

Example: date-time """

    help = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="help")
    r""" The help field of the ems_action_parameter. """

    items = fields.Str(data_key="items")
    r""" If the type of the parameter is an array, this specifies the type of items in the form of a JSON object, {"type":"type-value"}, where the type-value is one of the values for the type property.

Example: """

    max_items = Size(data_key="maxItems")
    r""" Specifies the maximum length of an array type parameter. """

    max_length = Size(data_key="maxLength")
    r""" Specifies the maximum length of a string type parameter. """

    maximum = Size(data_key="maximum")
    r""" Specifies the maximum value of the parameter. """

    min_items = Size(data_key="minItems")
    r""" Specifies the minimum length of an array type parameter. """

    min_length = Size(data_key="minLength")
    r""" Specifies the minimum length of a string type parameter. """

    minimum = Size(data_key="minimum")
    r""" Specifies the minimum value of the parameter. """

    name = fields.Str(data_key="name")
    r""" Parameter name.

Example: schedule-at """

    param_in = fields.Str(data_key="param_in")
    r""" Specifies where the parameter is placed when invoking the action.

Valid choices:

* body
* query """

    title = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="title")
    r""" The title field of the ems_action_parameter. """

    type = fields.Str(data_key="type")
    r""" Parameter type.

Valid choices:

* string
* number
* integer
* boolean
* array """

    validation_error_message = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="validation_error_message")
    r""" The validation_error_message field of the ems_action_parameter. """

    @property
    def resource(self):
        return EmsActionParameter

    gettable_fields = [
        "description",
        "enum",
        "exclusive_maximum",
        "exclusive_minimum",
        "format",
        "help",
        "items",
        "max_items",
        "max_length",
        "maximum",
        "min_items",
        "min_length",
        "minimum",
        "name",
        "param_in",
        "title",
        "type",
        "validation_error_message",
    ]
    """description,enum,exclusive_maximum,exclusive_minimum,format,help,items,max_items,max_length,maximum,min_items,min_length,minimum,name,param_in,title,type,validation_error_message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsActionParameter(Resource):

    _schema = EmsActionParameterSchema
