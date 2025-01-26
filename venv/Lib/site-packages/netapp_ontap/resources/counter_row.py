r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

import asyncio
from datetime import datetime
import inspect
from typing import Callable, Iterable, List, Optional, Union

try:
    RECLINE_INSTALLED = False
    import recline
    from recline.arg_types.choices import Choices
    from recline.commands import ReclineCommandError
    from netapp_ontap.resource_table import ResourceTable
    RECLINE_INSTALLED = True
except ImportError:
    pass

from marshmallow import fields, EXCLUDE  # type: ignore

import netapp_ontap
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size
from netapp_ontap import NetAppResponse, HostConnection
from netapp_ontap.validations import enum_validation, len_validation, integer_validation
from netapp_ontap.error import NetAppRestError


__all__ = ["CounterRow", "CounterRowSchema"]
__pdoc__ = {
    "CounterRowSchema.resource": False,
    "CounterRowSchema.opts": False,
    "CounterRow.counter_row_show": False,
    "CounterRow.counter_row_create": False,
    "CounterRow.counter_row_modify": False,
    "CounterRow.counter_row_delete": False,
}


class CounterRowSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CounterRow object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the counter_row."""

    aggregation = fields.Nested("netapp_ontap.models.instance_counter_aggregation.InstanceCounterAggregationSchema", data_key="aggregation", unknown=EXCLUDE)
    r""" The aggregation field of the counter_row."""

    counter_table = fields.Nested("netapp_ontap.resources.counter_table.CounterTableSchema", data_key="counter_table", unknown=EXCLUDE)
    r""" The counter_table field of the counter_row."""

    counters = fields.List(fields.Nested("netapp_ontap.models.counter.CounterSchema", unknown=EXCLUDE), data_key="counters")
    r""" Array of counter name/value pairs."""

    id = fields.Str(
        data_key="id",
    )
    r""" Unique row idenfier."""

    properties = fields.List(fields.Nested("netapp_ontap.models.counter_property.CounterPropertySchema", unknown=EXCLUDE), data_key="properties")
    r""" Array of property name/value pairs."""

    @property
    def resource(self):
        return CounterRow

    gettable_fields = [
        "links",
        "aggregation",
        "counter_table.links",
        "counter_table.name",
        "counters",
        "id",
        "properties",
    ]
    """links,aggregation,counter_table.links,counter_table.name,counters,id,properties,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in CounterRow.get_collection(fields=field)]
    return getter

async def _wait_for_job(response: NetAppResponse) -> None:
    """Examine the given response. If it is a job, asynchronously wait for it to
    complete. While polling, prints the current status message of the job.
    """

    if not response.is_job:
        return
    from netapp_ontap.resources import Job
    job = Job(**response.http_response.json()["job"])
    while True:
        job.get(fields="state,message")
        if hasattr(job, "message"):
            print("[%s]: %s" % (job.state, job.message))
        if job.state == "failure":
            raise NetAppRestError("CounterRow modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class CounterRow(Resource):
    r""" A single row of counter and property counter data. """

    _schema = CounterRowSchema
    _path = "/api/cluster/counter/tables/{counter_table[name]}/rows"
    _keys = ["counter_table.name", "id"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Returns a collection of counter rows."""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="counter row show")
        def counter_row_show(
            counter_table_name,
            id: Choices.define(_get_field_list("id"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["id", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of CounterRow resources

            Args:
                id: Unique row idenfier.
            """

            kwargs = {}
            if id is not None:
                kwargs["id"] = id
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return CounterRow.get_collection(
                counter_table_name,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all CounterRow resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Returns a collection of counter rows."""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Returns a single counter row."""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





