r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
<h2> ONTAP `cifs sessions file show` functionality is used to provide a list of currently opened files.<h2/>
### Information on the open files

* Lists all files opened in current session.
## Example
### Retrieving established open file information
To retrieve the list of open files, use the following API. Note that <i>return_records=true</i>.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsOpenFile

with HostConnection(
    "<cluster-mgmt-ip>", username="admin", password="password", verify=False
):
    print(list(CifsOpenFile.get_collection(return_timeout=15)))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    CifsOpenFile(
        {
            "share": {"name": "root", "mode": "rwd"},
            "svm": {"name": "svm1", "uuid": "62234287-4412-11ec-b358-005056bbdd2c"},
            "node": {"name": "node1", "uuid": "9ea96c65-4411-11ec-b358-005056bbdd2c"},
            "volume": {"name": "vol1", "uuid": "7a5ef257-52de-11e8-95f4-005056952000"},
            "continuously_available": "no",
            "connection": {"identifier": 127465, "count": 1},
            "open_mode": "w",
            "path": "a\\b\\c\\f1",
        }
    ),
    CifsOpenFile(
        {
            "share": {"name": "root", "mode": "rwd"},
            "svm": {"name": "svm2", "uuid": "62234287-4412-11ec-b358-005056bbdd2c"},
            "node": {"name": "node2", "uuid": "9ea96c65-4411-11ec-b358-005056bbdd2c"},
            "volume": {"name": "vol2", "uuid": "7a5ef257-52de-11e8-95f4-005056952000"},
            "continuously_available": "no",
            "connection": {"identifier": 127465, "count": 1},
            "open_mode": "r",
            "path": "a\\b\\c\\f2",
        }
    ),
]

```
</div>
</div>

---
### Retrieving specific open file Information
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsOpenFile

with HostConnection(
    "<cluster-mgmt-ip>", username="admin", password="password", verify=False
):
    resource = CifsOpenFile(
        identifier=14,
        **{
            "session.identifier": "4466726405421400095",
            "connection.identifier": "82792",
            "svm.uuid": "b009a9e7-4081-b576-7575-ada21efcaf16",
            "node.uuid": "8d7ef257-52de-11e8-95f4-005056952ecd",
        }
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
CifsOpenFile(
    {
        "share": {"name": "root", "mode": "rwd"},
        "svm": {"name": "svm1", "uuid": "62234287-4412-11ec-b358-005056bbdd2c"},
        "node": {"name": "node1", "uuid": "9ea96c65-4411-11ec-b358-005056bbdd2c"},
        "volume": {"name": "vol1", "uuid": "7a5ef257-52de-11e8-95f4-005056952000"},
        "continuously_available": "no",
        "connection": {"identifier": 127465, "count": 1},
        "open_mode": "r",
        "path": "a\\b\\c\\f1",
    }
)

```
</div>
</div>

---
### Closing a specific file based on `file.identifier`, `connection.identifier` and `session_id`
The file being closed is identified by the UUID of its SVM, the corresponding file.identifier, connection.identifier and session_id.
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsOpenFile

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = CifsOpenFile(
        identifier=14,
        **{
            "session.identifier": "4466726405421400095",
            "connection.identifier": "82792",
            "svm.uuid": "b009a9e7-4081-b576-7575-ada21efcaf16",
            "node.uuid": "8d7ef257-52de-11e8-95f4-005056952ecd",
        }
    )
    resource.delete()

```

---"""

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


__all__ = ["CifsOpenFile", "CifsOpenFileSchema"]
__pdoc__ = {
    "CifsOpenFileSchema.resource": False,
    "CifsOpenFileSchema.opts": False,
    "CifsOpenFile.cifs_open_file_show": False,
    "CifsOpenFile.cifs_open_file_create": False,
    "CifsOpenFile.cifs_open_file_modify": False,
    "CifsOpenFile.cifs_open_file_delete": False,
}


class CifsOpenFileSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsOpenFile object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the cifs_open_file."""

    connection = fields.Nested("netapp_ontap.models.cifs_open_file_connection.CifsOpenFileConnectionSchema", data_key="connection", unknown=EXCLUDE)
    r""" The connection field of the cifs_open_file."""

    continuously_available = fields.Str(
        data_key="continuously_available",
        validate=enum_validation(['no', 'yes']),
    )
    r""" The type of continuous availability protection provided to the file.
Opened files are continuously available if there are opened through a SMB3 client through a share with "continuously_available" set to yes.
These open files are capable of non-disruptively recovering from take over and giveback as well as general aggregate relocation.
- no: the open file is not continuously available.
- yes: the open file is continuously available.


Valid choices:

* no
* yes"""

    identifier = Size(
        data_key="identifier",
    )
    r""" The unique identifier for the opened file.

Example: 17"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the cifs_open_file."""

    open_mode = fields.Str(
        data_key="open_mode",
        validate=enum_validation(['r', 'w', 'd']),
    )
    r""" Open mode corresponding to the opened file
- r: Opened for read
- w: Opened for write
- d: Opened for Delete


Valid choices:

* r
* w
* d"""

    path = fields.Str(
        data_key="path",
    )
    r""" Path from CIFS share.

Example: sub1\sub2\f4"""

    range_locks_count = Size(
        data_key="range_locks_count",
    )
    r""" The number of range locks granted on the file.

Example: 4"""

    session = fields.Nested("netapp_ontap.models.cifs_open_file_session.CifsOpenFileSessionSchema", data_key="session", unknown=EXCLUDE)
    r""" The session field of the cifs_open_file."""

    share = fields.Nested("netapp_ontap.models.cifs_open_file_share.CifsOpenFileShareSchema", data_key="share", unknown=EXCLUDE)
    r""" The share field of the cifs_open_file."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the cifs_open_file."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['directory', 'regular', 'stream', 'symlink']),
    )
    r""" The type of opened file.
The file can be a regular file, directory, a symbolic link to an existing file/directory, or an alternate data stream.


Valid choices:

* directory
* regular
* stream
* symlink"""

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", data_key="volume", unknown=EXCLUDE)
    r""" The volume field of the cifs_open_file."""

    @property
    def resource(self):
        return CifsOpenFile

    gettable_fields = [
        "links",
        "connection",
        "continuously_available",
        "identifier",
        "node.links",
        "node.name",
        "node.uuid",
        "open_mode",
        "path",
        "range_locks_count",
        "session",
        "share",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "type",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """links,connection,continuously_available,identifier,node.links,node.name,node.uuid,open_mode,path,range_locks_count,session,share,svm.links,svm.name,svm.uuid,type,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "connection",
        "identifier",
        "node.name",
        "node.uuid",
        "session",
        "share",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """connection,identifier,node.name,node.uuid,session,share,svm.name,svm.uuid,volume.name,volume.uuid,"""

    postable_fields = [
        "connection",
        "identifier",
        "node.name",
        "node.uuid",
        "session",
        "share",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """connection,identifier,node.name,node.uuid,session,share,svm.name,svm.uuid,volume.name,volume.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in CifsOpenFile.get_collection(fields=field)]
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
            raise NetAppRestError("CifsOpenFile modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class CifsOpenFile(Resource):
    r""" Manage opened files over CIFS. """

    _schema = CifsOpenFileSchema
    _path = "/api/protocols/cifs/session/files"
    _keys = ["node.uuid", "svm.uuid", "identifier", "connection.identifier", "session.identifier"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves CIFS Open Files
### Learn more
* [`DOC /protocols/cifs/session/files`](#docs-NAS-protocols_cifs_session_files)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="cifs open file show")
        def cifs_open_file_show(
            fields: List[Choices.define(["continuously_available", "identifier", "open_mode", "path", "range_locks_count", "type", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of CifsOpenFile resources

            Args:
                continuously_available: The type of continuous availability protection provided to the file. Opened files are continuously available if there are opened through a SMB3 client through a share with \"continuously_available\" set to yes. These open files are capable of non-disruptively recovering from take over and giveback as well as general aggregate relocation. - no: the open file is not continuously available. - yes: the open file is continuously available. 
                identifier: The unique identifier for the opened file.
                open_mode: Open mode corresponding to the opened file - r: Opened for read - w: Opened for write - d: Opened for Delete 
                path: Path from CIFS share.
                range_locks_count: The number of range locks granted on the file.
                type: The type of opened file. The file can be a regular file, directory, a symbolic link to an existing file/directory, or an alternate data stream. 
            """

            kwargs = {}
            if continuously_available is not None:
                kwargs["continuously_available"] = continuously_available
            if identifier is not None:
                kwargs["identifier"] = identifier
            if open_mode is not None:
                kwargs["open_mode"] = open_mode
            if path is not None:
                kwargs["path"] = path
            if range_locks_count is not None:
                kwargs["range_locks_count"] = range_locks_count
            if type is not None:
                kwargs["type"] = type
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return CifsOpenFile.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all CifsOpenFile resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)



    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["CifsOpenFile"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Closes open files identified by svm.uuid, file.identifier, connection.identifier and session_id.
### Learn more
* [`DOC /protocols/cifs/session/files`](#docs-NAS-protocols_cifs_session_files)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves CIFS Open Files
### Learn more
* [`DOC /protocols/cifs/session/files`](#docs-NAS-protocols_cifs_session_files)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves specific CIFS Open File
### Learn more
* [`DOC /protocols/cifs/session/files`](#docs-NAS-protocols_cifs_session_files)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)



    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Closes open files identified by svm.uuid, file.identifier, connection.identifier and session_id.
### Learn more
* [`DOC /protocols/cifs/session/files`](#docs-NAS-protocols_cifs_session_files)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="cifs open file delete")
        async def cifs_open_file_delete(
        ) -> None:
            """Delete an instance of a CifsOpenFile resource

            Args:
                continuously_available: The type of continuous availability protection provided to the file. Opened files are continuously available if there are opened through a SMB3 client through a share with \"continuously_available\" set to yes. These open files are capable of non-disruptively recovering from take over and giveback as well as general aggregate relocation. - no: the open file is not continuously available. - yes: the open file is continuously available. 
                identifier: The unique identifier for the opened file.
                open_mode: Open mode corresponding to the opened file - r: Opened for read - w: Opened for write - d: Opened for Delete 
                path: Path from CIFS share.
                range_locks_count: The number of range locks granted on the file.
                type: The type of opened file. The file can be a regular file, directory, a symbolic link to an existing file/directory, or an alternate data stream. 
            """

            kwargs = {}
            if continuously_available is not None:
                kwargs["continuously_available"] = continuously_available
            if identifier is not None:
                kwargs["identifier"] = identifier
            if open_mode is not None:
                kwargs["open_mode"] = open_mode
            if path is not None:
                kwargs["path"] = path
            if range_locks_count is not None:
                kwargs["range_locks_count"] = range_locks_count
            if type is not None:
                kwargs["type"] = type

            if hasattr(CifsOpenFile, "find"):
                resource = CifsOpenFile.find(
                    **kwargs
                )
            else:
                resource = CifsOpenFile()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete CifsOpenFile: %s" % err)


