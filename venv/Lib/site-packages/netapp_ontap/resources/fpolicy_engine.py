r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
The FPolicy engine allows you to configure the external servers to which the file access notifications are sent. As part of FPolicy engine configuration, you can configure a server(s) to which the notification is sent, an optional set of secondary server(s) to which the notification is sent in the case of a primary server(s) failure, the port number for FPolicy application, the type of the engine, which is either synchronous or asynchronous and the format of the notifications, which is either xml or protobuf. </br>
For the synchronous engine, ONTAP will wait for a response from the FPolicy application before it allows the operation. With an asynchronous engine, ONTAP proceeds with the operation processing after sending the notification to the FPolicy application. An engine can belong to multiple FPolicy policies. If the format is not specified, the default format, xml, is configured.
## Examples
### Creating an FPolicy engine
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEngine("4f643fb4-fd21-11e8-ae49-0050568e2c1e")
    resource.name = "engine0"
    resource.port = 9876
    resource.primary_servers = ["10.132.145.22", "10.140.101.109"]
    resource.secondary_servers = ["10.132.145.20", "10.132.145.21"]
    resource.type = "synchronous"
    resource.format = "xml"
    resource.request_abort_timeout = "PT3M"
    resource.request_cancel_timeout = "PT29S"
    resource.server_progress_timeout = "PT1M"
    resource.status_request_interval = "PT23S"
    resource.keep_alive_interval = "PT2M"
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
FpolicyEngine(
    {
        "port": 9876,
        "type": "synchronous",
        "secondary_servers": ["10.132.145.20", "10.132.145.21"],
        "name": "engine0",
        "primary_servers": ["10.132.145.22", "10.140.101.109"],
        "format": "xml",
    }
)

```
</div>
</div>

---
### Creating an FPolicy engine with the minimum required fields
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEngine("4f643fb4-fd21-11e8-ae49-0050568e2c1e")
    resource.name = "engine0"
    resource.port = 9876
    resource.primary_servers = ["10.132.145.22", "10.140.101.109"]
    resource.type = "synchronous"
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
FpolicyEngine(
    {
        "port": 9876,
        "type": "synchronous",
        "name": "engine0",
        "primary_servers": ["10.132.145.22", "10.140.101.109"],
        "format": "xml",
    }
)

```
</div>
</div>

---
### Retrieving an FPolicy engine configuration for a particular SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            FpolicyEngine.get_collection(
                "4f643fb4-fd21-11e8-ae49-0050568e2c1e", fields="*", return_timeout=15
            )
        )
    )

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
[
    FpolicyEngine(
        {
            "port": 9876,
            "svm": {"uuid": "4f643fb4-fd21-11e8-ae49-0050568e2c1e"},
            "type": "synchronous",
            "name": "cifs",
            "primary_servers": ["10.20.20.10"],
        }
    ),
    FpolicyEngine(
        {
            "port": 9876,
            "max_server_requests": 500,
            "keep_alive_interval": "PT2M",
            "svm": {"uuid": "4f643fb4-fd21-11e8-ae49-0050568e2c1e"},
            "type": "synchronous",
            "secondary_servers": ["10.132.145.20", "10.132.145.22"],
            "status_request_interval": "PT23S",
            "buffer_size": {"send_buffer": 1048576, "recv_buffer": 262144},
            "name": "nfs",
            "request_cancel_timeout": "PT29S",
            "server_progress_timeout": "PT1M",
            "primary_servers": ["10.23.140.64", "10.140.101.109"],
            "resiliency": {"enabled": False, "retention_duration": "PT3M"},
            "format": "xml",
            "request_abort_timeout": "PT3M",
            "ssl_option": "no_auth",
        }
    ),
]

```
</div>
</div>

---
### Retrieving a specific FPolicy engine configuration for an SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEngine("4f643fb4-fd21-11e8-ae49-0050568e2c1e", name="cifs")
    resource.get(fields="*")
    print(resource)

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
FpolicyEngine(
    {
        "port": 9876,
        "svm": {"uuid": "4f643fb4-fd21-11e8-ae49-0050568e2c1e"},
        "type": "synchronous",
        "name": "cifs",
        "primary_servers": ["10.20.20.10"],
        "format": "xml",
    }
)

```
</div>
</div>

---
### Updating an FPolicy engine for an SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEngine("4f643fb4-fd21-11e8-ae49-0050568e2c1e", name="cifs")
    resource.port = 6666
    resource.secondary_servers = ["10.132.145.20", "10.132.145.21"]
    resource.type = "synchronous"
    resource.patch()

```

---
### Updating all the attributes of a specific FPolicy engine for an SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEngine

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEngine("4f643fb4-fd21-11e8-ae49-0050568e2c1e", name="cifs")
    resource.port = 9876
    resource.primary_servers = ["10.132.145.20", "10.140.101.109"]
    resource.secondary_servers = ["10.132.145.23", "10.132.145.21"]
    resource.type = "synchronous"
    resource.format = "protobuf"
    resource.patch()

```

---
### Deleting a specific FPolicy engine for an SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FpolicyEvent

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FpolicyEvent("4f643fb4-fd21-11e8-ae49-0050568e2c1e", name="cifs")
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


__all__ = ["FpolicyEngine", "FpolicyEngineSchema"]
__pdoc__ = {
    "FpolicyEngineSchema.resource": False,
    "FpolicyEngineSchema.opts": False,
    "FpolicyEngine.fpolicy_engine_show": False,
    "FpolicyEngine.fpolicy_engine_create": False,
    "FpolicyEngine.fpolicy_engine_modify": False,
    "FpolicyEngine.fpolicy_engine_delete": False,
}


class FpolicyEngineSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyEngine object"""

    buffer_size = fields.Nested("netapp_ontap.models.fpolicy_engine_buffer_size.FpolicyEngineBufferSizeSchema", data_key="buffer_size", unknown=EXCLUDE)
    r""" The buffer_size field of the fpolicy_engine."""

    certificate = fields.Nested("netapp_ontap.models.fpolicy_engine_certificate.FpolicyEngineCertificateSchema", data_key="certificate", unknown=EXCLUDE)
    r""" The certificate field of the fpolicy_engine."""

    format = fields.Str(
        data_key="format",
        validate=enum_validation(['xml', 'protobuf']),
    )
    r""" The format for the notification messages sent to the FPolicy servers.
  The possible values are:

    * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.
    * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form.


Valid choices:

* xml
* protobuf"""

    keep_alive_interval = fields.Str(
        data_key="keep_alive_interval",
    )
    r""" Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.

Example: PT2M"""

    max_server_requests = Size(
        data_key="max_server_requests",
        validate=integer_validation(minimum=1, maximum=10000),
    )
    r""" Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.

Example: 500"""

    name = fields.Str(
        data_key="name",
    )
    r""" Specifies the name to assign to the external server configuration.

Example: fp_ex_eng"""

    port = Size(
        data_key="port",
    )
    r""" Port number of the FPolicy server application.

Example: 9876"""

    primary_servers = fields.List(fields.Str, data_key="primary_servers")
    r""" The primary_servers field of the fpolicy_engine.

Example: ["10.132.145.20","10.140.101.109"]"""

    request_abort_timeout = fields.Str(
        data_key="request_abort_timeout",
    )
    r""" Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.

Example: PT40S"""

    request_cancel_timeout = fields.Str(
        data_key="request_cancel_timeout",
    )
    r""" Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.

Example: PT20S"""

    resiliency = fields.Nested("netapp_ontap.models.fpolicy_engine_resiliency.FpolicyEngineResiliencySchema", data_key="resiliency", unknown=EXCLUDE)
    r""" The resiliency field of the fpolicy_engine."""

    secondary_servers = fields.List(fields.Str, data_key="secondary_servers")
    r""" The secondary_servers field of the fpolicy_engine.

Example: ["10.132.145.20","10.132.145.21"]"""

    server_progress_timeout = fields.Str(
        data_key="server_progress_timeout",
    )
    r""" Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.

Example: PT1M"""

    ssl_option = fields.Str(
        data_key="ssl_option",
        validate=enum_validation(['no_auth', 'server_auth', 'mutual_auth']),
    )
    r""" Specifies the SSL option for external communication with the FPolicy server. Possible values include the following:

* no_auth       When set to "no_auth", no authentication takes place.
* server_auth   When set to "server_auth", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate.
* mutual_auth   When set to "mutual_auth", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM.


Valid choices:

* no_auth
* server_auth
* mutual_auth"""

    status_request_interval = fields.Str(
        data_key="status_request_interval",
    )
    r""" Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.

Example: PT10S"""

    svm = fields.Nested("netapp_ontap.models.fpolicy_engine_svm.FpolicyEngineSvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the fpolicy_engine."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['synchronous', 'asynchronous']),
    )
    r""" The notification mode determines what ONTAP does after sending notifications to FPolicy servers.
  The possible values are:

    * synchronous  - After sending a notification, wait for a response from the FPolicy server.
    * asynchronous - After sending a notification, file request processing continues.


Valid choices:

* synchronous
* asynchronous"""

    @property
    def resource(self):
        return FpolicyEngine

    gettable_fields = [
        "buffer_size",
        "certificate",
        "format",
        "keep_alive_interval",
        "max_server_requests",
        "name",
        "port",
        "primary_servers",
        "request_abort_timeout",
        "request_cancel_timeout",
        "resiliency",
        "secondary_servers",
        "server_progress_timeout",
        "ssl_option",
        "status_request_interval",
        "svm",
        "type",
    ]
    """buffer_size,certificate,format,keep_alive_interval,max_server_requests,name,port,primary_servers,request_abort_timeout,request_cancel_timeout,resiliency,secondary_servers,server_progress_timeout,ssl_option,status_request_interval,svm,type,"""

    patchable_fields = [
        "buffer_size",
        "certificate",
        "format",
        "keep_alive_interval",
        "max_server_requests",
        "port",
        "primary_servers",
        "request_abort_timeout",
        "request_cancel_timeout",
        "resiliency",
        "secondary_servers",
        "server_progress_timeout",
        "ssl_option",
        "status_request_interval",
        "svm",
        "type",
    ]
    """buffer_size,certificate,format,keep_alive_interval,max_server_requests,port,primary_servers,request_abort_timeout,request_cancel_timeout,resiliency,secondary_servers,server_progress_timeout,ssl_option,status_request_interval,svm,type,"""

    postable_fields = [
        "buffer_size",
        "certificate",
        "format",
        "keep_alive_interval",
        "max_server_requests",
        "name",
        "port",
        "primary_servers",
        "request_abort_timeout",
        "request_cancel_timeout",
        "resiliency",
        "secondary_servers",
        "server_progress_timeout",
        "ssl_option",
        "status_request_interval",
        "svm",
        "type",
    ]
    """buffer_size,certificate,format,keep_alive_interval,max_server_requests,name,port,primary_servers,request_abort_timeout,request_cancel_timeout,resiliency,secondary_servers,server_progress_timeout,ssl_option,status_request_interval,svm,type,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in FpolicyEngine.get_collection(fields=field)]
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
            raise NetAppRestError("FpolicyEngine modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class FpolicyEngine(Resource):
    r""" Defines how ONTAP makes and manages connections to external FPolicy servers. """

    _schema = FpolicyEngineSchema
    _path = "/api/protocols/fpolicy/{svm[uuid]}/engines"
    _keys = ["svm.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves FPolicy engine configurations of all the engines for a specified SVM. ONTAP allows creation of cluster-level FPolicy engines that act as a template for all the SVMs belonging to the cluster. These cluster-level FPolicy engines are also retrieved for the specified SVM.
### Related ONTAP commands
* `fpolicy policy external-engine show`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fpolicy engine show")
        def fpolicy_engine_show(
            svm_uuid,
            format: Choices.define(_get_field_list("format"), cache_choices=True, inexact=True)=None,
            keep_alive_interval: Choices.define(_get_field_list("keep_alive_interval"), cache_choices=True, inexact=True)=None,
            max_server_requests: Choices.define(_get_field_list("max_server_requests"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            port: Choices.define(_get_field_list("port"), cache_choices=True, inexact=True)=None,
            primary_servers: Choices.define(_get_field_list("primary_servers"), cache_choices=True, inexact=True)=None,
            request_abort_timeout: Choices.define(_get_field_list("request_abort_timeout"), cache_choices=True, inexact=True)=None,
            request_cancel_timeout: Choices.define(_get_field_list("request_cancel_timeout"), cache_choices=True, inexact=True)=None,
            secondary_servers: Choices.define(_get_field_list("secondary_servers"), cache_choices=True, inexact=True)=None,
            server_progress_timeout: Choices.define(_get_field_list("server_progress_timeout"), cache_choices=True, inexact=True)=None,
            ssl_option: Choices.define(_get_field_list("ssl_option"), cache_choices=True, inexact=True)=None,
            status_request_interval: Choices.define(_get_field_list("status_request_interval"), cache_choices=True, inexact=True)=None,
            type: Choices.define(_get_field_list("type"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["format", "keep_alive_interval", "max_server_requests", "name", "port", "primary_servers", "request_abort_timeout", "request_cancel_timeout", "secondary_servers", "server_progress_timeout", "ssl_option", "status_request_interval", "type", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of FpolicyEngine resources

            Args:
                format: The format for the notification messages sent to the FPolicy servers.   The possible values are:     * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.     * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form. 
                keep_alive_interval: Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.
                max_server_requests: Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.
                name: Specifies the name to assign to the external server configuration.
                port: Port number of the FPolicy server application.
                primary_servers: 
                request_abort_timeout: Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.
                request_cancel_timeout: Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.
                secondary_servers: 
                server_progress_timeout: Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.
                ssl_option: Specifies the SSL option for external communication with the FPolicy server. Possible values include the following: * no_auth       When set to \"no_auth\", no authentication takes place. * server_auth   When set to \"server_auth\", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate. * mutual_auth   When set to \"mutual_auth\", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM. 
                status_request_interval: Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.
                type: The notification mode determines what ONTAP does after sending notifications to FPolicy servers.   The possible values are:     * synchronous  - After sending a notification, wait for a response from the FPolicy server.     * asynchronous - After sending a notification, file request processing continues. 
            """

            kwargs = {}
            if format is not None:
                kwargs["format"] = format
            if keep_alive_interval is not None:
                kwargs["keep_alive_interval"] = keep_alive_interval
            if max_server_requests is not None:
                kwargs["max_server_requests"] = max_server_requests
            if name is not None:
                kwargs["name"] = name
            if port is not None:
                kwargs["port"] = port
            if primary_servers is not None:
                kwargs["primary_servers"] = primary_servers
            if request_abort_timeout is not None:
                kwargs["request_abort_timeout"] = request_abort_timeout
            if request_cancel_timeout is not None:
                kwargs["request_cancel_timeout"] = request_cancel_timeout
            if secondary_servers is not None:
                kwargs["secondary_servers"] = secondary_servers
            if server_progress_timeout is not None:
                kwargs["server_progress_timeout"] = server_progress_timeout
            if ssl_option is not None:
                kwargs["ssl_option"] = ssl_option
            if status_request_interval is not None:
                kwargs["status_request_interval"] = status_request_interval
            if type is not None:
                kwargs["type"] = type
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return FpolicyEngine.get_collection(
                svm_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all FpolicyEngine resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["FpolicyEngine"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a specific FPolicy engine configuration of an SVM. Modification of an FPolicy engine that is attached to one or more enabled FPolicy policies is not allowed.
### Related ONTAP commands
* `fpolicy policy external-engine modify`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["FpolicyEngine"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["FpolicyEngine"], NetAppResponse]:
        r"""Creates an FPolicy engine configuration for a specified SVM. FPolicy engine creation is allowed only on data SVMs.
### Required properties
* `svm.uuid` - Existing SVM in which to create the FPolicy engine.
* `name` - Name of external engine.
* `port` - Port number of the FPolicy server application.
* `primary_servers` - List of primary FPolicy servers to which the node will send notifications.
### Recommended optional properties
* `secondary_servers` - It is recommended to configure secondary FPolicy server to which the node will send notifications when the primary server is down.
### Default property values
* `type` - _synchronous_
* `format` - _xml_
### Related ONTAP commands
* `fpolicy policy external-engine create`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["FpolicyEngine"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the FPolicy external engine configuration. Deletion of an FPolicy engine that is attached to one or more FPolicy policies is not allowed.
### Related ONTAP commands
* `fpolicy policy external-engine modify`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves FPolicy engine configurations of all the engines for a specified SVM. ONTAP allows creation of cluster-level FPolicy engines that act as a template for all the SVMs belonging to the cluster. These cluster-level FPolicy engines are also retrieved for the specified SVM.
### Related ONTAP commands
* `fpolicy policy external-engine show`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a particular FPolicy engine configuration of a specified SVM. A cluster-level FPolicy engine configuration cannot be retrieved for a data SVM.
### Related ONTAP commands
* `fpolicy policy external-engine show`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Creates an FPolicy engine configuration for a specified SVM. FPolicy engine creation is allowed only on data SVMs.
### Required properties
* `svm.uuid` - Existing SVM in which to create the FPolicy engine.
* `name` - Name of external engine.
* `port` - Port number of the FPolicy server application.
* `primary_servers` - List of primary FPolicy servers to which the node will send notifications.
### Recommended optional properties
* `secondary_servers` - It is recommended to configure secondary FPolicy server to which the node will send notifications when the primary server is down.
### Default property values
* `type` - _synchronous_
* `format` - _xml_
### Related ONTAP commands
* `fpolicy policy external-engine create`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fpolicy engine create")
        async def fpolicy_engine_create(
            svm_uuid,
            buffer_size: dict = None,
            certificate: dict = None,
            format: str = None,
            keep_alive_interval: str = None,
            max_server_requests: Size = None,
            name: str = None,
            port: Size = None,
            primary_servers: dict = None,
            request_abort_timeout: str = None,
            request_cancel_timeout: str = None,
            resiliency: dict = None,
            secondary_servers: dict = None,
            server_progress_timeout: str = None,
            ssl_option: str = None,
            status_request_interval: str = None,
            svm: dict = None,
            type: str = None,
        ) -> ResourceTable:
            """Create an instance of a FpolicyEngine resource

            Args:
                buffer_size: 
                certificate: 
                format: The format for the notification messages sent to the FPolicy servers.   The possible values are:     * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.     * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form. 
                keep_alive_interval: Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.
                max_server_requests: Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.
                name: Specifies the name to assign to the external server configuration.
                port: Port number of the FPolicy server application.
                primary_servers: 
                request_abort_timeout: Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.
                request_cancel_timeout: Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.
                resiliency: 
                secondary_servers: 
                server_progress_timeout: Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.
                ssl_option: Specifies the SSL option for external communication with the FPolicy server. Possible values include the following: * no_auth       When set to \"no_auth\", no authentication takes place. * server_auth   When set to \"server_auth\", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate. * mutual_auth   When set to \"mutual_auth\", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM. 
                status_request_interval: Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.
                svm: 
                type: The notification mode determines what ONTAP does after sending notifications to FPolicy servers.   The possible values are:     * synchronous  - After sending a notification, wait for a response from the FPolicy server.     * asynchronous - After sending a notification, file request processing continues. 
            """

            kwargs = {}
            if buffer_size is not None:
                kwargs["buffer_size"] = buffer_size
            if certificate is not None:
                kwargs["certificate"] = certificate
            if format is not None:
                kwargs["format"] = format
            if keep_alive_interval is not None:
                kwargs["keep_alive_interval"] = keep_alive_interval
            if max_server_requests is not None:
                kwargs["max_server_requests"] = max_server_requests
            if name is not None:
                kwargs["name"] = name
            if port is not None:
                kwargs["port"] = port
            if primary_servers is not None:
                kwargs["primary_servers"] = primary_servers
            if request_abort_timeout is not None:
                kwargs["request_abort_timeout"] = request_abort_timeout
            if request_cancel_timeout is not None:
                kwargs["request_cancel_timeout"] = request_cancel_timeout
            if resiliency is not None:
                kwargs["resiliency"] = resiliency
            if secondary_servers is not None:
                kwargs["secondary_servers"] = secondary_servers
            if server_progress_timeout is not None:
                kwargs["server_progress_timeout"] = server_progress_timeout
            if ssl_option is not None:
                kwargs["ssl_option"] = ssl_option
            if status_request_interval is not None:
                kwargs["status_request_interval"] = status_request_interval
            if svm is not None:
                kwargs["svm"] = svm
            if type is not None:
                kwargs["type"] = type

            resource = FpolicyEngine(
                svm_uuid,
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create FpolicyEngine: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a specific FPolicy engine configuration of an SVM. Modification of an FPolicy engine that is attached to one or more enabled FPolicy policies is not allowed.
### Related ONTAP commands
* `fpolicy policy external-engine modify`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fpolicy engine modify")
        async def fpolicy_engine_modify(
            svm_uuid,
            format: str = None,
            query_format: str = None,
            keep_alive_interval: str = None,
            query_keep_alive_interval: str = None,
            max_server_requests: Size = None,
            query_max_server_requests: Size = None,
            name: str = None,
            query_name: str = None,
            port: Size = None,
            query_port: Size = None,
            primary_servers: dict = None,
            query_primary_servers: dict = None,
            request_abort_timeout: str = None,
            query_request_abort_timeout: str = None,
            request_cancel_timeout: str = None,
            query_request_cancel_timeout: str = None,
            secondary_servers: dict = None,
            query_secondary_servers: dict = None,
            server_progress_timeout: str = None,
            query_server_progress_timeout: str = None,
            ssl_option: str = None,
            query_ssl_option: str = None,
            status_request_interval: str = None,
            query_status_request_interval: str = None,
            type: str = None,
            query_type: str = None,
        ) -> ResourceTable:
            """Modify an instance of a FpolicyEngine resource

            Args:
                format: The format for the notification messages sent to the FPolicy servers.   The possible values are:     * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.     * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form. 
                query_format: The format for the notification messages sent to the FPolicy servers.   The possible values are:     * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.     * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form. 
                keep_alive_interval: Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.
                query_keep_alive_interval: Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.
                max_server_requests: Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.
                query_max_server_requests: Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.
                name: Specifies the name to assign to the external server configuration.
                query_name: Specifies the name to assign to the external server configuration.
                port: Port number of the FPolicy server application.
                query_port: Port number of the FPolicy server application.
                primary_servers: 
                query_primary_servers: 
                request_abort_timeout: Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.
                query_request_abort_timeout: Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.
                request_cancel_timeout: Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.
                query_request_cancel_timeout: Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.
                secondary_servers: 
                query_secondary_servers: 
                server_progress_timeout: Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.
                query_server_progress_timeout: Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.
                ssl_option: Specifies the SSL option for external communication with the FPolicy server. Possible values include the following: * no_auth       When set to \"no_auth\", no authentication takes place. * server_auth   When set to \"server_auth\", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate. * mutual_auth   When set to \"mutual_auth\", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM. 
                query_ssl_option: Specifies the SSL option for external communication with the FPolicy server. Possible values include the following: * no_auth       When set to \"no_auth\", no authentication takes place. * server_auth   When set to \"server_auth\", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate. * mutual_auth   When set to \"mutual_auth\", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM. 
                status_request_interval: Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.
                query_status_request_interval: Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.
                type: The notification mode determines what ONTAP does after sending notifications to FPolicy servers.   The possible values are:     * synchronous  - After sending a notification, wait for a response from the FPolicy server.     * asynchronous - After sending a notification, file request processing continues. 
                query_type: The notification mode determines what ONTAP does after sending notifications to FPolicy servers.   The possible values are:     * synchronous  - After sending a notification, wait for a response from the FPolicy server.     * asynchronous - After sending a notification, file request processing continues. 
            """

            kwargs = {}
            changes = {}
            if query_format is not None:
                kwargs["format"] = query_format
            if query_keep_alive_interval is not None:
                kwargs["keep_alive_interval"] = query_keep_alive_interval
            if query_max_server_requests is not None:
                kwargs["max_server_requests"] = query_max_server_requests
            if query_name is not None:
                kwargs["name"] = query_name
            if query_port is not None:
                kwargs["port"] = query_port
            if query_primary_servers is not None:
                kwargs["primary_servers"] = query_primary_servers
            if query_request_abort_timeout is not None:
                kwargs["request_abort_timeout"] = query_request_abort_timeout
            if query_request_cancel_timeout is not None:
                kwargs["request_cancel_timeout"] = query_request_cancel_timeout
            if query_secondary_servers is not None:
                kwargs["secondary_servers"] = query_secondary_servers
            if query_server_progress_timeout is not None:
                kwargs["server_progress_timeout"] = query_server_progress_timeout
            if query_ssl_option is not None:
                kwargs["ssl_option"] = query_ssl_option
            if query_status_request_interval is not None:
                kwargs["status_request_interval"] = query_status_request_interval
            if query_type is not None:
                kwargs["type"] = query_type

            if format is not None:
                changes["format"] = format
            if keep_alive_interval is not None:
                changes["keep_alive_interval"] = keep_alive_interval
            if max_server_requests is not None:
                changes["max_server_requests"] = max_server_requests
            if name is not None:
                changes["name"] = name
            if port is not None:
                changes["port"] = port
            if primary_servers is not None:
                changes["primary_servers"] = primary_servers
            if request_abort_timeout is not None:
                changes["request_abort_timeout"] = request_abort_timeout
            if request_cancel_timeout is not None:
                changes["request_cancel_timeout"] = request_cancel_timeout
            if secondary_servers is not None:
                changes["secondary_servers"] = secondary_servers
            if server_progress_timeout is not None:
                changes["server_progress_timeout"] = server_progress_timeout
            if ssl_option is not None:
                changes["ssl_option"] = ssl_option
            if status_request_interval is not None:
                changes["status_request_interval"] = status_request_interval
            if type is not None:
                changes["type"] = type

            if hasattr(FpolicyEngine, "find"):
                resource = FpolicyEngine.find(
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = FpolicyEngine(svm_uuid,)
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify FpolicyEngine: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the FPolicy external engine configuration. Deletion of an FPolicy engine that is attached to one or more FPolicy policies is not allowed.
### Related ONTAP commands
* `fpolicy policy external-engine modify`
### Learn more
* [`DOC /protocols/fpolicy/{svm.uuid}/engines`](#docs-NAS-protocols_fpolicy_{svm.uuid}_engines)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="fpolicy engine delete")
        async def fpolicy_engine_delete(
            svm_uuid,
            format: str = None,
            keep_alive_interval: str = None,
            max_server_requests: Size = None,
            name: str = None,
            port: Size = None,
            primary_servers: dict = None,
            request_abort_timeout: str = None,
            request_cancel_timeout: str = None,
            secondary_servers: dict = None,
            server_progress_timeout: str = None,
            ssl_option: str = None,
            status_request_interval: str = None,
            type: str = None,
        ) -> None:
            """Delete an instance of a FpolicyEngine resource

            Args:
                format: The format for the notification messages sent to the FPolicy servers.   The possible values are:     * xml  - Notifications sent to the FPolicy server will be formatted using the XML schema.     * protobuf - Notifications sent to the FPolicy server will be formatted using Protobuf schema, which is a binary form. 
                keep_alive_interval: Specifies the ISO-8601 interval time for a storage appliance to send Keep Alive message to an FPolicy server. The allowed range is between 10 to 600 seconds.
                max_server_requests: Specifies the maximum number of outstanding requests for the FPolicy server. It is used to specify maximum outstanding requests that will be queued up for the FPolicy server. The value for this field must be between 1 and 10000.  The default values are 500, 1000 or 2000 for Low-end(<64 GB memory), Mid-end(>=64 GB memory) and High-end(>=128 GB memory) Platforms respectively.
                name: Specifies the name to assign to the external server configuration.
                port: Port number of the FPolicy server application.
                primary_servers: 
                request_abort_timeout: Specifies the ISO-8601 timeout duration for a screen request to be aborted by a storage appliance. The allowed range is between 0 to 200 seconds.
                request_cancel_timeout: Specifies the ISO-8601 timeout duration for a screen request to be processed by an FPolicy server. The allowed range is between 0 to 100 seconds.
                secondary_servers: 
                server_progress_timeout: Specifies the ISO-8601 timeout duration in which a throttled FPolicy server must complete at least one screen request. If no request is processed within the timeout, connection to the FPolicy server is terminated. The allowed range is between 0 to 100 seconds.
                ssl_option: Specifies the SSL option for external communication with the FPolicy server. Possible values include the following: * no_auth       When set to \"no_auth\", no authentication takes place. * server_auth   When set to \"server_auth\", only the FPolicy server is authenticated by the SVM. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate. * mutual_auth   When set to \"mutual_auth\", mutual authentication takes place between the SVM and the FPolicy server. This means authentication of the FPolicy server by the SVM along with authentication of the SVM by the FPolicy server. With this option, before creating the FPolicy external engine, the administrator must install the public certificate of the certificate authority (CA) that signed the FPolicy server certificate along with the public certificate and key file for authentication of the SVM. 
                status_request_interval: Specifies the ISO-8601 interval time for a storage appliance to query a status request from an FPolicy server. The allowed range is between 0 to 50 seconds.
                type: The notification mode determines what ONTAP does after sending notifications to FPolicy servers.   The possible values are:     * synchronous  - After sending a notification, wait for a response from the FPolicy server.     * asynchronous - After sending a notification, file request processing continues. 
            """

            kwargs = {}
            if format is not None:
                kwargs["format"] = format
            if keep_alive_interval is not None:
                kwargs["keep_alive_interval"] = keep_alive_interval
            if max_server_requests is not None:
                kwargs["max_server_requests"] = max_server_requests
            if name is not None:
                kwargs["name"] = name
            if port is not None:
                kwargs["port"] = port
            if primary_servers is not None:
                kwargs["primary_servers"] = primary_servers
            if request_abort_timeout is not None:
                kwargs["request_abort_timeout"] = request_abort_timeout
            if request_cancel_timeout is not None:
                kwargs["request_cancel_timeout"] = request_cancel_timeout
            if secondary_servers is not None:
                kwargs["secondary_servers"] = secondary_servers
            if server_progress_timeout is not None:
                kwargs["server_progress_timeout"] = server_progress_timeout
            if ssl_option is not None:
                kwargs["ssl_option"] = ssl_option
            if status_request_interval is not None:
                kwargs["status_request_interval"] = status_request_interval
            if type is not None:
                kwargs["type"] = type

            if hasattr(FpolicyEngine, "find"):
                resource = FpolicyEngine.find(
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = FpolicyEngine(svm_uuid,)
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete FpolicyEngine: %s" % err)


