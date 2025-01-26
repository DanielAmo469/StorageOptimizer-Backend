r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Amazon Web Services Key Management Services (AWS KMS) is a cloud key
management service (KMS) that provides a secure store for secrets. This
feature allows ONTAP to securely store its encryption keys using AWS KMS.
In order to use AWS KMS with ONTAP, you must first create a
Customer Master Key (CMK) in AWS KMS and provide an Access Key ID and
Secret Access Key for a user that has appropriate access to the newly
created CMK in the AWS KMS."
## Examples
### Enabling AWS KMS for an SVM
The following example shows how to enable AWS KMS at the SVM-scope. Note the <i>return_records=true</i> query parameter is used to obtain the newly created key manager configuration.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms()
    resource.svm = {"uuid": "f36ff553-e713-11ea-bd56-005056bb4222"}
    resource.region = "us-east-1"
    resource.key_id = "kmip-aws"
    resource.access_key_id = "AK7ATC35ZXU6GKUDQURT"
    resource.secret_access_key = "Ahrut-#ghty5-881Ht"
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
AwsKms(
    {
        "key_id": "kmip-aws",
        "svm": {"name": "vs0", "uuid": "f36ff553-e713-11ea-bd56-005056bb4222"},
        "access_key_id": "AK7ATC35ZXU6GKUDQURT",
        "_links": {
            "self": {
                "href": "/api/security/aws-kms/f72098a2-e908-11ea-bd56-005056bb4222"
            }
        },
        "region": "us-east-1",
        "uuid": "f72098a2-e908-11ea-bd56-005056bb4222",
    }
)

```
</div>
</div>

---
### Retrieving all AWS KMS configurations
The following example shows how to retrieve all AWS KMS configurations.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(AwsKms.get_collection(fields="*")))

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[
    AwsKms(
        {
            "scope": "svm",
            "key_id": "kmip-aws",
            "svm": {"name": "vs0", "uuid": "f36ff553-e713-11ea-bd56-005056bb4222"},
            "default_domain": "amazonaws.com",
            "access_key_id": "AK7ATC35ZXU6GKUDQURT",
            "service": "KMS",
            "_links": {
                "self": {
                    "href": "/api/security/aws-kms/f72098a2-e908-11ea-bd56-005056bb4222"
                }
            },
            "polling_period": 60,
            "region": "us-east-1",
            "timeout": 10,
            "uuid": "f72098a2-e908-11ea-bd56-005056bb4222",
        }
    )
]

```
</div>
</div>

---
### Retrieving a specific AWS KMS configuration
The following example shows how to retrieve information for a specific AWS KMS configuration.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="f72098a2-e908-11ea-bd56-005056bb4222")
    resource.get(fields="*")
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
AwsKms(
    {
        "scope": "svm",
        "key_id": "kmip-aws",
        "svm": {"name": "vs0", "uuid": "f36ff553-e713-11ea-bd56-005056bb4222"},
        "default_domain": "amazonaws.com",
        "access_key_id": "AK7ATC35ZXU6GKUDQURT",
        "service": "KMS",
        "_links": {
            "self": {
                "href": "/api/security/aws-kms/f72098a2-e908-11ea-bd56-005056bb4222"
            }
        },
        "polling_period": 60,
        "region": "us-east-1",
        "timeout": 10,
        "uuid": "f72098a2-e908-11ea-bd56-005056bb4222",
    }
)

```
</div>
</div>

---
### Retrieving the advanced properties of an AWS configured for a specific SVM
These values are not retrieved by default with the 'fields=*' option.
The following example retrieves the advanced properties of a configured AWS for a specific SVM; there is an added computational cost in retrieving their values. The properties are not populated for either a collection GET or an instance GET unless they are explicitly requested using the `fields` query parameter or GET for all advanced properties is enabled.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="7052c6c0-a503-11ec-a68f-005056ac75a0")
    resource.get(fields="state,amazon_reachability,ekmip_reachability")
    print(resource)

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
AwsKms(
    {
        "amazon_reachability": {"code": "0", "reachable": True, "message": ""},
        "state": {"code": "0", "cluster_state": True, "message": ""},
        "ekmip_reachability": [
            {
                "reachable": True,
                "code": "0",
                "node": {
                    "name": "node1",
                    "_links": {
                        "self": {
                            "href": "/api/cluster/nodes/817f544f-a98d-11ec-ae20-005056ac7c32"
                        }
                    },
                    "uuid": "817f544f-a98d-11ec-ae20-005056ac7c32",
                },
                "message": "",
            },
            {
                "reachable": True,
                "code": "0",
                "node": {
                    "name": "node2",
                    "_links": {
                        "self": {
                            "href": "/api/cluster/nodes/84b3f5f3-a98d-11ec-9ff4-005056acfbfe"
                        }
                    },
                    "uuid": "84b3f5f3-a98d-11ec-9ff4-005056acfbfe",
                },
                "message": "",
            },
        ],
        "_links": {
            "self": {
                "href": "/api/security/aws-kms/d70efc34-aa13-11ec-a059-005056ac7c32"
            }
        },
        "uuid": "d70efc34-aa13-11ec-a059-005056ac7c32",
    }
)

```
</div>
</div>

---
### Updating the "access_key_id" of a specific AWS KMS configuration
The following example shows how to update the "access_key_id" for a specific AWS KMS configuration.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="f72098a2-e908-11ea-bd56-005056bb4222")
    resource.access_key_id = "AK7ATC35ZXU6GKUDQURT"
    resource.secret_access_key = "Ahrut-#ghty5-881Ht"
    resource.patch()

```

---
### Updating a specific AWS KMS configuration to allow it to use a proxy.
The following example shows how to update a specific AWS KMS configuration to allow the AWS KMS instance to use a proxy.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="f72098a2-e908-11ea-bd56-005056bb4222")
    resource.default_domain = "216.9"
    resource.host = "172.20.216.9"
    resource.port = 8000
    resource.service = "10"
    resource.verify_host = False
    resource.verify_ip = False
    resource.patch()

```

---
### Deleting a specific AWS KMS configuration
The following example shows how to delete a specific AWS KMS configuration.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="f72098a2-e908-11ea-bd56-005056bb4222")
    resource.delete()

```

---
### Restoring keys from a KMIP server
The following example shows how to restore keys for a AWS KMS configuration.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import AwsKms

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = AwsKms(uuid="33820b57-ec90-11ea-875e-005056bbf3f0")
    resource.restore()

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


__all__ = ["AwsKms", "AwsKmsSchema"]
__pdoc__ = {
    "AwsKmsSchema.resource": False,
    "AwsKmsSchema.opts": False,
    "AwsKms.aws_kms_show": False,
    "AwsKms.aws_kms_create": False,
    "AwsKms.aws_kms_modify": False,
    "AwsKms.aws_kms_delete": False,
}


class AwsKmsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AwsKms object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the aws_kms."""

    access_key_id = fields.Str(
        data_key="access_key_id",
    )
    r""" AWS Access Key ID of the user that has appropriate access to AWS KMS.

Example: AKIATCFKZXU6GKUDQUWN"""

    amazon_reachability = fields.Nested("netapp_ontap.models.aws_connectivity.AwsConnectivitySchema", data_key="amazon_reachability", unknown=EXCLUDE)
    r""" The amazon_reachability field of the aws_kms."""

    default_domain = fields.Str(
        data_key="default_domain",
    )
    r""" AWS KMS default domain.

Example: domainName"""

    ekmip_reachability = fields.List(fields.Nested("netapp_ontap.models.ekmip_server_connectivity.EkmipServerConnectivitySchema", unknown=EXCLUDE), data_key="ekmip_reachability")
    r""" The ekmip_reachability field of the aws_kms."""

    encryption_context = fields.Str(
        data_key="encryption_context",
    )
    r""" Additional layer of authentication and logging.

Example: aws:fsx:fs-id=fs-0785c8beceb895999"""

    host = fields.Str(
        data_key="host",
    )
    r""" AWS KMS host's hostname.

Example: aws-host.host.com"""

    key_id = fields.Str(
        data_key="key_id",
    )
    r""" AWS Key ID.

Example: kmip-aws"""

    polling_period = Size(
        data_key="polling_period",
    )
    r""" Polling period in minutes.

Example: 55"""

    port = Size(
        data_key="port",
    )
    r""" AWS KMS port.

Example: 443"""

    proxy_host = fields.Str(
        data_key="proxy_host",
    )
    r""" Proxy host.

Example: proxy.eng.com"""

    proxy_password = fields.Str(
        data_key="proxy_password",
    )
    r""" Proxy password. Password is not audited.

Example: awskze-Jwjje2-WJJPer"""

    proxy_port = Size(
        data_key="proxy_port",
    )
    r""" Proxy port.

Example: 1234"""

    proxy_type = fields.Str(
        data_key="proxy_type",
        validate=enum_validation(['http', 'https']),
    )
    r""" Proxy type.

Valid choices:

* http
* https"""

    proxy_username = fields.Str(
        data_key="proxy_username",
    )
    r""" Proxy username.

Example: proxyuser"""

    region = fields.Str(
        data_key="region",
    )
    r""" AWS region of the AWS KMS.

Example: us-east-1"""

    scope = fields.Str(
        data_key="scope",
        validate=enum_validation(['svm', 'cluster']),
    )
    r""" Set to "svm" for interfaces owned by an SVM. Otherwise, set to "cluster".

Valid choices:

* svm
* cluster"""

    secret_access_key = fields.Str(
        data_key="secret_access_key",
    )
    r""" AWS Secret Access Key for the provided access key ID.

Example: tyb1kA/bSr5654LncZNwNcvAOR7hu1vPo0nPrtoG"""

    service = fields.Str(
        data_key="service",
    )
    r""" AWS service type.

Example: dynamodb.*.amazonaws.com"""

    skip_verify = fields.Boolean(
        data_key="skip_verify",
    )
    r""" Set to true to bypass verification of the user provided access_key_id
and secret_access_key. An error will be returned if 'skip_verify' is
provided but 'access_key_id' is not.


Example: false"""

    state = fields.Nested("netapp_ontap.models.aws_kms_state.AwsKmsStateSchema", data_key="state", unknown=EXCLUDE)
    r""" The state field of the aws_kms."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the aws_kms."""

    timeout = Size(
        data_key="timeout",
    )
    r""" AWS Connection timeout, in seconds.

Example: 20"""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" A unique identifier for the AWS KMS.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412"""

    verify = fields.Boolean(
        data_key="verify",
    )
    r""" Set to true to verify the AWS KMS host.

Example: false"""

    verify_host = fields.Boolean(
        data_key="verify_host",
    )
    r""" Set to true to verify the AWS KMS host's hostname.

Example: true"""

    verify_ip = fields.Boolean(
        data_key="verify_ip",
    )
    r""" Set to true to verify the AWS KMS host's IP address.

Example: false"""

    @property
    def resource(self):
        return AwsKms

    gettable_fields = [
        "links",
        "access_key_id",
        "amazon_reachability",
        "default_domain",
        "ekmip_reachability",
        "encryption_context",
        "host",
        "key_id",
        "polling_period",
        "port",
        "proxy_host",
        "proxy_port",
        "proxy_type",
        "proxy_username",
        "region",
        "scope",
        "service",
        "skip_verify",
        "state",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "timeout",
        "uuid",
        "verify",
        "verify_host",
        "verify_ip",
    ]
    """links,access_key_id,amazon_reachability,default_domain,ekmip_reachability,encryption_context,host,key_id,polling_period,port,proxy_host,proxy_port,proxy_type,proxy_username,region,scope,service,skip_verify,state,svm.links,svm.name,svm.uuid,timeout,uuid,verify,verify_host,verify_ip,"""

    patchable_fields = [
        "access_key_id",
        "default_domain",
        "encryption_context",
        "host",
        "key_id",
        "polling_period",
        "port",
        "proxy_host",
        "proxy_password",
        "proxy_port",
        "proxy_type",
        "proxy_username",
        "region",
        "secret_access_key",
        "service",
        "skip_verify",
        "svm.name",
        "svm.uuid",
        "timeout",
        "verify",
        "verify_host",
        "verify_ip",
    ]
    """access_key_id,default_domain,encryption_context,host,key_id,polling_period,port,proxy_host,proxy_password,proxy_port,proxy_type,proxy_username,region,secret_access_key,service,skip_verify,svm.name,svm.uuid,timeout,verify,verify_host,verify_ip,"""

    postable_fields = [
        "access_key_id",
        "default_domain",
        "encryption_context",
        "host",
        "key_id",
        "polling_period",
        "port",
        "proxy_host",
        "proxy_password",
        "proxy_port",
        "proxy_type",
        "proxy_username",
        "region",
        "secret_access_key",
        "service",
        "skip_verify",
        "svm.name",
        "svm.uuid",
        "timeout",
        "verify",
        "verify_host",
        "verify_ip",
    ]
    """access_key_id,default_domain,encryption_context,host,key_id,polling_period,port,proxy_host,proxy_password,proxy_port,proxy_type,proxy_username,region,secret_access_key,service,skip_verify,svm.name,svm.uuid,timeout,verify,verify_host,verify_ip,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in AwsKms.get_collection(fields=field)]
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
            raise NetAppRestError("AwsKms modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class AwsKms(Resource):
    """Allows interaction with AwsKms objects on the host"""

    _schema = AwsKmsSchema
    _path = "/api/security/aws-kms"
    _keys = ["uuid"]
    _action_form_data_parameters = { 'file':'file', }

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves all AWS KMS instances configured for all clusters and SVMs.
### Related ONTAP commands
* `security key-manager external aws show`
* `security key-manager external aws check`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="aws kms show")
        def aws_kms_show(
            fields: List[Choices.define(["access_key_id", "default_domain", "encryption_context", "host", "key_id", "polling_period", "port", "proxy_host", "proxy_password", "proxy_port", "proxy_type", "proxy_username", "region", "scope", "secret_access_key", "service", "skip_verify", "timeout", "uuid", "verify", "verify_host", "verify_ip", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of AwsKms resources

            Args:
                access_key_id: AWS Access Key ID of the user that has appropriate access to AWS KMS.
                default_domain: AWS KMS default domain.
                encryption_context: Additional layer of authentication and logging.
                host: AWS KMS host's hostname.
                key_id: AWS Key ID.
                polling_period: Polling period in minutes.
                port: AWS KMS port.
                proxy_host: Proxy host.
                proxy_password: Proxy password. Password is not audited.
                proxy_port: Proxy port.
                proxy_type: Proxy type.
                proxy_username: Proxy username.
                region: AWS region of the AWS KMS.
                scope: Set to \"svm\" for interfaces owned by an SVM. Otherwise, set to \"cluster\".
                secret_access_key: AWS Secret Access Key for the provided access key ID.
                service: AWS service type.
                skip_verify: Set to true to bypass verification of the user provided access_key_id and secret_access_key. An error will be returned if 'skip_verify' is provided but 'access_key_id' is not. 
                timeout: AWS Connection timeout, in seconds.
                uuid: A unique identifier for the AWS KMS.
                verify: Set to true to verify the AWS KMS host.
                verify_host: Set to true to verify the AWS KMS host's hostname.
                verify_ip: Set to true to verify the AWS KMS host's IP address.
            """

            kwargs = {}
            if access_key_id is not None:
                kwargs["access_key_id"] = access_key_id
            if default_domain is not None:
                kwargs["default_domain"] = default_domain
            if encryption_context is not None:
                kwargs["encryption_context"] = encryption_context
            if host is not None:
                kwargs["host"] = host
            if key_id is not None:
                kwargs["key_id"] = key_id
            if polling_period is not None:
                kwargs["polling_period"] = polling_period
            if port is not None:
                kwargs["port"] = port
            if proxy_host is not None:
                kwargs["proxy_host"] = proxy_host
            if proxy_password is not None:
                kwargs["proxy_password"] = proxy_password
            if proxy_port is not None:
                kwargs["proxy_port"] = proxy_port
            if proxy_type is not None:
                kwargs["proxy_type"] = proxy_type
            if proxy_username is not None:
                kwargs["proxy_username"] = proxy_username
            if region is not None:
                kwargs["region"] = region
            if scope is not None:
                kwargs["scope"] = scope
            if secret_access_key is not None:
                kwargs["secret_access_key"] = secret_access_key
            if service is not None:
                kwargs["service"] = service
            if skip_verify is not None:
                kwargs["skip_verify"] = skip_verify
            if timeout is not None:
                kwargs["timeout"] = timeout
            if uuid is not None:
                kwargs["uuid"] = uuid
            if verify is not None:
                kwargs["verify"] = verify
            if verify_host is not None:
                kwargs["verify_host"] = verify_host
            if verify_ip is not None:
                kwargs["verify_ip"] = verify_ip
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return AwsKms.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all AwsKms resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["AwsKms"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the AWS KMS configuration.
### Optional properties
* `region` - AWS region of the AWS KMS.
* `service` - AWS service type.
* `default_domain` - AWS KMS default domain.
* `port` - AWS KMS port.
* `proxy_type` - Type of proxy (http, https, etc.), if proxy configuration is used.
* `proxy_host` - Proxy hostname if proxy configuration is used.
* `proxy_port` - Proxy port number if proxy configuration is used.
* `proxy_username` - Proxy username if proxy configuration is used.
* `proxy_password` - Proxy password if proxy configuration is used.
* `polling_period` - Polling period in minutes.
* `timeout` - AWS Connection timeout, in seconds.
* `verify` - Set to true to verify the AWS KMS host.
* `verify_host` - Set to true to verify the AWS KMS host's hostname.
* `verify_ip` - Set to true to verify the AWS KMS host's IP address.
* `host` - AWS KMS host's hostname.
* `secret_access_key` - AWS secret access key for the access key ID provided.
* `access-key-id` - AWS access key ID of the user with the appropriate access to AWS KMS.
* `skip_verify` - Set to true to bypass verification of the user provided access_key_id and secret_access_key.
* `encryption_context` - Additional layer of authentication and logging.
### Related ONTAP commands
* `security key-manager external aws update-config`
* `security key-manager external aws update-credentials`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["AwsKms"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["AwsKms"], NetAppResponse]:
        r"""Configures the AWS KMS configuration for the specified SVM.
### Required properties
* `svm.uuid` or `svm.name` - Existing SVM in which to create an AWS KMS.
* `region` - AWS region of the AWS KMS.
* `key_id` - AWS Key ID
### Optional properties
* `access_key_id` - AWS access key ID of the user who has the appropriate access to AWS KMS.
* `secret_access_key` - AWS secret access key for the access key ID provided.
* `service` - AWS service type.
* `default_domain` - AWS KMS default domain.
* `port` - AWS KMS port.
* `proxy_type` - Type of proxy (http, https, etc.), if proxy configuration is used.
* `proxy_host` - Proxy hostname if proxy configuration is used.
* `proxy_port` - Proxy port number if proxy configuration is used.
* `proxy_username` - Proxy username if proxy configuration is used.
* `proxy_password` - Proxy password if proxy configuration is used.
* `polling_period` - Polling period in minutes.
* `encryption_context` - Additional layer of authentication and logging.
### Related ONTAP commands
* `security key-manager external aws enable`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["AwsKms"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an AWS KMS configuration.
### Related ONTAP commands
* `security key-manager external aws disable`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves all AWS KMS instances configured for all clusters and SVMs.
### Related ONTAP commands
* `security key-manager external aws show`
* `security key-manager external aws check`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the AWS KMS configuration for the SVM specified by the UUID.
### Related ONTAP commands
* `security key-manager external aws show`
* `security key-manager external aws check`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
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
        r"""Configures the AWS KMS configuration for the specified SVM.
### Required properties
* `svm.uuid` or `svm.name` - Existing SVM in which to create an AWS KMS.
* `region` - AWS region of the AWS KMS.
* `key_id` - AWS Key ID
### Optional properties
* `access_key_id` - AWS access key ID of the user who has the appropriate access to AWS KMS.
* `secret_access_key` - AWS secret access key for the access key ID provided.
* `service` - AWS service type.
* `default_domain` - AWS KMS default domain.
* `port` - AWS KMS port.
* `proxy_type` - Type of proxy (http, https, etc.), if proxy configuration is used.
* `proxy_host` - Proxy hostname if proxy configuration is used.
* `proxy_port` - Proxy port number if proxy configuration is used.
* `proxy_username` - Proxy username if proxy configuration is used.
* `proxy_password` - Proxy password if proxy configuration is used.
* `polling_period` - Polling period in minutes.
* `encryption_context` - Additional layer of authentication and logging.
### Related ONTAP commands
* `security key-manager external aws enable`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="aws kms create")
        async def aws_kms_create(
        ) -> ResourceTable:
            """Create an instance of a AwsKms resource

            Args:
                links: 
                access_key_id: AWS Access Key ID of the user that has appropriate access to AWS KMS.
                amazon_reachability: 
                default_domain: AWS KMS default domain.
                ekmip_reachability: 
                encryption_context: Additional layer of authentication and logging.
                host: AWS KMS host's hostname.
                key_id: AWS Key ID.
                polling_period: Polling period in minutes.
                port: AWS KMS port.
                proxy_host: Proxy host.
                proxy_password: Proxy password. Password is not audited.
                proxy_port: Proxy port.
                proxy_type: Proxy type.
                proxy_username: Proxy username.
                region: AWS region of the AWS KMS.
                scope: Set to \"svm\" for interfaces owned by an SVM. Otherwise, set to \"cluster\".
                secret_access_key: AWS Secret Access Key for the provided access key ID.
                service: AWS service type.
                skip_verify: Set to true to bypass verification of the user provided access_key_id and secret_access_key. An error will be returned if 'skip_verify' is provided but 'access_key_id' is not. 
                state: 
                svm: 
                timeout: AWS Connection timeout, in seconds.
                uuid: A unique identifier for the AWS KMS.
                verify: Set to true to verify the AWS KMS host.
                verify_host: Set to true to verify the AWS KMS host's hostname.
                verify_ip: Set to true to verify the AWS KMS host's IP address.
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if access_key_id is not None:
                kwargs["access_key_id"] = access_key_id
            if amazon_reachability is not None:
                kwargs["amazon_reachability"] = amazon_reachability
            if default_domain is not None:
                kwargs["default_domain"] = default_domain
            if ekmip_reachability is not None:
                kwargs["ekmip_reachability"] = ekmip_reachability
            if encryption_context is not None:
                kwargs["encryption_context"] = encryption_context
            if host is not None:
                kwargs["host"] = host
            if key_id is not None:
                kwargs["key_id"] = key_id
            if polling_period is not None:
                kwargs["polling_period"] = polling_period
            if port is not None:
                kwargs["port"] = port
            if proxy_host is not None:
                kwargs["proxy_host"] = proxy_host
            if proxy_password is not None:
                kwargs["proxy_password"] = proxy_password
            if proxy_port is not None:
                kwargs["proxy_port"] = proxy_port
            if proxy_type is not None:
                kwargs["proxy_type"] = proxy_type
            if proxy_username is not None:
                kwargs["proxy_username"] = proxy_username
            if region is not None:
                kwargs["region"] = region
            if scope is not None:
                kwargs["scope"] = scope
            if secret_access_key is not None:
                kwargs["secret_access_key"] = secret_access_key
            if service is not None:
                kwargs["service"] = service
            if skip_verify is not None:
                kwargs["skip_verify"] = skip_verify
            if state is not None:
                kwargs["state"] = state
            if svm is not None:
                kwargs["svm"] = svm
            if timeout is not None:
                kwargs["timeout"] = timeout
            if uuid is not None:
                kwargs["uuid"] = uuid
            if verify is not None:
                kwargs["verify"] = verify
            if verify_host is not None:
                kwargs["verify_host"] = verify_host
            if verify_ip is not None:
                kwargs["verify_ip"] = verify_ip

            resource = AwsKms(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create AwsKms: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the AWS KMS configuration.
### Optional properties
* `region` - AWS region of the AWS KMS.
* `service` - AWS service type.
* `default_domain` - AWS KMS default domain.
* `port` - AWS KMS port.
* `proxy_type` - Type of proxy (http, https, etc.), if proxy configuration is used.
* `proxy_host` - Proxy hostname if proxy configuration is used.
* `proxy_port` - Proxy port number if proxy configuration is used.
* `proxy_username` - Proxy username if proxy configuration is used.
* `proxy_password` - Proxy password if proxy configuration is used.
* `polling_period` - Polling period in minutes.
* `timeout` - AWS Connection timeout, in seconds.
* `verify` - Set to true to verify the AWS KMS host.
* `verify_host` - Set to true to verify the AWS KMS host's hostname.
* `verify_ip` - Set to true to verify the AWS KMS host's IP address.
* `host` - AWS KMS host's hostname.
* `secret_access_key` - AWS secret access key for the access key ID provided.
* `access-key-id` - AWS access key ID of the user with the appropriate access to AWS KMS.
* `skip_verify` - Set to true to bypass verification of the user provided access_key_id and secret_access_key.
* `encryption_context` - Additional layer of authentication and logging.
### Related ONTAP commands
* `security key-manager external aws update-config`
* `security key-manager external aws update-credentials`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="aws kms modify")
        async def aws_kms_modify(
        ) -> ResourceTable:
            """Modify an instance of a AwsKms resource

            Args:
                access_key_id: AWS Access Key ID of the user that has appropriate access to AWS KMS.
                query_access_key_id: AWS Access Key ID of the user that has appropriate access to AWS KMS.
                default_domain: AWS KMS default domain.
                query_default_domain: AWS KMS default domain.
                encryption_context: Additional layer of authentication and logging.
                query_encryption_context: Additional layer of authentication and logging.
                host: AWS KMS host's hostname.
                query_host: AWS KMS host's hostname.
                key_id: AWS Key ID.
                query_key_id: AWS Key ID.
                polling_period: Polling period in minutes.
                query_polling_period: Polling period in minutes.
                port: AWS KMS port.
                query_port: AWS KMS port.
                proxy_host: Proxy host.
                query_proxy_host: Proxy host.
                proxy_password: Proxy password. Password is not audited.
                query_proxy_password: Proxy password. Password is not audited.
                proxy_port: Proxy port.
                query_proxy_port: Proxy port.
                proxy_type: Proxy type.
                query_proxy_type: Proxy type.
                proxy_username: Proxy username.
                query_proxy_username: Proxy username.
                region: AWS region of the AWS KMS.
                query_region: AWS region of the AWS KMS.
                scope: Set to \"svm\" for interfaces owned by an SVM. Otherwise, set to \"cluster\".
                query_scope: Set to \"svm\" for interfaces owned by an SVM. Otherwise, set to \"cluster\".
                secret_access_key: AWS Secret Access Key for the provided access key ID.
                query_secret_access_key: AWS Secret Access Key for the provided access key ID.
                service: AWS service type.
                query_service: AWS service type.
                skip_verify: Set to true to bypass verification of the user provided access_key_id and secret_access_key. An error will be returned if 'skip_verify' is provided but 'access_key_id' is not. 
                query_skip_verify: Set to true to bypass verification of the user provided access_key_id and secret_access_key. An error will be returned if 'skip_verify' is provided but 'access_key_id' is not. 
                timeout: AWS Connection timeout, in seconds.
                query_timeout: AWS Connection timeout, in seconds.
                uuid: A unique identifier for the AWS KMS.
                query_uuid: A unique identifier for the AWS KMS.
                verify: Set to true to verify the AWS KMS host.
                query_verify: Set to true to verify the AWS KMS host.
                verify_host: Set to true to verify the AWS KMS host's hostname.
                query_verify_host: Set to true to verify the AWS KMS host's hostname.
                verify_ip: Set to true to verify the AWS KMS host's IP address.
                query_verify_ip: Set to true to verify the AWS KMS host's IP address.
            """

            kwargs = {}
            changes = {}
            if query_access_key_id is not None:
                kwargs["access_key_id"] = query_access_key_id
            if query_default_domain is not None:
                kwargs["default_domain"] = query_default_domain
            if query_encryption_context is not None:
                kwargs["encryption_context"] = query_encryption_context
            if query_host is not None:
                kwargs["host"] = query_host
            if query_key_id is not None:
                kwargs["key_id"] = query_key_id
            if query_polling_period is not None:
                kwargs["polling_period"] = query_polling_period
            if query_port is not None:
                kwargs["port"] = query_port
            if query_proxy_host is not None:
                kwargs["proxy_host"] = query_proxy_host
            if query_proxy_password is not None:
                kwargs["proxy_password"] = query_proxy_password
            if query_proxy_port is not None:
                kwargs["proxy_port"] = query_proxy_port
            if query_proxy_type is not None:
                kwargs["proxy_type"] = query_proxy_type
            if query_proxy_username is not None:
                kwargs["proxy_username"] = query_proxy_username
            if query_region is not None:
                kwargs["region"] = query_region
            if query_scope is not None:
                kwargs["scope"] = query_scope
            if query_secret_access_key is not None:
                kwargs["secret_access_key"] = query_secret_access_key
            if query_service is not None:
                kwargs["service"] = query_service
            if query_skip_verify is not None:
                kwargs["skip_verify"] = query_skip_verify
            if query_timeout is not None:
                kwargs["timeout"] = query_timeout
            if query_uuid is not None:
                kwargs["uuid"] = query_uuid
            if query_verify is not None:
                kwargs["verify"] = query_verify
            if query_verify_host is not None:
                kwargs["verify_host"] = query_verify_host
            if query_verify_ip is not None:
                kwargs["verify_ip"] = query_verify_ip

            if access_key_id is not None:
                changes["access_key_id"] = access_key_id
            if default_domain is not None:
                changes["default_domain"] = default_domain
            if encryption_context is not None:
                changes["encryption_context"] = encryption_context
            if host is not None:
                changes["host"] = host
            if key_id is not None:
                changes["key_id"] = key_id
            if polling_period is not None:
                changes["polling_period"] = polling_period
            if port is not None:
                changes["port"] = port
            if proxy_host is not None:
                changes["proxy_host"] = proxy_host
            if proxy_password is not None:
                changes["proxy_password"] = proxy_password
            if proxy_port is not None:
                changes["proxy_port"] = proxy_port
            if proxy_type is not None:
                changes["proxy_type"] = proxy_type
            if proxy_username is not None:
                changes["proxy_username"] = proxy_username
            if region is not None:
                changes["region"] = region
            if scope is not None:
                changes["scope"] = scope
            if secret_access_key is not None:
                changes["secret_access_key"] = secret_access_key
            if service is not None:
                changes["service"] = service
            if skip_verify is not None:
                changes["skip_verify"] = skip_verify
            if timeout is not None:
                changes["timeout"] = timeout
            if uuid is not None:
                changes["uuid"] = uuid
            if verify is not None:
                changes["verify"] = verify
            if verify_host is not None:
                changes["verify_host"] = verify_host
            if verify_ip is not None:
                changes["verify_ip"] = verify_ip

            if hasattr(AwsKms, "find"):
                resource = AwsKms.find(
                    **kwargs
                )
            else:
                resource = AwsKms()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify AwsKms: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an AWS KMS configuration.
### Related ONTAP commands
* `security key-manager external aws disable`

### Learn more
* [`DOC /security/aws-kms`](#docs-security-security_aws-kms)"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="aws kms delete")
        async def aws_kms_delete(
        ) -> None:
            """Delete an instance of a AwsKms resource

            Args:
                access_key_id: AWS Access Key ID of the user that has appropriate access to AWS KMS.
                default_domain: AWS KMS default domain.
                encryption_context: Additional layer of authentication and logging.
                host: AWS KMS host's hostname.
                key_id: AWS Key ID.
                polling_period: Polling period in minutes.
                port: AWS KMS port.
                proxy_host: Proxy host.
                proxy_password: Proxy password. Password is not audited.
                proxy_port: Proxy port.
                proxy_type: Proxy type.
                proxy_username: Proxy username.
                region: AWS region of the AWS KMS.
                scope: Set to \"svm\" for interfaces owned by an SVM. Otherwise, set to \"cluster\".
                secret_access_key: AWS Secret Access Key for the provided access key ID.
                service: AWS service type.
                skip_verify: Set to true to bypass verification of the user provided access_key_id and secret_access_key. An error will be returned if 'skip_verify' is provided but 'access_key_id' is not. 
                timeout: AWS Connection timeout, in seconds.
                uuid: A unique identifier for the AWS KMS.
                verify: Set to true to verify the AWS KMS host.
                verify_host: Set to true to verify the AWS KMS host's hostname.
                verify_ip: Set to true to verify the AWS KMS host's IP address.
            """

            kwargs = {}
            if access_key_id is not None:
                kwargs["access_key_id"] = access_key_id
            if default_domain is not None:
                kwargs["default_domain"] = default_domain
            if encryption_context is not None:
                kwargs["encryption_context"] = encryption_context
            if host is not None:
                kwargs["host"] = host
            if key_id is not None:
                kwargs["key_id"] = key_id
            if polling_period is not None:
                kwargs["polling_period"] = polling_period
            if port is not None:
                kwargs["port"] = port
            if proxy_host is not None:
                kwargs["proxy_host"] = proxy_host
            if proxy_password is not None:
                kwargs["proxy_password"] = proxy_password
            if proxy_port is not None:
                kwargs["proxy_port"] = proxy_port
            if proxy_type is not None:
                kwargs["proxy_type"] = proxy_type
            if proxy_username is not None:
                kwargs["proxy_username"] = proxy_username
            if region is not None:
                kwargs["region"] = region
            if scope is not None:
                kwargs["scope"] = scope
            if secret_access_key is not None:
                kwargs["secret_access_key"] = secret_access_key
            if service is not None:
                kwargs["service"] = service
            if skip_verify is not None:
                kwargs["skip_verify"] = skip_verify
            if timeout is not None:
                kwargs["timeout"] = timeout
            if uuid is not None:
                kwargs["uuid"] = uuid
            if verify is not None:
                kwargs["verify"] = verify
            if verify_host is not None:
                kwargs["verify_host"] = verify_host
            if verify_ip is not None:
                kwargs["verify_ip"] = verify_ip

            if hasattr(AwsKms, "find"):
                resource = AwsKms.find(
                    **kwargs
                )
            else:
                resource = AwsKms()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete AwsKms: %s" % err)

    def rekey_external(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Rekeys or re-versions the AWS KMS Key Encryption Key (KEK) for the given AWS KMS.
### Related ONTAP commands
* `security key-manager external aws rekey-external`
"""
        return super()._action(
            "rekey-external", body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    rekey_external.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._action.__doc__)
    def rekey_internal(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Rekeys SVM KEK for the given AWS KMS.
### Related ONTAP commands
* `security key-manager external aws rekey-internal`
"""
        return super()._action(
            "rekey-internal", body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    rekey_internal.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._action.__doc__)
    def restore(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Restores the keys for an SVM from a configured AWS KMS.
### Related ONTAP commands
* `security key-manager external AWS restore`
"""
        return super()._action(
            "restore", body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    restore.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._action.__doc__)

