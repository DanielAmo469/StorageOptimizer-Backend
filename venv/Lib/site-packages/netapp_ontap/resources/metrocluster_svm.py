r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

# Overview
Retrieves configuration information for all pairs of SVMs in MetroCluster.
####
---
### Related ONTAP commands

* `metrocluster vserver show`
## Examples
```
GET https://<mgmt-ip>/api/cluster/metrocluster/svms/?fields=*"
{
  "records": [
    {
      "cluster": {
        "uuid": "9623385a-6c4c-11ec-a8cc-005056aca0c8",
        "name": "cluster1"
      },
      "svm": {
        "uuid": "2ea76ca7-6c5f-11ec-b430-005056aca0c8",
        "name": "svm1"
      },
      "partner_svm": {
        "uuid": "2ea76ca7-6c5f-11ec-b430-005056aca0c8",
        "name": "svm1-mc"
      },
      "configuration_state": "healthy",
      "_links": {
        "self": {
          "href": "/api/cluster/metrocluster/svms/9623385a-6c4c-11ec-a8cc-005056aca0c8/2ea76ca7-6c5f-11ec-b430-005056aca0c8"
        }
      }
    },
    {
      "cluster": {
        "uuid": "988d33a0-6c4c-11ec-8e28-005056aceeed",
        "name": "cluster2"
      },
      "svm": {
        "uuid": "2fa16461-6c5f-11ec-8f69-005056aceeed",
        "name": "svm2"
      },
      "partner_svm": {
        "uuid": "2fa16461-6c5f-11ec-8f69-005056aceeed",
        "name": "svm2-mc"
      },
      "configuration_state": "healthy",
      "_links": {
        "self": {
          "href": "/api/cluster/metrocluster/svms/988d33a0-6c4c-11ec-8e28-005056aceeed/2fa16461-6c5f-11ec-8f69-005056aceeed"
        }
      }
    }
  ],
  "num_records": 2,
  "_links": {
    "self": {
      "href": "/api/cluster/metrocluster/svms/?fields=*"
    }
  }
}
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


__all__ = ["MetroclusterSvm", "MetroclusterSvmSchema"]
__pdoc__ = {
    "MetroclusterSvmSchema.resource": False,
    "MetroclusterSvmSchema.opts": False,
    "MetroclusterSvm.metrocluster_svm_show": False,
    "MetroclusterSvm.metrocluster_svm_create": False,
    "MetroclusterSvm.metrocluster_svm_modify": False,
    "MetroclusterSvm.metrocluster_svm_delete": False,
}


class MetroclusterSvmSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterSvm object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the metrocluster_svm."""

    cluster = fields.Nested("netapp_ontap.resources.cluster.ClusterSchema", data_key="cluster", unknown=EXCLUDE)
    r""" The cluster field of the metrocluster_svm."""

    configuration_state = fields.Str(
        data_key="configuration_state",
        validate=enum_validation(['degraded', 'healthy', 'pending_setup', 'pending_switchback', 'replication_paused', 'syncing', 'unhealthy']),
    )
    r""" Configuration state.

Valid choices:

* degraded
* healthy
* pending_setup
* pending_switchback
* replication_paused
* syncing
* unhealthy"""

    failed_reason = fields.Nested("netapp_ontap.models.error.ErrorSchema", data_key="failed_reason", unknown=EXCLUDE)
    r""" Reason for SVM object replication failure."""

    partner_svm = fields.Nested("netapp_ontap.models.metrocluster_svm_partner_svm.MetroclusterSvmPartnerSvmSchema", data_key="partner_svm", unknown=EXCLUDE)
    r""" The partner_svm field of the metrocluster_svm."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the metrocluster_svm."""

    @property
    def resource(self):
        return MetroclusterSvm

    gettable_fields = [
        "links",
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "configuration_state",
        "failed_reason",
        "partner_svm",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """links,cluster.links,cluster.name,cluster.uuid,configuration_state,failed_reason,partner_svm,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "partner_svm",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,partner_svm,svm.name,svm.uuid,"""

    postable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "partner_svm",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,partner_svm,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in MetroclusterSvm.get_collection(fields=field)]
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
            raise NetAppRestError("MetroclusterSvm modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class MetroclusterSvm(Resource):
    r""" Retrieves configuration information for all pairs of SVMs in MetroCluster. REST /api/cluster/metrocluster/svms/? """

    _schema = MetroclusterSvmSchema
    _path = "/api/cluster/metrocluster/svms"
    _keys = ["cluster.uuid", "svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves configuration information for all pairs of SVMs in MetroCluster. REST /api/cluster/metrocluster/svms/?
### Learn more
* [`DOC /cluster/metrocluster/svms`](#docs-cluster-cluster_metrocluster_svms)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="metrocluster svm show")
        def metrocluster_svm_show(
            fields: List[Choices.define(["configuration_state", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of MetroclusterSvm resources

            Args:
                configuration_state: Configuration state.
            """

            kwargs = {}
            if configuration_state is not None:
                kwargs["configuration_state"] = configuration_state
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return MetroclusterSvm.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all MetroclusterSvm resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves configuration information for all pairs of SVMs in MetroCluster. REST /api/cluster/metrocluster/svms/?
### Learn more
* [`DOC /cluster/metrocluster/svms`](#docs-cluster-cluster_metrocluster_svms)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves configuration information for an SVM in a MetroCluster relationship.
### Learn more
* [`DOC /cluster/metrocluster/svms`](#docs-cluster-cluster_metrocluster_svms)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





