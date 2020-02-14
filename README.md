# Nutanix Objects Scripts

These are scripts that are handy to have for Nutanix Objects

### qupload.py

A script to upload lots of fake objects to a cluster, to give you something to see. You can upload objects up to 10M in size. `objsize` is in KB.

```
python qupload.py --endpoint http://10.38.2.53 --access_key 5RPoaecsV2Yq7Jf_1A0lWQFmwH6x0wcj --secret_key B83e-vUxwJPfjkMIup_oxww5DR40f_wb --bucket test-bucket6 --objsize 10240 --workload write,copy --parallel_threads 100
```

### REST calls

**Status (Objects Enabled or not)**
```
curl -k https://127.0.0.1:9440/api/nutanix/v3/services/oss/status -u admin:Nutanix.123
```

**Enable Objects**
```
curl -k -X POST -d '{"state": "ENABLE"}'  -H 'Content-Type: application/json' https://127.0.0.1:9440/api/nutanix/v3/services/oss  -u admin:Nutanix.123
```

**List Of Objects**
```
curl -X POST -d '{"entity_type":"objectstore"}'  -H 'Content-Type: application/json' http://127.0.0.1:7301/api/nutanix/v3/groups
```

**Deploying Object cluster**
```
curl -X POST -d '{"verify":false,"api_version":"3.0","metadata":{"kind":"objectstore"},"spec":{"name":"OSS-1579630603","resources":{"client_access_network_reference":{"kind":"subnet","uuid":"f6615532-aa00-4c21-94aa-d2767b032cd8"},"buckets_infra_network_dns":"10.45.50.75","domain":"buckets.nutanix.com","buckets_infra_network_vip":"10.45.50.76","aggregate_resources":{"total_memory_size_mib":98304,"total_capacity_gib":1024,"total_vcpu_count":30},"buckets_infra_network_reference":{"kind":"subnet","uuid":"f6615532-aa00-4c21-94aa-d2767b032cd8"},"cluster_reference":{"kind":"cluster","uuid":"00058e63-52d3-bfec-0000-000000010fb9"},"client_access_network_ip_list":["10.45.50.80","10.45.50.84","10.45.50.85","10.45.50.86"]},"description":"Test OSS deployment."},"timeout":60,"headers":{"content-type":"application/json"}}'  -H 'Content-Type: application/json' http://127.0.0.1:7301/api/nutanix/v3/objectstores
```

### Staging Script

This script installs A LOT of stuff:

```
curl --remote-name --location https://raw.githubusercontent.com/jncox/stageworkshop/master/bootstrap.sh && sh ${_##*/}
```

### Generate Splunk Stuff

Credit: James Brown, Nutanix

Deploy `TA-Nutanix.zip` to a Splunk server. Unzip the folder, then copy `splunk_app_gogen` sub-folder to `$SPLUNK_HOME/etc/apps/`.

In Splunk, you can then go to Apps and enable GoGen. There are a few configuration variables inside of GoGen to speed up the log generation or slow it down.
