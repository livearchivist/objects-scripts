# Nutanix Objects Scripts

These are scripts that are handy to have for Nutanix Objects

### qupload.py

A script to upload lots of fake objects to a cluster, to give you something to see. You can upload objects up to 10M in size. `objsize` is in KB.

```
python qupload.py --endpoint http://10.38.2.53 --access_key 5RPoaecsV2Yq7Jf_1A0lWQFmwH6x0wcj --secret_key B83e-vUxwJPfjkMIup_oxww5DR40f_wb --bucket test-bucket6 --objsize 10240 --workload write,copy --parallel_threads 100
```
