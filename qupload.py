"""
This is just my dirty script to upload few objects. If you need any help, plz
contact me @ anirudh.sonar@nutanix.com

NOTE : This script is not meant for perf measurement. Its not optimized to give
you correct numbers.It can generate some moderate load and thats the purpose of
this script.
"""
import os
import sys
import time
import math
import boto3
import signal
import string
import random
import logging
import hashlib
import datetime
import argparse
import traceback
from Queue import Queue
from threading import Thread, Lock
from botocore.config import Config
from hanging_threads import start_monitoring

def connect(ip="", access=None, secret=None, aws=False, **kwargs):
  if (not ip and not aws) or (ip and (not access or not secret)):
    raise Exception("Endpoint URL or creds are missing")
  session = boto3.session.Session()
  config = Config(connect_timeout=1,
                  max_pool_connections=kwargs.pop("max_pool_connections"),
                  retries={'max_attempts': 0},
                  s3={"addressing_style":"path"})
  params={}
  params["config"] = config
  params["service_name"] = "s3"
  params["aws_access_key_id"] = access
  params["aws_secret_access_key"] = secret
  if ip:
    params["endpoint_url"] = ip
  params["use_ssl"]=False
  params["verify"]=False
  return session.client(**params)

class UploadObjs(object):
  def __init__(self, s3obj, static_data=False, **kwargs):
    self._stats = {}
    self._data = ""
    self._md5 = None
    self.s3obj = s3obj
    self._base64 = None
    self._objnames = []
    self._lock = Lock()
    self._total_size = 0
    self._global_count = 0
    self._bucket_names = []
    self._local_data = None
    self._current_object_count = 0
    self._start_time = time.time()
    self._static_data = static_data
    self._total_failures_recorded = 0
    self._timeout = kwargs.pop("runtime")
    self._retry_count = kwargs.pop("retry_count", 3)
    self._retry_delay = kwargs.pop("retry_delay", 10)
    self._task_manager = None
    self._time_to_exit = False
    self._execution_completed = False
    #self._global_queue = Q

  def start_workload(self, parallel_threads, **kwargs):
    start_monitoring(seconds_frozen=60)
    workload_perc = {}
    workloads = kwargs.pop("workload").split(",")
    wperc = 100/len(workloads)
    for workload in workloads:
      wload = workload.split(":")
      if len(wload) == 1:
        workload_perc[wload[0]] = wperc
        continue
      workload_perc[wload[0]] = int(wload[1])
    quota = {}
    for workload_type, workload_per in workload_perc.iteritems():
      quota[workload_type] = {"perc":workload_per}
    self._task_manager = parallel_execution(parallel_threads, quotas=quota)
    self._prepare_stage(**kwargs)
    self._exit_marker = self._create_exit_marker()
    self._start_and_monitor(workload_perc.keys(), **kwargs)

  def _start_and_monitor(self, workloads, **kwargs):
    self._exit_on_cntr_c()
    self._workload_threads = []
    failures_to_tolerate = kwargs.pop("failures_to_tolerate", -1)
    for workload in workloads:
      thrd = Thread(target=getattr(self, workload), kwargs=kwargs)
      thrd.start()
      thrd.name = "%sCntrThread"%(workload.title())
      self._workload_threads.append(thrd)
    while True:
      if failures_to_tolerate > 0 :
        if self._total_failures_recorded > failures_to_tolerate:
          ERROR("Total failures are above acceptable range. Found vs "\
                "acceptable : %s vs %s"%(self._total_failures_recorded,
                                         failures_to_tolerate))
          self._stop_workload("Too many failures found")
          return
      elif failures_to_tolerate > 0:
        WARN("Total failed tasks : %s"%(failures_to_tolerate))
      if os.path.isfile(self._exit_marker):
        INFO("Exit marker found. Initiating exit")
        self._stop_workload("Exit marker found")
        os.remove(self._exit_marker)
      if self._time_to_exit:
        self._stop_workload("Exit marker found")
        return
      if self._execution_completed:
        INFO("Execution is completed")
        self._stop_workload("Execution Completed")
      time.sleep(10)

  def _exit_on_cntr_c(self):
    def signal_handler(sig, frame):
      WARN('You pressed Ctrl+C!, exiting workload')
      self._stop_workload("Ctrl+C Pressed")
    signal.signal(signal.SIGINT, signal_handler)

  def _create_exit_marker(self):
    filename = "/tmp/%s_exit_marker.txt"%(time.time())
    INFO("Exit marker to exit program : %s"%(filename))
    return filename

  def _stop_workload(self, reason=None):
    INFO("Stopping workload. Reason : %s"%(reason))
    self._time_to_exit = True
    self._task_manager._time_to_exit = True
    self._task_manager.complete_run()

  def _prepare_stage(self, **kwargs):
    if kwargs.get("skip_bucket_creation"):
      return
    bucket = kwargs.pop("bucket")
    enable_versioning = kwargs.pop("enable_versioning")
    if bucket:
      self._create_bucket(bucket, enable_versioning)
      self._bucket_names.append(bucket)
      return
    num_buckets = kwargs.pop("num_buckets")
    bucket_prefix = kwargs.pop("bucket_prefix")
    for i in range(num_buckets):
      bucket = bucket_prefix+str(i)
      self._create_bucket(bucket, enable_versioning)
      self._bucket_names.append(bucket)

  def _create_bucket(self, bucket, enable_versioning):
    try:
      self.s3obj.head_bucket(Bucket=bucket)
      return
    except Exception as err:
      ERROR("Error during head on bucket : %s, Error : %s"%(bucket, err))
    INFO("Creating bucket : %s"%(bucket))
    self._execute(self.s3obj.create_bucket, Bucket=bucket)
    if enable_versioning:
      self._execute(self.s3obj.put_bucket_versioning, Bucket=bucket,
                    VersioningConfiguration={"Status":"Enabled"})

  def copy(self, **kwargs):
    params = {}
    params["quota_type"] = "copy"
    object_prefix = kwargs.pop("objname_prefix")
    count = 0
    last_noted_time = time.time()
    srckey = None
    size = 0
    while True:
      if self._exit_test():
        return
      srcbucket = random.choice(self._bucket_names)
      if not srckey:
        srckey, size = self._get_object_from_bucket(srcbucket)
        time.sleep(1)
        continue
      if srckey and time.time()-last_noted_time > 30:
        srckey, size = self._get_object_from_bucket(srcbucket)
        last_noted_time = time.time()
      self._lock.acquire()
      self._total_size += size
      self._lock.release()
      params["Bucket"] = random.choice(self._bucket_names)
      params["Key"] = "%s%s"%(object_prefix, self._get_next_object_count())
      params["CopySource"] = {"Bucket": srcbucket, "Key": srckey}
      self._task_manager.add(self._copy_object, **params)
      count += 1
      if count%100 == 0:
        INFO("Total Objects copied : %s in %.1f secs, "
             %(count, time.time()-self._start_time))

  def _get_object_from_bucket(self, bucket):
    listo = self._execute(self.s3obj.list_objects, Bucket=bucket)
    if "Contents" in listo:
      key =  random.choice(listo["Contents"])
      return key["Key"], key["Size"]
    return None

  def _exit_test(self):
    if self._time_to_exit:
      INFO("Exit is called. Either exit marker is found or timeout met")
      return True
    elif self._timeout > 0 and \
                      (time.time() - self._start_time) > self._timeout:
      self._execution_completed = True
      INFO("Test timeout reached. Stopping write workload")
      return True
    return False

  def _copy_object(self, **kwargs):
    self._execute(self.s3obj.copy_object, **kwargs)

  def _get_next_object_count(self):
    self._lock.acquire()
    count = self._current_object_count
    self._current_object_count += 1
    self._lock.release()
    return count

  def write(self, **kwargs):
    num_objects = kwargs.get("num_objects")
    stime = time.time()
    totalsize = 0
    num_delimeters = kwargs.get("num_delimeters")+1
    obj_depth = kwargs.pop("depth")
    objname_prefix = kwargs.pop("objname_prefix")
    kwargs["quota_type"] = "write"
    last_printed_time = time.time()
    if num_delimeters == 0:
      num_delimeters = 1
    if self._static_data:
      self._generate_static_data(kwargs["objsize"], kwargs["compressible"])
    while True:
      if self._exit_test():
        return
      elif (num_objects > -1 and num_objects < self._current_object_count) or num_delimeters == 0:
        INFO("Num objects (delimeters %s) created vs requested : %s vs %s"
             %(kwargs.get("num_delimeters"), self._current_object_count,
               num_objects))
        self._execution_completed = True
        return
      kwargs["bucket"] = random.choice(self._bucket_names)
      if kwargs.get("delimeter_obj") and (self._current_object_count == 0 or \
            self._current_object_count%kwargs.get("num_leaf_objects") == 0):
        objname_prefix = self._get_key(obj_depth)
        INFO("Selecting Prefix : %s"%(objname_prefix))
        num_delimeters -= 1
      totalsize += kwargs["objsize"]
      objname = "%s.%s"%(objname_prefix, self._get_next_object_count())
      kwargs["objprefix"] = objname
      self._task_manager.add(self._put_object, **kwargs)
      if time.time()-last_printed_time>1:
        last_printed_time = time.time()
        ttime=time.time()-stime
        self._lock.acquire()
        self._total_size += totalsize
        totalsize = 0
        self._global_count = self._global_count + 100
        self._lock.release()
        INFO("Total Objects/size created/(failed %s) : %s (%s) in %.1f secs, "
             "ops/sec : %.1f, Throughput : %sps"%(self._total_failures_recorded,
             self._current_object_count, self._convert_size(self._total_size), ttime,
             self._current_object_count/ttime,
             self._convert_size(self._total_size/ttime)))

  def delete(self, **kwargs):
    prefix = kwargs.pop("objname_prefix")
    self._in_use_buckets = []
    self._delete_bucket_to_marker_map = { "num_objs_deleted":0,
        "num_deletemarkers_deleted":0, "total_size":0, "total_time":0 }
    last_printed_time = time.time()
    if not self._bucket_names:
      self._bucket_names = self._get_all_bucket_names(kwargs.pop("bucket"),
                                                    kwargs.pop("bucket_prefix"))
    for bucket in self._bucket_names:
      if bucket not in self._delete_bucket_to_marker_map:
        versioning = self._execute(self.s3obj.get_bucket_versioning,
                     Bucket=bucket).get("Status") in ["Suspended", "Enabled"]
        self._delete_bucket_to_marker_map[bucket] = {"versioning":versioning,
                                                     "vmarkers":[("", "")],
                                                     "markers":[""]}
    params = {}
    params["prefix"] = prefix
    params["quota_type"] = "delete"
    del(kwargs)
    while True:
      if self._time_to_exit:
        INFO("Exit is called. Stopping delete workload")
        return
      if self._timeout > 0 and (time.time() - self._start_time > self._timeout):
        self._execution_completed = True
        INFO("Test timeout reached. Stopping delete workload")
        return
      bucket = self._get_random_bucket()
      params["bucket"] = bucket
      self._task_manager.add(self._list_and_delete, **params)
      if  time.time()-last_printed_time > 2:
        last_printed_time = time.time()
        INFO("Objects deleted so far : %s (DeleteMarkers %s), Total Size : %s,"
              " Total Time : %s secs" \
          %(self._delete_bucket_to_marker_map["num_objs_deleted"],
            self._delete_bucket_to_marker_map["num_deletemarkers_deleted"],
            self._convert_size(self._delete_bucket_to_marker_map["total_size"]),
            self._delete_bucket_to_marker_map["total_time"]/\
                self._task_manager.size()))

  def _get_random_bucket(self, unique=True, retry_interval=0.1):
    if not unique:
        return random.choice(self._bucket_names)
    while True:
      bucket = random.choice(self._bucket_names)
      if bucket not in self._in_use_buckets:
        self._in_use_buckets.append(bucket)
        return bucket
      time.sleep(retry_interval)

  def _get_all_bucket_names(self, bucket_name=None, bucket_prefix=""):
    all_buckets = [bucket_name]
    if not bucket_name:
      all_buckets = [bu["Name"] for bu in self._execute(self.s3obj.list_buckets\
                    )["Buckets"] if bucket_prefix in bu["Name"]]
    INFO("Total buckets found : %s"%(all_buckets))
    return all_buckets

  def read(self, **kwargs):
    self._bucket_to_marker_map = {}
    self._bucket_to_marker_map["num_objs_read"] = 0
    self._bucket_to_marker_map["total_time"] = 0
    self._bucket_to_marker_map["total_size"] = 0
    if not self._bucket_names:
      self._bucket_names = self._get_all_bucket_names(kwargs.pop("bucket"),
                                                    kwargs.pop("bucket_prefix"))
    params = {}
    params["quota_type"] = "read"
    params["skip_integrity_check"] = kwargs.pop("skip_integrity_check")
    del(kwargs)
    for bucket in self._bucket_names:
      if bucket not in self._bucket_to_marker_map:
        self._bucket_to_marker_map[bucket] = [("", "")]
    stime = time.time()
    self._secs_to_ignore = 0
    last_printed_stats = time.time()
    while True:
      if self._exit_test():
        return
      bucket = random.choice(self._bucket_names)
      params["bucket"] = bucket
      self._task_manager.add(self._list_and_read, **params)
      if self._bucket_to_marker_map["num_objs_read"] > 0 and \
                    time.time()-last_printed_stats > 2:
        last_printed_stats = time.time()
        total_objs = self._bucket_to_marker_map["num_objs_read"]
        total_size = self._bucket_to_marker_map["total_size"]
        total_time = (time.time() - stime) - self._secs_to_ignore
        INFO("Objects read so far : %s (failed %s), in %ssecs "
             "secs, ops/sec : %s, Total Size : %s, Read Throughput : %s%s"
             %(total_objs, self._total_failures_recorded, total_time,
               0 if total_size == 0 or total_time == 0 else \
                                                        total_objs/total_time,
               0 if total_size == 0 else self._convert_size(total_size),
               0 if total_size == 0 or total_time == 0 else  \
               self._convert_size(total_size/total_time),"ps"))

  def _list_and_read(self, **kwargs):
    bucket = kwargs.pop("bucket")
    skip_integrity_check = kwargs.pop("skip_integrity_check")
    bucket_marker = self._bucket_to_marker_map.get(bucket)[-1]
    version, vmarker, kmarker, etime =  self._list_objects(bucket,
                                                           bucket_marker[0],
                                                           bucket_marker[1])
    self._bucket_to_marker_map[bucket].append((vmarker, kmarker))
    total_size = 0
    if version and version.get("Versions"):
      for i in version.get("Versions"):
        if self._exit_test():
          return
        total_size +=  i.get("Size", 0)
        self._read(bucket, i.get("Key"), str(i.get("VersionId")),
                   skip_integrity_check)
      self._lock.acquire()
      self._bucket_to_marker_map["num_objs_read"] +=len(version.get("Versions"))
      self._bucket_to_marker_map["total_size"] += total_size
      self._global_count +=  len(version.get("Versions"))
      self._secs_to_ignore += etime
      self._lock.release()

  def _read(self, bucket, objname, versionid, skip_integrity_check):
    res = self._execute(self.s3obj.get_object, Bucket=bucket, Key=objname,
                        VersionId=versionid)
    stream = res["Body"]
    datahash = hashlib.md5()
    while True:
      tmp = stream.read()
      if not tmp:
        if skip_integrity_check:
          return
        break
      if not skip_integrity_check:
        datahash.update(tmp)
    md5 = datahash.hexdigest()
    s3md5 = res["ETag"].strip("\"")
    if s3md5 != md5:
      raise Exception("Data corruption found, Bucket/Object : %s / %s(version :"
                      " %s), Expected md5 vs found : %s vs %s"
                      %(bucket, objname, versionid, ms5, s3md5 ))

  def _execute(self, method, **kwargs):
    retry_count = kwargs.pop("retry_count", self._retry_count)
    retry_delay = kwargs.pop("retry_delay", self._retry_delay)
    for i in range(retry_count+1):
      if self._time_to_exit:
        WARN("Exit called : Skipping execution of %s (args %s)"
             %(method, kwargs))
        return
      if hasattr(kwargs.get("Body"), "seek"):
        kwargs["Body"].seek(0)
      try:
        stime=time.time()
        res =  method(**kwargs)
        etime=time.time()
        if res.get("Errors"):
          raise Exception(res.get("Errors"))
        res["etime"] = etime-stime
        return res
      except Exception as err:
        traceback.print_exc()
        prnt_args = ''.join(['{0}={1},'.format(k, v) for k,v in \
                            kwargs.iteritems() if k!="Body"])
        ERROR("Hit exception while executing %s with ERROR : %s, args : %s"
              "Retry Count : %s/%s, Retry Delay : %ss, ExecutionTime : %s"
              %(method, err, prnt_args, i, retry_count, retry_delay,
                time.time()-stime))
        if i < retry_count:
          time.sleep(retry_delay)
        self._total_failures_recorded += 1
    raise Exception("Hit exception after %s retries while executing %s with"
                    " args : %s, \nTrace : %s"%(self._retry_count, method,
                                                kwargs, traceback.print_exc()))

  def _list_and_delete(self, **kwargs):
    bucket = kwargs["bucket"]
    prefix = kwargs["prefix"]
    bucket_info = self._delete_bucket_to_marker_map[bucket]
    res = None
    if bucket_info["versioning"]:
      markers = bucket_info["vmarkers"][-1]
      res, vmarker, kmarker, etime = self._list_objects(bucket=bucket,
                                                        vmarker=markers[0],
                                                        kmarker=markers[1],
                                                        prefix=prefix)
      self._delete_bucket_to_marker_map[bucket]["vmarkers"].append((vmarker,
                                                                    kmarker))
    else:
      res, marker, etime = self._list_objects(Bucket=bucket,
                                              marker=bucket_info["markers"][-1],
                                              prefix=prefix)
      self._delete_bucket_to_marker_map[bucket]["markers"].append(marker)
    self._lock.acquire()
    if bucket in self._in_use_buckets:
        self._in_use_buckets.pop(self._in_use_buckets.index(bucket))
    self._lock.release()
    if not res:
      return
    objects = []
    dmarkers = 0
    size = 0
    if res.get("Versions"):
      for i in res.get("Versions"):
        objects.append({"Key" :i.get("Key"), "VersionId":i.get("VersionId")})
        size += i["Size"]
      if res.get("DeleteMarkers"):
        for i in res.get("DeleteMarkers"):
          objects.append({"Key" :i.get("Key"), "VersionId":i.get("VersionId")})
        dmarkers = len(res.get("DeleteMarkers"))
    elif res.get("Contents"):
      for obj in res.get("Contents"):
        objects.append({"Key":obj["Key"]})
        size += i["Size"]
    else:
        WARN("No objects found in bucket %s to delete."%(bucket))
    stime = time.time()
    #DEBUG("Deleting %s objects from bucket %s"%(len(objects), bucket))
    self._execute(self.s3obj.delete_objects, Bucket=bucket,
                  Delete={"Objects":objects})
    etime = time.time()-stime
    self._lock.acquire()
    self._delete_bucket_to_marker_map["num_objs_deleted"] += len(objects)
    self._delete_bucket_to_marker_map["total_time"] += etime
    self._delete_bucket_to_marker_map["total_size"] += size
    self._delete_bucket_to_marker_map["num_deletemarkers_deleted"] += dmarkers
    self._lock.release()

  def _list_objects(self, bucket, vmarker=None, kmarker=None, marker=None,
                    prefix=None):
    params = {}
    params["Bucket"] = bucket
    if kmarker or vmarker:
      params["VersionIdMarker"] = vmarker
      params["KeyMarker"] = kmarker
    if prefix:
      params["Prefix"] = prefix
    if marker:
      DEBUG("Listing objects. Bucket : %s, Marker : %s"%(bucket, marker))
      params["Marker"] = marker
      res = self._execute(self.s3obj.list_objects, **params)
      return res,res.get("NextVersionIdMarker",""),res.get("NextKeyMarker",""),\
             res["etime"]
    else:
      DEBUG("Listing versions. Bucket : %s, VersionIdMarker : %s, ObjectMarker "
            ": %s, "%(bucket, vmarker, kmarker))
      res = self._execute(self.s3obj.list_object_versions, **params)
    return res,res.get("NextVersionIdMarker",""),res.get("NextKeyMarker",""), \
           res["etime"]

  def _put_object(self, bucket, objsize, objprefix, **kwargs):
    data, md5, base64 = self._get_data(objsize,
                                       kwargs.pop("compressible", False))
    objname = "%s_%s"%(objprefix,md5)
    self._execute(self.s3obj.put_object, Bucket=bucket, Key=objname, Body=data,
                  ContentMD5=base64)

  def _get_data(self, objsize, compressible):
    if objsize == 0:
      return "", 'd41d8cd98f00b204e9800998ecf8427e','1B2M2Y8AsgTpgAmY7PhCfg=='
    if self._static_data:
      return self._data, self._md5, self._base64
    return self._generate_data(objsize, compressible)

  def _generate_static_data(self, objsize, compressible):
    INFO("Generating static data")
    self._dataobj, self._md5, self._base64 = self._generate_data(objsize,
                                                                 compressible)
    while True:
      data = self._dataobj.read()
      if not data:
        break
      self._data = self._data+data
    return self._data, self._md5, self._base64

  def _generate_data(self, objsize, compressible):
    body1 = RandomData(objsize, compressible)
    md5sum, base64 = body1.md5sum()
    body1._current_pointer = 0
    return body1, md5sum, base64

  def _get_key(self, depth=[200,500]):
    if not self._objnames:
      self._objnames = self._get_key_list()
    size = random.randint(depth[0], depth[1])
    key=random.choice(self._objnames)
    while size > 0:
      key=key+"/"+random.choice(self._objnames)
      size = size - 1
    return "%s_%.f"%(key, time.time())

  def _get_key_list(self, size=(8,12), sizeoflist=5000):
    res = []
    while len(res) < sizeoflist:
      res.append(self._get_random_str(random.randint(size[0], size[1])))
    return res

  def _convert_size(self, size_bytes):
    if size_bytes == 0:
      return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

class parallel_execution():
  def __init__(self, threadpoolsize=10, timeout=3600, quotas=None):
    self._poolsize = threadpoolsize
    self._timeout = timeout
    self._default_quotas = {"generic":{"perc":100},
              "read":{"perc":0},
              "write":{"perc":0},
              "delete":{"perc":0}
              }
    if not quotas:
      quotas = self._default_quotas
    self._quotas = quotas
    self._queues = {}
    self._time_to_exit = False
    self._workers = []
    self._lock = Lock()
    self._init_workers()

  def _assign_workers(self):
    total_workers = 0
    for key in self._quotas.keys():
      quota = self._quotas[key]
      num_workers = (quota["perc"] * self._poolsize)/100
      total_workers = total_workers + num_workers
      quota["num_workers"] = num_workers
      self._quotas[key] = quota
      INFO("Creating queue for workload : %s, with quota : %s"%(key, quota))
      queue = Queue(num_workers*2)
      self._queues[key] = queue
    free_threads =  self._poolsize - total_workers
    if free_threads > 0:
      #TODO add free threads to generic pool
      """
      if "generic" in self._queues:
        queue = Queue(free_threads)
        self._queues["generic"] = queue
      """

  def add(self, target, **kwargs):
    timeout = kwargs.pop("timeout", self._timeout)
    quota_type = kwargs.pop("quota_type", "generic")
    queue = self._queues[quota_type]
    if self._time_to_exit:
      INFO("Exit is called, marking target as no-op")
      return
    queue.put({"target":target, "kwargs":kwargs}, timeout=timeout)

  def complete_run(self, wait_time=0.1):
    INFO("Complete run is called")
    self._time_to_exit = True
    while len(self._workers) > 0:
      for workerthread in self._workers:
        if not workerthead.is_active:
          self._workers.pop(workerthread)
      time.sleep(wait_time)

  def _init_workers(self):
    self._assign_workers()
    for quota_type in self._quotas.keys():
      INFO("Creating workers for : %s workload, with args : %s"
           %(quota_type, self._quotas[quota_type]))
      num_workers = self._quotas[quota_type]["num_workers"]
      threads = []
      for i in range(num_workers):
        worker_name = "%sWorkerThread %s"%(quota_type.title(), i)
        initworker = worker(self, quota_type, worker_name)
        wthread = Thread(target=initworker.execute)
        wthread.name =  worker_name
        wthread.start()
        threads.append(wthread)
      self._quotas["threads"] = threads

  def _get_task(self, worker_type):
    self._lock.acquire()
    task = self._pop_task(worker_type)
    self._lock.release()
    return task

  def _pop_task(self, worker_type):
    while True:
      if self._time_to_exit:
        #Empty Queue and exit.
        while not self._queues[worker_type].empty():
          INFO("Emptying workload:%s Queue"%(worker_type))
          self._queues[worker_type].get()
        return None
      if self._queues[worker_type].empty():
        time.sleep(0.1)
        continue
      task = self._queues[worker_type].get()
      return task

  def size(self):
    return self._poolsize

class worker:
  def __init__(self, pe, worker_type, worker_name):
    self._pe = pe
    self.worker_type = worker_type
    self._is_active = True
    self.worker_name = worker_name
    INFO("Initializing worker %s, for workload : %s"%(worker_name, worker_type))

  def execute(self):
    while True:
      task = self._pe._get_task(self.worker_type)
      if self._pe._time_to_exit or not task:
        self._is_active = False
        INFO("Exit is called. Exiting worker %s, type %s"%(self.worker_name,
                                                           self.worker_type))
        return
      target = task.pop("target")
      kwargs = task.pop("kwargs")
      target(**kwargs)

  def is_active(self):
    return self._is_active

class RandomData():
  def __init__(self, size, compressiable=True):
    self.size = size
    self._local_data_map = {}
    self._current_pointer = 0
    self._compressiable = compressiable
    self._local_static_data_size = 1024*1024
    self._local_static_data = self._generate_static_data()
    self._total_blocks = 0
    self._md5sum = None
    self._base64 = None
    self._generate_data()
    self._current_pointer = 0

  def read(self, size=1024*1024):
    if self._current_pointer == self._total_blocks:
      return ""
    data = self._generate_data_block(self._local_data_map[self._current_pointer])
    self._current_pointer = self._current_pointer+1
    return data

  def md5sum(self):
    if not self._md5sum:
      self._current_pointer = 0
      datahash = hashlib.md5()
      while True:
        chunk = self.read()
        if not chunk:
          break
        datahash.update(chunk)
      self._md5sum = datahash.hexdigest()
      self._base64 = self._md5sum.decode("hex").encode("base64").strip()
      self._current_pointer = 0
    return self._md5sum, self._base64

  def seek(self, offset, whence=0):
    if offset > self._total_blocks:
      ERROR("Requested offset (%s) is outside of file. Seeking to end of file"
             %(offset))
      offset = self._total_blocks
    if whence == 2:
      self._current_pointer = self._total_blocks
      return
    if whence == 1:
      return
    self._current_pointer = offset

  def tell(self):
    return self._current_pointer

  def _generate_data(self):
    num_blocks_to_generate = self.size / self._local_static_data_size
    last_block_size = self.size%self._local_static_data_size
    self._total_blocks = num_blocks_to_generate
    if last_block_size:
      self._total_blocks = self._total_blocks + 1
    while num_blocks_to_generate > 0 :
      random_data_index = random.randint(0, self._local_static_data_size)
      self._local_data_map[self._current_pointer] = (random_data_index,
                                                   self._local_static_data_size)
      num_blocks_to_generate = num_blocks_to_generate - 1
      self._current_pointer = self._current_pointer + 1
    if last_block_size:
      self._local_data_map[self._current_pointer] = (0, last_block_size)
    self._current_pointer = 0

  def _generate_data_block(self, offset):
    random_data_index = offset[0]
    size_of_block = offset[1]
    current_data_block = self._local_static_data[random_data_index:]
    remaining_data  = self._local_static_data[:\
                      (self._local_static_data_size - len(current_data_block))]
    data = "%s%s"%(current_data_block, remaining_data)
    return  data[:size_of_block]
    #return "%s%s"%(current_data_block, remaining_data)[:size_of_block]

  def _generate_static_data(self):
    if self._compressiable:
      INFO("Generating compressible data")
      return self._get_random_str(8) * (self._local_static_data_size/8)
    return self._get_random_str(self._local_static_data_size)

  def _get_random_str(self, size):
    return ''.join(random.choice(string.ascii_uppercase + string.digits + \
           string.ascii_lowercase) for _ in range(size))

def logconfig(logfilename):
  logger.setLevel(logging.DEBUG)
  logging.getLogger('boto3').setLevel(logging.CRITICAL)
  logging.getLogger('botocore').setLevel(logging.CRITICAL)
  logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
  logging.getLogger('urllib3').setLevel(logging.CRITICAL)

  ch = logging.StreamHandler()
  ch.setLevel(logging.DEBUG)
  formatter = logging.Formatter('%(asctime)s (%(threadName)s) %(levelname)s '\
                                '[%(filename)s:%(lineno)d] : %(message)s')
  ch.setFormatter(formatter)
  fh = logging.FileHandler(logfilename)
  logger.addHandler(fh)
  logger.addHandler(ch)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Upload logs to Buckets cluster")
  parser.add_argument('--endpoint_url', required=False, metavar='ENDPOINT_URL',
                      help="Endpoing URL to connect to S3 server")
  parser.add_argument('--access_key', required=False, metavar='<ACCESS_KEY>',
                      help="Access key to log in")
  parser.add_argument('--secret_key', required=False, metavar='<SECRETE_KEY>',
                      help="Secret key to log in")
  parser.add_argument("--bucket", required=False, metavar='<BUCKETNAME>',
                       help="Bucket name to upload results to",
                       default="")
  parser.add_argument("--enable_versioning", required=False,
                      metavar='<EnableVersioning>', help="Enable Versioning."\
                      "Default : True", default="True")
  parser.add_argument("--objsize", required=False, metavar='<OBJCTSIZE>',
                      help="Object size in kb. Default:1M",
                      default=100)
  parser.add_argument("--parallel_threads", required=False,
                      metavar='ParallelUploads', help="Parallel objects upload"\
                      "s. Default:10", default=10)
  parser.add_argument("--parallel_bucket_ops", required=False,
                      metavar='ParallelBucketsOps', help="Num of buckets to "\
                      "work in parallel. Default:10", default=1)
  parser.add_argument("--objname_prefix", required=False, metavar='OBJPREFIX',
                      help="Objname_prefix. Default:key", default="key")
  parser.add_argument("--retry_count", required=False, metavar='<RETRYCOUNT>',
                      help="Num of retries for object put", default=3)
  parser.add_argument("--static_data", required=False, metavar='STATICDATA',
                      help="Whether to use static data or not", default="True")
  parser.add_argument("--compressible_data", required=False,
                      metavar='<COMPRESSIBLEDATA>', help="Whether to generate"\
                      "compressible data or not", default="True")
  parser.add_argument("--workload", required=False, metavar='<WorkLoad>',
                      help="Workload to run. Supported Values : "\
                       "write|read|delete|write,read|write,read,delete|read,"\
                       "delete|write,delete|write:80,read:20|write:40,read:40,"\
                       "delete:20. Default:write", default="write")
  parser.add_argument("--skip_integrity_check", required=False,
                      metavar='<SkipIntegrityCheck>', help="Skip integrity "\
                      "check during object GET.Default:False", default="False")
  parser.add_argument("--delimeter_obj", required=False,
                      metavar='<UploadDelimeterObjects>', help="Upload "\
                      "delimeter object.Default:False", default="False")
  parser.add_argument("--dir_depth", required=False, metavar='<DirDepth>',
                      help="Create dir depth structure.Default:15,20",
                      default="15,20")
  parser.add_argument("--num_objects", required=False, metavar='<NumObjects>',
                      help="Number of objects to create in given bucket. "\
                      "Default:unlimited", default=-1)
  parser.add_argument("--num_leaf_objects", required=False,
                      metavar='NumLeafObjects', help="Num of leaf  objects"\
                      "to create in given dir in bucket. Default:10000",
                      default=10000)
  parser.add_argument("--num_delimeters", required=False,
                      metavar='NumDelimeters',
                      help="Number of delimeters  to create in bucket. "\
                      "Default:Unlimited", default=-1)
  parser.add_argument("--retry_delay", required=False, metavar='<RetryDelay>',
                      help="Delay between each retry. Default:10s",
                      default=10)
  parser.add_argument("--cfgfile", required=False, metavar='<cfgfile>',
                      help="Config file to load params from. Default:None",
                      default=None)
  parser.add_argument("--runtime", required=False, metavar='<RunTime>',
                      help="How long to run the workload. Default:Nolimit",
                      default=-1)
  parser.add_argument("--num_buckets", required=False, metavar='<NumBuckets>',
                      help="Num buckets to work on. Default:1",
                      default=1)
  parser.add_argument("--bucket_prefix", required=False,
                      metavar='<BucketPrefix>', help="Bucket name to start "\
                      "with. Default:bucketworkload", default="bucketworkload")
  parser.add_argument("--log_path", required=False, metavar='<LogFile>',
                      help="Logpath.Default:/tmp",
                      default="/tmp")
  parser.add_argument("--skip_bucket_creation", required=False,
                      metavar='<SkipBucktCreation>',
                      help="Skip bucket creation. Default:False",
                      default="False")

  args = vars(parser.parse_args())
  cfg_file = args.pop("cfgfile")
  if cfg_file:
    with open(cfg_file) as fh:
      while True:
        line = fh.readline()
        if not line:
          break
        param = line.strip().split("=")
        args[param[0]] = param[1]
  logfile="{0}/{1}_{2}.log".format(args.pop("log_path"), \
          "_".join(["".join(i.split(":"))for i in args["workload"].split(",")])\
                    ,datetime.datetime.today().strftime('%Y-%m-%d.%H.%M.%S'))
  logger = logging.getLogger()
  logconfig(logfile)
  INFO = logger.info
  ERROR = logger.error
  DEBUG = logger.debug
  WARN = logger.warn

  INFO("Args passed to test : %s, log : %s"%(args, logfile))
  objsize = int(args.get("objsize")) * 1024
  runtime = int(args.pop("runtime"))
  static_data = True if (args.pop("static_data").lower() == "true" \
                and objsize < 11*1024*1024) else False
  compressible = True if (args.pop("compressible_data").lower() == "true") else\
                 False
  enable_versioning = True if args.pop("enable_versioning").lower() == "true" \
                      else False
  delimeter_obj = True if args.pop("delimeter_obj").lower() == "true" else False
  sbc = True if args.pop("skip_bucket_creation").lower() == "true" else False
  args["skip_bucket_creation"] = sbc
  args["skip_integrity_check"] = True if \
                args.pop("skip_integrity_check").lower() == "true" else False
  args["enable_versioning"] = enable_versioning
  args["objsize"] = objsize
  args["compressible"] = compressible
  args["depth"] = [int(i) for i in args.pop("dir_depth", "15,20").split(",")]
  args["delimeter_obj"] = delimeter_obj
  args["num_objects"] = int(args.pop("num_objects"))
  args["num_buckets"] = int(args.pop("num_buckets"))
  args["parallel_threads"] = int(args.pop("parallel_threads"))
  args["num_leaf_objects"] = int(args.pop("num_leaf_objects"))
  args["num_delimeters"] = int(args.pop("num_delimeters"))
  INFO("Final Arguments passed to program : %s"%(args))
  c=connect(ip=args.pop("endpoint_url"), access=args.pop("access_key"),
            secret=args.pop("secret_key"), max_pool_connections=200)
  upload = UploadObjs(c, static_data, retry_count=int(args.pop("retry_count")),
                      retry_delay=int(args.pop("retry_delay")), runtime=runtime)
  upload.start_workload(**args)
