"""
This is just my dirty script to upload few objects. If you need any help, plz
contact me &#64; anirudh.sonar&#64;nutanix.com

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
    srcbucket = None
    while True:
      if self._exit_test("copy"):
        return
      if not srckey:
        srcbucket = random.choice(self._bucket_names)
        srckey, size = self._get_object_from_bucket(srcbucket)
        time.sleep(1)
        continue
      if srckey and time.time()-last_noted_time > 30:
        srcbucket = random.choice(self._bucket_names)
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
      self._b...
