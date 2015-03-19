/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2014 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import akka.actor.Props

import org.apache.spark._
import org.apache.spark.monotasks.LocalDagScheduler
import org.apache.spark.monotasks.compute.PrepareMonotask
import org.apache.spark.util.{SparkUncaughtExceptionHandler, AkkaUtils, Utils}
import org.apache.spark.performance_logging.ContinuousMonitor

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 * In coarse-grained mode, an existing actor system is provided.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    executorBackend: ExecutorBackend,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
  extends Logging
{

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  @volatile private var isStopped = false

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  val executorSource = new ExecutorSource(this, executorId)

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Create an actor for receiving RPCs from the driver
  private val executorActor = env.actorSystem.actorOf(
    Props(new ExecutorActor(executorId)), "ExecutorActor")

  // Create our DependencyManager, which manages the class loader.
  private val dependencyManager = new DependencyManager(env, conf, userClassPath, isLocal)

  // If a task result is larger than this, we use the block manager to send the task result back.
  private val maximumResultSizeBytes =
    AkkaUtils.maxFrameSizeBytes(conf) - AkkaUtils.reservedSizeBytes

  private val localDagScheduler = new LocalDagScheduler(executorBackend, env.blockManager)

  private val continuousMonitor = new ContinuousMonitor(
    conf,
    localDagScheduler.getNumRunningComputeMonotasks,
    localDagScheduler.getNumRunningMacrotasks)
  continuousMonitor.start(env)

  def launchTask(
      taskAttemptId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer) {
    // TODO: Do we really need to propogate this task started message back to the scheduler?
    //       Doesn't the scheduler just drop it?
    executorBackend.statusUpdate(taskAttemptId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
    val context = new TaskContextImpl(
      env,
      localDagScheduler,
      maximumResultSizeBytes,
      dependencyManager,
      taskAttemptId,
      attemptNumber)
    val prepareMonotask = new PrepareMonotask(context, serializedTask)
    localDagScheduler.submitMonotask(prepareMonotask)
  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    // TODO: Support killing tasks: https://github.com/NetSys/spark-monotasks/issues/4
  }

  def stop() {
    env.metricsSystem.report()
    env.actorSystem.stop(executorActor)
    continuousMonitor.stop()
    isStopped = true
  }
}
