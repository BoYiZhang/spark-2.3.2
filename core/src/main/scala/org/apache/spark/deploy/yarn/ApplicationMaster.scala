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

package org.apache.spark.deploy.yarn

import java.io.{File, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{Socket, URI, URL}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.collection.mutable.HashMap
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.deploy.yarn.security.{AMCredentialRenewer, YARNHadoopDelegationTokenManager}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util._

/**
  * --class WordCount --jar file:/A/spark-test/spark-test.jar --arg '/tmp/zl/data/data.txt' --properties-file /A/application_1544436077214_7170/__spark_conf__/__spark_conf__.properties
  *  准备条件：
  *  1.本地：  application_1544436077214_7170
  *
  *           --properties-file 这个参数，指定的是本地的路径，application_1544436077214_7170文件我扔到项目里面了
  *
  *
  *
  *  2.远程：
  *         hdfs上面必须保存： hdfs://bj-rack001-hadoop002:8020/user/hadoop/.sparkStaging/application_1544436077214_7170
  *         application_1544436077214_7170 文件在项目中有保存。
  *
  *  3.不成就自己手动生成一份啊
 */
private[spark] class ApplicationMaster(args: ApplicationMasterArguments) extends Logging {

  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.
  logInfo("当前，task的container 只被计算一次(TaskSetManager) ，多个容器不一定是最优的，可能需要更好地处理这个问题方式 ")
  private val isClusterMode = args.userClass != null

  private val sparkConf = new SparkConf()
  if (args.propertiesFile != null) {
    logInfo("加载配置文件SparkConf。。。");
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sparkConf.set(k, v)
    }
  }

  private val securityMgr = new SecurityManager(sparkConf)

  logInfo("Set system properties for each config entry. This covers two use cases:");
  logInfo("- The default configuration stored by the SparkHadoopUtil class");
  logInfo("- The user application creating a new SparkConf in cluster mode");

  logInfo("Both cases create a new SparkConf object which reads these configs from system properties.");

  logInfo("更新系统环境配置。。。")
  sparkConf.getAll.foreach { case (k, v) =>
    sys.props(k) = v
  }


  logInfo("获取yarnConf 配置 。。。")
  private val yarnConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))


  logInfo("获取 UserGroupInformation   配置 。。。")
  private val ugi = {
    val original = UserGroupInformation.getCurrentUser()

    // If a principal and keytab were provided, log in to kerberos, and set up a thread to
    // renew the kerberos ticket when needed. Because the UGI API does not expose the TTL
    // of the TGT, use a configuration to define how often to check that a relogin is necessary.
    // checkTGTAndReloginFromKeytab() is a no-op if the relogin is not yet needed.
    val principal = sparkConf.get(PRINCIPAL).orNull
    val keytab = sparkConf.get(KEYTAB).orNull
    if (principal != null && keytab != null) {
      UserGroupInformation.loginUserFromKeytab(principal, keytab)

      val renewer = new Thread() {
        override def run(): Unit = Utils.tryLogNonFatalError {
          while (true) {
            TimeUnit.SECONDS.sleep(sparkConf.get(KERBEROS_RELOGIN_PERIOD))
            UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab()
          }
        }
      }
      renewer.setName("am-kerberos-renewer")
      renewer.setDaemon(true)
      renewer.start()

      // Transfer the original user's tokens to the new user, since that's needed to connect to
      // YARN. It also copies over any delegation tokens that might have been created by the
      // client, which will then be transferred over when starting executors (until new ones
      // are created by the periodic task).
      val newUser = UserGroupInformation.getCurrentUser()
      SparkHadoopUtil.get.transferCredentials(original, newUser)
      newUser
    } else {
      SparkHadoopUtil.get.createSparkUser()
    }
  }

  logInfo("创建YarnRMClient 客户端")
  logInfo("Handles registering and unregistering the application with the YARN ResourceManager.")
  private val client = doAsUser { new YarnRMClient() }

  // Default to twice the number of executors (twice the maximum number of executors if dynamic
  // allocation is enabled), with a minimum of 3.

  logInfo("设置executors 失败次数，默认是executors的两倍(如果executors启用动态分配)， 最小值3 ，最大值不超过Int的最大值。。。。。 ")

  private val maxNumExecutorFailures = {
    logInfo("是否开启executors 动态分配" + Utils.isDynamicAllocationEnabled(sparkConf) )
    val effectiveNumExecutors =
      if (Utils.isDynamicAllocationEnabled(sparkConf)) {
        logInfo("最大 Executors 数量")
        logInfo("DYN_ALLOCATION_MAX_EXECUTORS : spark.dynamicAllocation.maxExecutors")
        sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      } else {
        logInfo("------------------- ")
        logInfo("EXECUTOR_INSTANCES : spark.executor.instance ")
        sparkConf.get(EXECUTOR_INSTANCES).getOrElse(0)
      }


    logInfo("By default, effectiveNumExecutors is Int.MaxValue if dynamic allocation is enabled. We need     ")
    logInfo("avoid the integer overflow here.     ")

    logInfo("设置defaultMaxNumExecutorFailures数量。     ")
    logInfo("默认，如果启动动态分配，这 有效的executors的数量是Int的最大值。 要避免Int类型溢出。    ")

    val defaultMaxNumExecutorFailures = math.max(3,
      if (effectiveNumExecutors > Int.MaxValue / 2) Int.MaxValue else (2 * effectiveNumExecutors))

    sparkConf.get(MAX_EXECUTOR_FAILURES).getOrElse(defaultMaxNumExecutorFailures)
  }

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""
  @volatile private var userClassThread: Thread = _

  @volatile private var reporterThread: Thread = _
  @volatile private var allocator: YarnAllocator = _

  // A flag to check whether user has initialized spark context
  @volatile private var registered = false

  logInfo("加载用户类userClassLoader : ")
  private val userClassLoader = {
    val classpath = Client.getUserClasspath(sparkConf)
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }

    if (isClusterMode) {
      logInfo("集群模式，加载类。。。。 ")
      logInfo("(实验性)当在driver中加载类时，是否用户添加的jar比Spark自己的jar优先级高。这个属性可以降低Spark依赖和用户依赖的冲突。它现在还是一个实验性的特征。 ")
      if (Client.isUserClassPathFirst(sparkConf, isDriver = true)) {
        logInfo("集群模式，类加载器： ChildFirstURLClassLoader 。 ")
        logInfo("集群模式，类加载器： urls ：  "+ urls)
        logInfo("集群模式，类加载器： getContextOrSparkClassLoader ：  "+ Utils.getContextOrSparkClassLoader)
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        logInfo("集群模式，类加载器： MutableURLClassLoader 。 ")
        logInfo("集群模式，类加载器： urls ：  "+ urls)
        logInfo("集群模式，类加载器： getContextOrSparkClassLoader ：  "+ Utils.getContextOrSparkClassLoader)
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }
    } else {
      logInfo("非集群模式，类加载器： MutableURLClassLoader 。 ")
      logInfo("非集群模式，类加载器： urls ：  "+ urls)
      logInfo("非集群模式，类加载器： getContextOrSparkClassLoader ：  "+ Utils.getContextOrSparkClassLoader)
      new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
    }
  }

  // Lock for controlling the allocator (heartbeat) thread.
  logInfo("锁定用于控制分配器（心跳）线程。 。 ")
  private val allocatorLock = new Object()

  // Steady state heartbeat interval. We want to be reasonably responsive without causing too many
  // requests to RM.
  logInfo("稳定的心跳周期， 不想对RM造成太多的请求。。。。 ")

  private val heartbeatInterval = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.

    logInfo("RM_AM_EXPIRY_INTERVAL_MS  :  am.liveness-monitor.expiry-interval-ms  默认值 120000 ms = 2min ")
    logInfo("设置心跳间隔，周期：  ")


    logInfo("RM_HEARTBEAT_INTERVAL : am.liveness-monitor.expiry-interval-ms  默认值 3s  ")
    logInfo("expiryInterval 默认10min， 最小值3s ")
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)
    math.max(0, math.min(expiryInterval / 2, sparkConf.get(RM_HEARTBEAT_INTERVAL)))
    logInfo(" 当前心跳间隔heartbeatInterval ：  expiryInterval :   "+ expiryInterval)

    expiryInterval
  }

  // Initial wait interval before allocator poll, to allow for quicker ramp up when executors are
  // being requested.

  logInfo(" 在 分配 之前初始化 等待间隔，使得executors 更快的执行  ")

  logInfo("INITIAL_HEARTBEAT_INTERVAL :spark.yarn.scheduler.initial-allocation.interval   默认值 200ms  ")

  private val initialAllocationInterval = math.min(heartbeatInterval,
    sparkConf.get(INITIAL_HEARTBEAT_INTERVAL))

  logInfo("Next wait interval before allocator poll. ")
  private var nextAllocationInterval = initialAllocationInterval

  private var rpcEnv: RpcEnv = null

  logInfo("In cluster mode, used to tell the AM when the user's SparkContext has been initialized. ")
  private val sparkContextPromise = Promise[SparkContext]()

  private var credentialRenewer: AMCredentialRenewer = _

  logInfo("Load the list of localized files set by the client. This is used when launching executors, ")
  logInfo("and is loaded here so that these configs don't pollute the Web UI's environment page in ")
  logInfo("cluster mode. ")

  logInfo(" client 加载本地初始化文件， 被用于启动executors ， 集群模式下， 在这里加载配置文件的时候，不会影响Web UI. ")
  // todo 加载资源
  private val localResources = doAsUser {
    logInfo("准备加载本地资源。。。。。")
    val resources = HashMap[String, LocalResource]()

    def setupDistributedCache(
        file: String,
        rtype: LocalResourceType,
        timestamp: String,
        size: String,
        vis: String): Unit = {


      val uri = new URI(file)

      logInfo("设置分布式缓存 setupDistributedCache#uri： "+uri )

      val amJarRsrc = Records.newRecord(classOf[LocalResource])
      amJarRsrc.setType(rtype)
      amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
      amJarRsrc.setTimestamp(timestamp.toLong)
      amJarRsrc.setSize(size.toLong)

      val fileName = Option(uri.getFragment()).getOrElse(new Path(uri).getName())
      resources(fileName) = amJarRsrc
      logInfo("设置分布式缓存 setupDistributedCache#amJarRsrc： "+amJarRsrc )

    }

    val distFiles = sparkConf.get(CACHED_FILES)
    val fileSizes = sparkConf.get(CACHED_FILES_SIZES)
    val timeStamps = sparkConf.get(CACHED_FILES_TIMESTAMPS)
    val visibilities = sparkConf.get(CACHED_FILES_VISIBILITIES)
    val resTypes = sparkConf.get(CACHED_FILES_TYPES)

    for (i <- 0 to distFiles.size - 1) {
      val resType = LocalResourceType.valueOf(resTypes(i))
      setupDistributedCache(distFiles(i), resType, timeStamps(i).toString, fileSizes(i).toString,
      visibilities(i))
    }

    logInfo("Distribute the conf archive to executors. ")
    logInfo("为executors分配配置. ")
    sparkConf.get(CACHED_CONF_ARCHIVE).foreach { path =>
      val uri = new URI(path)
      val fs = FileSystem.get(uri, yarnConf)
      val status = fs.getFileStatus(new Path(uri))
      logInfo("SPARK-16080: Make sure to use the correct name for the destination when distributing the ")
      logInfo("conf archive to executors. ")


      val destUri = new URI(uri.getScheme(), uri.getRawSchemeSpecificPart(),
        Client.LOCALIZED_CONF_DIR)
      logInfo("为executors分配配置  ==》 destUri ：  "+destUri )

      setupDistributedCache(destUri.toString(), LocalResourceType.ARCHIVE,
        status.getModificationTime().toString, status.getLen.toString,
        LocalResourceVisibility.PRIVATE.name())
    }

    logInfo("Clean up the configuration so it doesn't show up in the Web UI (since it's really noisy). ")
    logInfo("清理 配置，所以不会在Web UI 中显示，因为其很杂乱。。。    ")
    CACHE_CONFIGS.foreach { e =>
      sparkConf.remove(e)
      sys.props.remove(e.key)
    }

    resources.toMap
  }

  def getAttemptId(): ApplicationAttemptId = {
    client.getAttemptId()
  }

  final def run(): Int = {
    doAsUser {
      runImpl()
    }
    exitCode
  }

  private def runImpl(): Unit = {

    logInfo("启动： runImpl 。。。。。。。")
    try {
      //todo 为了调试，这个我就先注释掉，
//      val appAttemptId = client.getAttemptId()

      //todo test
      val appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1545118153810L, 0), 0)
//      val appAttemptId = Records.newRecord(classOf[ApplicationAttemptId])

      var attemptID: Option[String] = None

      if (isClusterMode) {
        logInfo("集群模式，需要设置一下系统属性值。。。。。")

        logInfo("Set the web ui port to be ephemeral for yarn so we don't conflict with")
        logInfo("other spark processes running on the same box")
        logInfo(" 临时设置web ui 端口，在同一个box中，不会与其他spark进程冲突")

        System.setProperty("spark.ui.port", "0")

        logInfo("Set the master and deploy mode property to match the requested mode.")
        System.setProperty("spark.master", "yarn")
        System.setProperty("spark.submit.deployMode", "cluster")

        logInfo("Set this internal configuration if it is running on cluster mode, this")
        logInfo("configuration will be checked in SparkContext to avoid misuse of yarn cluster mode.")
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

        attemptID = Option(appAttemptId.getAttemptId.toString)
      }

      new CallerContext(
        "APPMASTER", sparkConf.get(APP_CALLER_CONTEXT),
        Option(appAttemptId.getApplicationId.toString), attemptID).setCurrentContext()

      logInfo("ApplicationAttemptId: " + appAttemptId)

      logInfo("This shutdown hook should run *after* the SparkContext is shut down.")
      logInfo("设置一个钩子，当SparkContext 停止 。。。。。 ")
      val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1
      ShutdownHookManager.addShutdownHook(priority) { () =>
        val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

        if (!finished) {
          logInfo("The default state of ApplicationMaster is failed if it is invoked by shut down hook.")
          logInfo("This behavior is different compared to 1.x version.")
          logInfo("If user application is exited ahead of time by calling System.exit(N), here mark")
          logInfo("this application as failed with EXIT_EARLY. For a good shutdown, user shouldn't call")
          logInfo("System.exit(0) to terminate the application.")
          finish(finalStatus,
            ApplicationMaster.EXIT_EARLY,
            "Shutdown hook called before final status was reported.")
        }

        if (!unregistered) {
          logInfo("we only want to unregister if we don't want the RM to retry")
          if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
            unregister(finalStatus, finalMsg)
            cleanupStagingDir()
          }
        }
      }

      logInfo("如果配置了证书，必须定期检查token")
      logInfo( "   If the credentials file config is present, we must periodically renew tokens. So create")
      logInfo("a new AMDelegationTokenRenewer")

      if (sparkConf.contains(CREDENTIALS_FILE_PATH)) {
        logInfo("Start a short-lived thread for AMCredentialRenewer, the only purpose is to set the")
        logInfo("classloader so that main jar and secondary jars could be used by AMCredentialRenewer.")
        val credentialRenewerThread = new Thread {
          setName("AMCredentialRenewerStarter")
          setContextClassLoader(userClassLoader)

          override def run(): Unit = {
            val credentialManager = new YARNHadoopDelegationTokenManager(
              sparkConf,
              yarnConf,
              conf => YarnSparkHadoopUtil.hadoopFSsToAccess(sparkConf, conf))

            val credentialRenewer =
              new AMCredentialRenewer(sparkConf, yarnConf, credentialManager)
            credentialRenewer.scheduleLoginFromKeytab()
          }
        }

        credentialRenewerThread.start()
        credentialRenewerThread.join()
      }

      if (isClusterMode) {
        logInfo("集群模式：启动Driver ，执行runDriver方法")
        runDriver()
      } else {
        logInfo("非集群模式：启动runExecutorLauncher ，执行runExecutorLauncher方法")
        runExecutorLauncher()
      }
    } catch {
      case e: Exception =>
        logInfo("catch everything else if not specifically handled")
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + e)
    }
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus(): FinalApplicationStatus = {
    if (isClusterMode) {
      FinalApplicationStatus.FAILED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
   */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null): Unit = {
    synchronized {
      if (registered && !unregistered) {
        logInfo(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
        unregistered = true
        client.unregister(status, Option(diagnostics).getOrElse(""))
      }
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null): Unit = {

    synchronized {
      if (!finished) {
        val inShutdown = ShutdownHookManager.inShutdown()
        if (registered) {
          exitCode = code
          finalStatus = status
        } else {
          finalStatus = FinalApplicationStatus.FAILED
          exitCode = ApplicationMaster.EXIT_SC_NOT_INITED
        }
        logInfo(s"Final app status: $finalStatus, exitCode: $exitCode" +
          Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
        finalMsg = msg
        finished = true
        if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
          logDebug("shutting down reporter thread")
          reporterThread.interrupt()
        }
        if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
          logDebug("shutting down user thread")
          userClassThread.interrupt()
        }
        if (!inShutdown && credentialRenewer != null) {
          credentialRenewer.stop()
          credentialRenewer = null
        }
      }
    }
  }

  private def sparkContextInitialized(sc: SparkContext) = {
    sparkContextPromise.synchronized {
      // Notify runDriver function that SparkContext is available
      sparkContextPromise.success(sc)
      // Pause the user class thread in order to make proper initialization in runDriver function.
      sparkContextPromise.wait()
    }
  }

  private def resumeDriver(): Unit = {
    logInfo("When initialization in runDriver happened the user class thread has to be resumed.")
    sparkContextPromise.synchronized {
      sparkContextPromise.notify()
    }
  }

  // 向 Yarn 注册ApplicationMaster
  private def registerAM(
      _sparkConf: SparkConf,
      _rpcEnv: RpcEnv,
      driverRef: RpcEndpointRef,
      uiAddress: Option[String]) = {
    val appId = client.getAttemptId().getApplicationId().toString()
    val attemptId = client.getAttemptId().getAttemptId().toString()
    val historyAddress = ApplicationMaster
      .getHistoryServerAddress(_sparkConf, yarnConf, appId, attemptId)

    val driverUrl = RpcEndpointAddress(
      _sparkConf.get("spark.driver.host"),
      _sparkConf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString


    // Before we initialize the allocator, let's log the information about how executors will
    // be run up front, to avoid printing this out for every single executor being launched.
    // Use placeholders for information that changes such as executor IDs.
    logInfo {
      val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
      val executorCores = sparkConf.get(EXECUTOR_CORES)

      logInfo("创建ExecutorRunnable ： ")
      val dummyRunner = new ExecutorRunnable(None, yarnConf, sparkConf, driverUrl, "<executorId>",
        "<hostname>", executorMemory, executorCores, appId, securityMgr, localResources)

      logInfo("创建ExecutorRunnable ： " + dummyRunner )


      logInfo("输出启动信息：dummyRunner.launchContextDebugInfo() " )
      dummyRunner.launchContextDebugInfo()

    }

    logInfo("根据RM 注册 application master ,并返回一个资源调度器   ")
    allocator = client.register(driverUrl,
      driverRef,
      yarnConf,
      _sparkConf,
      uiAddress,
      historyAddress,
      securityMgr,
      localResources)

    // Initialize the AM endpoint *after* the allocator has been initialized. This ensures
    // that when the driver sends an initial executor request (e.g. after an AM restart),
    // the allocator is ready to service requests.
    rpcEnv.setupEndpoint("YarnAM", new AMEndpoint(rpcEnv, driverRef))

    //申请Executor资源
    allocator.allocateResources()

    //TODO 启动汇报线程
    logInfo(" 启动 launchReporterThread    ")
    reporterThread = launchReporterThread()
  }

  /**
   * @return An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
   */
  private def createSchedulerRef(host: String, port: String): RpcEndpointRef = {

    // 注意这里实例化的是YarnSchedulerEndpoint
    // AMEndpoint的driverEndpoint是指YarnSchedulerEndpoint， 而不是DriverEndpoint
    rpcEnv.setupEndpointRef(
      RpcAddress(host, port.toInt),
      YarnSchedulerBackend.ENDPOINT_NAME)
  }

  //todo runDriver
  private def runDriver(): Unit = {

    addAmIpFilter(None)


    logInfo("执行startUserApplication 方法， 启动用户类   并返回一个线程。。。。")
    userClassThread = startUserApplication()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    logInfo("Waiting for spark context initialization...")



    val totalWaitTime = sparkConf.get(AM_MAX_WAIT_TIME)
    try {

      val sc = ThreadUtils.awaitResult(sparkContextPromise.future, Duration(totalWaitTime, TimeUnit.MILLISECONDS))

      if (sc != null) {
        rpcEnv = sc.env.rpcEnv
        val driverRef = createSchedulerRef(
          sc.getConf.get("spark.driver.host"),
          sc.getConf.get("spark.driver.port"))

        logInfo("注册AM  registerAM " )
        registerAM(sc.getConf, rpcEnv, driverRef, sc.ui.map(_.webUrl))
        registered = true
      } else {
        // Sanity check; should never happen in normal operation, since sc should only be null
        // if the user app did not create a SparkContext.
        throw new IllegalStateException("User did not initialize spark context!")
      }




      logInfo("resumeDriver...")
      resumeDriver()
      userClassThread.join()
    } catch {
      case e: SparkException if e.getCause().isInstanceOf[TimeoutException] =>
        logError(
          s"SparkContext did not initialize after waiting for $totalWaitTime ms. " +
           "Please check earlier log output for errors. Failing the application.")
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_SC_NOT_INITED,
          "Timed out waiting for SparkContext.")
    } finally {
      resumeDriver()
    }
  }

  private def runExecutorLauncher(): Unit = {
    val hostname = Utils.localHostName
    val amCores = sparkConf.get(AM_CORES)

    // 实例化 rpcEnv
    rpcEnv = RpcEnv.create("sparkYarnAM", hostname, hostname, -1, sparkConf, securityMgr,
      amCores, true)

    // 等待DriverEndpoint服务启动，
    val driverRef = waitForSparkDriver()
    addAmIpFilter(Some(driverRef))
    registerAM(sparkConf, rpcEnv, driverRef, sparkConf.getOption("spark.driver.appUIAddress"))
    registered = true


    // reporterThread会与yarn的ResourceManager保持心跳，知道程序运行结束
    reporterThread.join()
  }

  private def launchReporterThread(): Thread = {
    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = sparkConf.get(MAX_REPORTER_THREAD_FAILURES)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                s"Max number of executor failures ($maxNumExecutorFailures) reached")
            } else {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException => // do nothing
            case e: ApplicationAttemptNotFoundException =>
              failureCount += 1
              logError("Exception from Reporter thread.", e)
              finish(FinalApplicationStatus.FAILED, ApplicationMaster.EXIT_REPORTER_FAILURE,
                e.getMessage)
            case e: Throwable =>
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"$failureCount time(s) from Reporter thread.")
              } else {
                logWarning(s"Reporter thread fails $failureCount time(s) in a row.", e)
              }
          }
          try {
            val numPendingAllocate = allocator.getPendingAllocate.size
            var sleepStart = 0L
            var sleepInterval = 200L // ms
            allocatorLock.synchronized {
              sleepInterval =
                if (numPendingAllocate > 0 || allocator.getNumPendingLossReasonRequests > 0) {
                  val currentAllocationInterval =
                    math.min(heartbeatInterval, nextAllocationInterval)
                  nextAllocationInterval = currentAllocationInterval * 2 // avoid overflow
                  currentAllocationInterval
                } else {
                  nextAllocationInterval = initialAllocationInterval
                  heartbeatInterval
                }
              sleepStart = System.currentTimeMillis()
              allocatorLock.wait(sleepInterval)
            }
            val sleepDuration = System.currentTimeMillis() - sleepStart
            if (sleepDuration < sleepInterval) {
              // log when sleep is interrupted
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                  s"Slept for $sleepDuration/$sleepInterval ms.")
              // if sleep was less than the minimum interval, sleep for the rest of it
              val toSleep = math.max(0, initialAllocationInterval - sleepDuration)
              if (toSleep > 0) {
                logDebug(s"Going back to sleep for $toSleep ms")
                // use Thread.sleep instead of allocatorLock.wait. there is no need to be woken up
                // by the methods that signal allocatorLock because this is just finishing the min
                // sleep interval, which should happen even if this is signalled again.
                Thread.sleep(toSleep)
              }
            } else {
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                  s"Slept for $sleepDuration/$sleepInterval.")
            }
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo(s"Started progress reporter thread with (heartbeat : $heartbeatInterval, " +
            s"initial allocation : $initialAllocationInterval) intervals")
    t
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir(): Unit = {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.get(PRESERVE_STAGING_FILES)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        logInfo("Deleting staging directory " + stagingDirPath)
        val fs = stagingDirPath.getFileSystem(yarnConf)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private def waitForSparkDriver(): RpcEndpointRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    // 解析driverEndpoint的服务地址
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)

    // 计算超时时间
    // Spark driver should already be up since it launched us, but we don't want to
    // wait forever, so wait 100 seconds max to match the cluster mode setting.
    val totalWaitTimeMs = sparkConf.get(AM_MAX_WAIT_TIME)
    val deadline = System.currentTimeMillis + totalWaitTimeMs

    while (!driverUp && !finished && System.currentTimeMillis < deadline) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Driver now available: %s:%s".format(driverHost, driverPort))
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at %s:%s, retrying ...".
            format(driverHost, driverPort))
          Thread.sleep(100L)
      }
    }

    if (!driverUp) {
      throw new SparkException("Failed to connect to driver!")
    }

    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)

    // 运行AMEndpoint服务
    createSchedulerRef(driverHost, driverPort.toString)
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter(driver: Option[RpcEndpointRef]) = {

    logInfo("添加 Yarn IP 过滤器， 以保证 UI 属性")
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    driver match {
      case Some(d) =>
        d.send(AddWebUIFilter(amFilter, params.toMap, proxyBase))

      case None =>
        System.setProperty("spark.ui.filters", amFilter)
        params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    }

    logInfo("添加 Yarn IP 过滤器  addAmIpFilter# params: "+ params)

  }

  /**
   * Start the user class, which contains the spark driver, in a separate Thread.
   * If the main routine exits cleanly or exits with System.exit(N) for any N
   * we assume it was successful, for all other cases we assume failure.
   *
   * Returns the user thread that was started.
    *
    *
    * 用一个单独的线程， 启动 spark driver ，运行用户类
    *如果main 方法 退出或者使用 System.exit(N) 退出，我们认为程序已经运行完成，否者认为运行失败
   */
  private def startUserApplication(): Thread = {

    logInfo("Starting the user application in a separate Thread")
    logInfo("用一个单独的线程， 启动 spark driver ，运行用户类")
    var userArgs = args.userArgs
    logInfo("用户参数： userArgs ：" + userArgs)

    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      // When running pyspark, the app is run using PythonRunner. The second argument is the list
      // of files to add to PYTHONPATH, which Client.scala already handles, so it's empty.
      userArgs = Seq(args.primaryPyFile, "") ++ userArgs
    }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      // TODO(davies): add R dependencies here
    }


//    args.userClass = "WordCount"
    val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])
    logInfo("获取main方法  用户类加载器 userClassLoader ：" + userClassLoader)
    logInfo("获取main方法   mainMethod ：" + mainMethod)


    val userThread = new Thread {
      override def run() {
        try {
          mainMethod.invoke(null, userArgs.toArray)

          logInfo("输出状态信息。。。。")
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)

          logDebug("Done running users class")
          logDebug("用户类正在运行")

        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case SparkUserAppException(exitCode) =>
                val msg = s"User application exited with status $exitCode"
                logError(msg)
                finish(FinalApplicationStatus.FAILED, exitCode, msg)
              case cause: Throwable =>
                logError("User class threw exception: " + cause, cause)
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + StringUtils.stringifyException(cause))
            }
            sparkContextPromise.tryFailure(e.getCause())
        } finally {
          // Notify the thread waiting for the SparkContext, in case the application did not
          // instantiate one. This will do nothing when the user code instantiates a SparkContext
          // (with the correct master), or when the user code throws an exception (due to the
          // tryFailure above).
          sparkContextPromise.trySuccess(null)
        }
      }
    }
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

  private def resetAllocatorInterval(): Unit = allocatorLock.synchronized {
    nextAllocationInterval = initialAllocationInterval
    allocatorLock.notifyAll()
  }

  /**
   * An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
   */
  private class AMEndpoint(override val rpcEnv: RpcEnv, driver: RpcEndpointRef)
    extends RpcEndpoint with Logging {

    override def onStart(): Unit = {
      driver.send(RegisterClusterManager(self))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        Option(allocator) match {
          case Some(a) =>
            if (a.requestTotalExecutorsWithPreferredLocalities(r.requestedTotal,
              r.localityAwareTasks, r.hostToLocalTaskCount, r.nodeBlacklist)) {
              resetAllocatorInterval()
            }
            context.reply(true)

          case None =>
            logWarning("Container allocator is not ready to request executors yet.")
            context.reply(false)
        }

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)

      case GetExecutorLossReason(eid) =>
        Option(allocator) match {
          case Some(a) =>
            a.enqueueGetLossReasonRequest(eid, context)
            resetAllocatorInterval()
          case None =>
            logWarning("Container allocator is not ready to find executor loss reasons yet.")
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      // In cluster mode, do not rely on the disassociated event to exit
      // This avoids potentially reporting incorrect exit codes if the driver fails
      if (!isClusterMode) {
        logInfo(s"Driver terminated or disconnected! Shutting down. $remoteAddress")
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
      }
    }
  }

  private def doAsUser[T](fn: => T): T = {
    ugi.doAs(new PrivilegedExceptionAction[T]() {
      override def run: T = fn
    })
  }

}

object ApplicationMaster extends Logging {

  // exit codes for different causes, no reason behind the values
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15
  private val EXIT_EARLY = 16

  private var master: ApplicationMaster = _

  def main(args: Array[String]): Unit = {
    logInfo("注册日志。。。。")
    SignalUtils.registerLogger(log)
    logInfo("获取参数。。。。")
    val amArgs = new ApplicationMasterArguments(args)
    logInfo("创建ApplicationMaster对象。。。。")
    master = new ApplicationMaster(amArgs)
    logInfo("执行！！！  调用ApplicationMaster对象的run方法")
    System.exit(master.run())


  }

  private[spark] def sparkContextInitialized(sc: SparkContext): Unit = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def getAttemptId(): ApplicationAttemptId = {
    master.getAttemptId
  }

  private[spark] def getHistoryServerAddress(
      sparkConf: SparkConf,
      yarnConf: YarnConfiguration,
      appId: String,
      attemptId: String): String = {
    sparkConf.get(HISTORY_SERVER_ADDRESS)
      .map { text => SparkHadoopUtil.get.substituteHadoopVariables(text, yarnConf) }
      .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}/${attemptId}" }
      .getOrElse("")
  }
}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
 */
object ExecutorLauncher {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hadoop")

    val sparkConf = new SparkConf()

    val driverUrl = ""

    val yarnConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

    val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
    val executorCores = sparkConf.get(EXECUTOR_CORES)

    val appId = "appId"
    val attemptId = "attemptId"

    val securityMgr = new SecurityManager(sparkConf)



    var localResources:Map[String,LocalResource] = Map()



    val dummyRunner = new ExecutorRunnable(None, yarnConf, sparkConf, driverUrl, "<executorId>",
      "<hostname>", executorMemory, executorCores, appId, securityMgr, localResources)


    dummyRunner.launchContextDebugInfo()


    //  commands
    //  0 = "{{JAVA_HOME}}/bin/java"
    //  1 = "-server"
    //  2 = "-Xmx1024m"
    //  3 = "-Djava.io.tmpdir={{PWD}}/tmp"
    //  4 = "-Dspark.yarn.app.container.log.dir=<LOG_DIR>"
    //  5 = "-XX:OnOutOfMemoryError='kill %p'"
    //  6 = "org.apache.spark.executor.CoarseGrainedExecutorBackend"
    //  7 = "--driver-url"
    //  8 = ""
    //  9 = "--executor-id"
    //  10 = "<executorId>"
    //  11 = "--hostname"
    //  12 = "<hostname>"
    //  13 = "--cores"
    //  14 = "1"
    //  15 = "--app-id"
    //  16 = "appId"
    //  17 = "--user-class-path"
    //  18 = "file:$PWD/__app__.jar"
    //  19 = "1><LOG_DIR>/stdout"
    //  20 = "2><LOG_DIR>/stderr"


    //  env
    //  0 = {Tuple2@3027} "(CLASSPATH,{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>/usr/hdp/2.6.5.0-292/hadoop/conf<CPS>/usr/hdp/2.6.5.0-292/hadoop/*<CPS>/usr/hdp/2.6.5.0-292/hadoop/lib/*<CPS>/usr/hdp/current/hadoop-hdfs-client/*<CPS>/usr/hdp/current/hadoop-hdfs-client/lib/*<CPS>/usr/hdp/current/hadoop-yarn-client/*<CPS>/usr/hdp/current/hadoop-yarn-client/lib/*<CPS>/usr/hdp/current/ext/hadoop/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__)"
    //  1 = {Tuple2@3028} "(SPARK_CONF_DIR,/workspace/spark-2.3.2/conf)"
    //  2 = {Tuple2@3029} "(SPARK_HOME,/workspace/spark-2.3.2)"



  }




}
