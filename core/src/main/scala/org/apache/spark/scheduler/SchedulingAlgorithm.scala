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

package org.apache.spark.scheduler

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}


//  FIFO排序类中的比较函数的实现很简单：
//  Schedulable A和Schedulable B的优先级，优先级值越小，优先级越高
//  A优先级与B优先级相同，若A对应stage id越小，优先级越高
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {

    // 先比较priority，即优先级
    // priority相同的话，再比较stageId
    // 前者小于后者的话，返回true，否则为false

    // 先比较两个TaskSetManagerder的优先级priority，
    // 优先级相同再比较stageId。而这个priority在TaskSet生成时，就是jobId，
    // 也就是FIFO是先按照Job的顺序再按照Stage的顺序进行顺序调度，一个Job完了再调度另一个Job，
    // Job内是按照Stage的顺序进行调度。关于priority生成的代码如下所示：

    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}



//  结合以上代码，我们可以比较容易看出Fair调度模式的比较逻辑：
//
//  正在运行的task个数小于   最小共享核心数的要比不小于的优先级高
//  若两者正在运行的task个数都小于最小共享核心数，则比较minShare使用率的值，
//  即runningTasks.toDouble / math.max(minShare, 1.0).toDouble，越小则优先级越高
//  若minShare使用率相同，则比较权重使用率，即runningTasks.toDouble / s.weight.toDouble，越小则优先级越高
//  如果权重使用率还相同，则比较两者的名字
//
//  对于Fair调度模式，需要先对RootPool的各个子Pool进行排序，再对子Pool中的TaskSetManagers进行排序，
//  使用的算法都是FairSchedulingAlgorithm.FairSchedulingAlgorithm

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0

    // 前者的runningTasks<minShare而后者相反的的话，返回true；
    // runningTasks为正在运行的tasks数目，minShare为最小共享cores数；
    // 前面两个if判断的意思是两个TaskSetManager中，
    // 如果其中一个正在运行的tasks数目小于最小共享cores数，
    // 则优先调度该TaskSetManager


    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      // 如果两者的正在运行的tasks数目都比最小共享cores数小的话，再比较minShareRatio
      // minShareRatio为正在运行的tasks数目与最小共享cores数的比率
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      // 最后比较taskToWeightRatio，即权重使用率，weight代表调度池对资源获取的权重，越大需要越多的资源
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
  }
}

