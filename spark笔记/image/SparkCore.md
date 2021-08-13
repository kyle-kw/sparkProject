

[toc]



# 一、Spark运行架构

## 1.1	运行架构

Spark框架的核心是一个计算引擎，它采用了标准的master-slave的结构。

当Spark执行时的基本结构如下：

​	![image-20210809112559010](./image/SparkCore_RDD_Acc_Bc/image-20210809112559010.png)

图中Driver表示master，复制整个集群中的作业任务调度。Executor表示slave，负责实际执行任务。



## 1.2	核心组件

**Driver**

Spark 驱动器节点，用于执行 Spark 任务中的 main 方法，负责实际代码的执行工作。Driver 在 Spark 作业执行时主要负责：

- 将用户程序转化为作业（job）
- 在 Executor 之间调度任务(task)
- 跟踪 Executor 的执行情况
- 通过 UI 展示查询运行情况

简单理解，所谓的 Driver 就是驱使整个应用运行起来的程序，也称之为Driver 类。



**Executor**

Spark Executor 是集群中工作节点（Worker）中的一个 JVM 进程，负责在 Spark 作业中运行具体任务（Task），任务彼此之间相互独立。Spark 应用启动时，Executor 节点被同时启动，并且始终伴随着整个 Spark 应用的生命周期而存在。如果有 Executor 节点发生了故障或崩溃，Spark 应用也可以继续执行，会将出错节点上的任务调度到其他 Executor 节点上继续运行。

Executor 有两个核心功能：

- 负责运行组成 Spark 应用的任务，并将结果返回给驱动器进程
- 它们通过自身的块管理器（Block Manager）为用户程序中要求缓存的 RDD 提供内存式存储。RDD  是直接缓存在 Executor 进程内的，因此任务可以在运行时充分利用缓存数据加速运算。



## 1.3	组件简单展示

现在来简单演示一下，Driver和Executor在Spark中的运行方式。

### 1.3.1	简单传输

首先创建一个Executor对象

```scala
package com.local.text

import java.io.InputStream
import java.net.{ServerSocket, Socket}

object Executor {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据...")

    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream

    val i: Int = in.read()

    println("客户端接收到的数据：" + i)

    in.close()
    client.close()
    server.close()

  }
}
```



创建一个Driver对象

```scala
package com.local.text

import java.io.OutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {

    val client = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream

    out.write(10)
    out.flush()
    out.close()

    client.close()
  }
}

```



先启动Executor，再运行Driver，就可以看到，Executor端接收到的数据：

​	![image-20210809115035591](./image/SparkCore_RDD_Acc_Bc/image-20210809115035591.png)



### 1.3.2	传输逻辑运算以及数据

接下来复杂一点，上面只是传过去了一个整数，在Driver和Executor端我们需要将逻辑运算进行传输，现在试着将计算传过去。

创建一个Task类，来实现将数据乘2

```scala
package com.local.text

class Task extends Serializable {

  // 数据
  val datas = List(1,2,3,4)

  // 计算逻辑
  val logic:(Int) => Int = _ * 2

  // 进行计算
  def computer() ={
    datas.map(logic)
  }
}

```

要注意这里的Task类继承了**Serializable**，使他可以进行序列化，就可以进行传输了。



重写Driver和Executor对象

```scala
package com.local.text

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket}

object Executor {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务器启动，等待接收数据")

    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task:Task = objIn.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.computer()

    println("计算节点计算的结果：" + ints.mkString(","))

    objIn.close()
    client.close()
    server.close()

  }
}
```



```scala
package com.local.text

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {

    val client = new Socket("localhost", 9999)

    val out: OutputStream = client.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val task = new Task()

    objOut.writeObject(task)
    objOut.flush()
    objOut.close()

    client.close()
    println("客户端数据发送完毕。")
  }
}
```



结果如下：

​	![image-20210809125705159](./image/SparkCore_RDD_Acc_Bc/image-20210809125705159.png)





### 1.3.3	分布式计算

上面计算的节点只有一个，现实中，有多个计算节点。下面我们尝试一下将数据分开，分别进行计算。

为了将数据可以分割开，我们创建一个Task来专门存储数据和计算逻辑：

```scala
package com.local.text

class Task extends Serializable {

  val datas = List(1,2,3,4)
  val logic : (Int)=>Int = _ * 2

}
```

创建一个SubTask，来放入计算和选择逻辑：

```scala
package com.local.text

class SubTask extends Serializable {

  var datas:List[Int] = _
  var logic:(Int)=>Int = _

  def computer():List[Int] = {
    datas.map(logic)
  }
}
```



接下来创建Driver和Executor来进行通信。

```scala
package com.local.text

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
    
  def main(args: Array[String]): Unit = {
      
    val server1 = new ServerSocket(9999)
    val server2 = new ServerSocket(8888)
    println("客户端启动，等待连接...")

    // 客户端1
    val client1: Socket = server1.accept()
    val in1: InputStream = client1.getInputStream
    val objIn1 = new ObjectInputStream(in1)
    val subTask1: SubTask = objIn1.readObject().asInstanceOf[SubTask]
    val ints1: List[Int] = subTask1.computer()
    println("计算节点1,计算的结果为：" + ints1)
      
    objIn1.close()
    client1.close()
    server1.close()

    // 客户端2
    val client2: Socket = server2.accept()
    val in2: InputStream = client2.getInputStream
    val objIn2 = new ObjectInputStream(in2)
    val subTask2: SubTask = objIn2.readObject().asInstanceOf[SubTask]
    val ints2: List[Int] = subTask2.computer()
    println("计算节点2,计算的结果为：" + ints2)
      
    objIn1.close()
    client1.close()
    server1.close()
  }
}
```



```scala
package com.local.text

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Deiver {

  def main(args: Array[String]): Unit = {
      
    // 客户端1、2
    val client1 = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)

    val task = new Task()
    val out1: OutputStream = client1.getOutputStream
    val out2: OutputStream = client2.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)
    val objOut2 = new ObjectOutputStream(out2)

    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.datas = task.datas.take(2)

    objOut1.writeObject(subTask)
    objOut1.flush()
    objOut1.close()
    client1.close()

    subTask.datas = task.datas.takeRight(2)
    objOut2.writeObject(subTask)
    objOut2.flush()
    objOut2.close()
    objOut2.close()

    println("客户端数据发送完毕。")
  }
}
```

结果如下：

​	![image-20210809130657528](./image/SparkCore_RDD_Acc_Bc/image-20210809130657528.png)



这样就基本实现了，多Executor节点，一个Driver节点的模式了。

这里的SubTask类，就类似于Spark中RDD的功能，用于保存计算方式，而不保存数据。





## 1.4	核心概念



**Executor 与 Core**

​		Spark Executor 是集群中运行在工作节点（Worker）中的一个 JVM 进程，是整个集群中的专门用于计算的节点。在提交应用中，可以供参数指定计算节点的个数，以及对应的资源。这里的资源一般指的是工作节点 Executor 的内存大小和使用的虚拟 CPU 核（Core）数量。

​		应用程序相关启动参数如下：

​	![image-20210809141129107](./image/SparkCore_RDD_Acc_Bc/image-20210809141129107.png)



**并行度**

​		在分布式计算框架中一般都是多个任务同时执行，由于任务分布在不同的计算节点进行计算，所以能够真正地实现多任务并行执行，记住，这里是并行，而不是并发，可以理解为一个cpu运行一个任务，多个cpu运行多个任务就是并行；一个cpu运行多个任务（多线程），就是并发。这里我们将整个集群并行执行任务的数量称之为并行度。那么一个作业到底并行度是多少呢？这个取决于框架的默认配置。应用程序也可以在运行过程中动态修改。



**有向无环图**

​		![image-20210809141301976](./image/SparkCore_RDD_Acc_Bc/image-20210809141301976.png)

​		大数据计算引擎框架我们根据使用方式的不同一般会分为四类，其中第一类就是Hadoop 所承载的 MapReduce,它将计算分为两个阶段，分别为 Map 阶段 和 Reduce 阶段。对于上层应用来说，就不得不想方设法去拆分算法，甚至于不得不在上层应用实现多个 Job的串联，以完成一个完整的算法，例如迭代计算。 由于这样的弊端，催生了支持 DAG 框架的产生。因此，支持 DAG  的框架被划分为第二代计算引擎。如 Tez 以及更上层的Oozie。这里我们不去细究各种 DAG 实现之间的区别，不过对于当时的 Tez 和 Oozie 来说，大多还是批处理的任务。接下来就是以 Spark 为代表的第三代的计算引擎。第三代计算引擎的特点主要是 Job 内部的 DAG 支持（不跨越 Job），以及实时计算。

​		这里所谓的有向无环图，并不是真正意义的图形，而是由 Spark 程序直接映射成的数据流的高级抽象模型。**简单理解**就是将整个程序计算的执行过程用图形表示出来,这样更直观，更便于理解，可以用于表示程序的拓扑结构。

假设运行A依赖于B，而运行B依赖于C和D，它们之间的关系如下：

​	![image-20210809142517806](./image/SparkCore_RDD_Acc_Bc/image-20210809142517806.png)

此时可正常运行，又假设运行D依赖于A，它们之间的关系如下：

​	![image-20210809142734666](./image/SparkCore_RDD_Acc_Bc/image-20210809142734666.png)

此时不能正常运行，因为它们之间形成了一个闭环，A依赖B，B依赖D，D依赖A，永远无法达成条件。

因此正常的程序应该遵循有向无环图。





# 二、Spark核心编程



Spark 计算框架为了能够进行高并发和高吞吐的数据处理，封装了三大数据结构，用于处理不同的应用场景。三大数据结构分别是：

- RDD : 弹性分布式数据集

- 累加器：分布式共享只写变量

- 广播变量：分布式共享只读变量



## 2.1	RDD弹性分布式数据集

RDD（Resilient Distributed Dataset）叫做弹性分布式数据集，是 Spark 中最基本的数据处理模型。代码中是一个抽象类，它代表一个弹性的、不可变、可分区、里面的元素可并行计算的集合。

RDD和IO中流的概念很像，我们从IO流开始说。



### 2.1.1	RDD 和 IO流

IO流是Input和Output的简称，其中包括**字节流**和**字符流**。

首先创建一个简单的流，直接读取文件并打印出来：

​	![image-20210809143916598](./image/SparkCore_RDD_Acc_Bc/image-20210809143916598.png)

这里的流，当开始读取时，读取到一个字节，就打印一个字节。感觉一个一个字节打印太乱了，我们就想让它累计到一定字节之后，再打印输出。这时我们就需要加上一个缓冲区：

​	![image-20210809144253500](./image/SparkCore_RDD_Acc_Bc/image-20210809144253500.png)





有的字符是由多个字节组成，所以我们希望当读到的字节构成一个字符时，这是就需要将**字节**转化为**字符**。

​	![image-20210809144634078](./image/SparkCore_RDD_Acc_Bc/image-20210809144634078.png)



IO操作体现了装饰者设计模式。

现在思考一个问题，当一个流构建好之后，文件（数据）开始读取了么？

答案是没有，只有我们进行read操作之后，文件（数据）才开始读取。**没有进行read之前，流对象只是保留了怎样读取文件的操作，而没有进行读取文件。**



**RDD的数据处理方式类似于IO流，也有装饰者设计模式。**

以WordCount为例，RDD的封装如下：

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("data/input/words.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordsToOne: RDD[(String, Int)] = words.map((_, 1))
    val res: RDD[(String, Int)] = wordsToOne.reduceByKey(_ + _)

    res.collect().foreach(println)

    sc.stop()
  }
}
```



​	![image-20210809145426941](./image/SparkCore_RDD_Acc_Bc/image-20210809145426941.png)

RDD在textFile、flatMap、map、reduceByKey时，只是将RDD一层层的封装起来，没有真的读数据，RDD中不保存数据，保存的是处理数据的逻辑。只有当运行到collect时，才会开始读数据，进行计算输出。

RDD的数据只有在调用collect方法时，才会真正执行业务逻辑操作。之前的封装全部都是功能的扩展。

这就是RDD和IO流的区别：RDD是不保存数据的，但是IO可以临时保存一部分数据。





### 2.1.2	RDD的特点



**弹性**

- 存储的弹性：内存与磁盘的自动切换；

- 容错的弹性：数据丢失可以自动恢复；

- 计算的弹性：计算出错重试机制；

- 分片的弹性：可根据需要重新分片。

**分布式**：数据存储在大数据集群不同节点上

**数据集**：RDD 封装了计算逻辑，并不保存数据

**数据抽象**：RDD 是一个抽象类，需要子类具体实现

**不可变**：RDD 封装了计算逻辑，是不可以改变的，想要改变，只能产生新的 RDD，在新的 RDD 里面封装计算逻辑

**可分区**、并行计算





### 2.1.3	RDD 核心属性

​	![image-20210809150820560](./image/SparkCore_RDD_Acc_Bc/image-20210809150820560.png)

RDD有5个核心属性：

**分区列表**

​		RDD 数据结构中存在分区列表，用于执行任务时并行计算，是实现分布式计算的重要属性。

​	![image-20210809150949955](./image/SparkCore_RDD_Acc_Bc/image-20210809150949955.png)



**分区计算函数**

​		Spark 在计算时，是使用分区函数对每一个分区进行计算。

​	![image-20210809151137963](./image/SparkCore_RDD_Acc_Bc/image-20210809151137963.png)



**RDD之间的依赖关系**

​		RDD 是计算模型的封装，当需求中需要将多个计算模型进行组合时，就需要将多个 RDD 建立依赖关系。

​	![image-20210809151300656](./image/SparkCore_RDD_Acc_Bc/image-20210809151300656.png)



**分区器（可选）**

​		当数据为 KV 类型数据时，可以通过设定分区器自定义数据的分区。

​	![image-20210809151430807](./image/SparkCore_RDD_Acc_Bc/image-20210809151430807.png)



**首选位置（可选）**

​		计算数据时，可以根据计算节点的状态选择不同的节点位置进行计算。

​	![image-20210809151520065](./image/SparkCore_RDD_Acc_Bc/image-20210809151520065.png)





### 2.1.4	RDD执行原理

从计算的角度来讲，数据处理过程中需要计算资源（内存 & CPU）和计算模型（逻辑）。执行时，需要将计算资源和计算模型进行协调和整合。

Spark 框架在执行时，先申请资源，然后将应用程序的数据处理逻辑分解成一个一个的计算任务。然后将任务发到已经分配资源的计算节点上, 按照指定的计算模型进行数据计算。最后得到计算结果。





## 2.2	RDD基础



### 2.2.1	RDD创建

在 Spark 中创建 RDD 的创建方式可以分为四种：

1. **从集合（内存）中创建 RDD**

从集合中创建RDD，Spark主要提供了两个方法：**parallelize** 和 **makeRDD**

```scala
val seq: Seq[Int] = Seq[Int](1,2,3,4)

val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
val rdd2: RDD[Int] = sc.parallelize(seq)
val rdd3: RDD[Int] = sc.makeRDD(seq)
```

查看一下makeRDD的源码：

```scala
def makeRDD[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }
```

可以看出来makeRDD底层还是调用的parallelize方法，只不过让创建RDD更加清晰、方便。



2. **从外部存储（文件）创建 RDD**

   ​	由外部存储系统的数据集创建 RDD 包括：本地的文件系统，所有 Hadoop 支持的数据集，比如 HDFS、HBase 等。

```scala
// 从文件中创建RDD
val rdd: RDD[String] = sc.textFile("data/input/words.txt")
// 支持模糊匹配
val rdd2: RDD[String] = sc.textFile("data/input/words*.txt")
// 支持以整个文件读取
val rdd3: RDD[(String, String)] = sc.wholeTextFiles("data/input/words*.txt")
```

​		整个文件读取的格式如下：

​	![image-20210809153246216](./image/SparkCore_RDD_Acc_Bc/image-20210809153246216.png)



3. **从其他 RDD 创建**

主要是通过一个 RDD 运算完后，再产生新的 RDD。



4. **直接创建 RDD（new）**

使用 new 的方式直接构造 RDD，一般由 Spark 框架自身使用。





### 2.2.2	RDD 并行度与分区

默认情况下，Spark 可以将一个作业切分多个任务后，发送给 Executor 节点并行计算，而能 够并行计算的任务数量我们称之为并行度。这个数量可以在构建 RDD 时指定。

```scala
//从内存中创建RDD,并制定分区数量为2
val rdd1: RDD[Int] = sc.parallelize(
      List(1, 2, 3, 4),2
    )
```



#### 内存数据

数据是怎么被切分的？

读取**内存数据**时，切分数据的源码如下：

```scala
def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
  (0 until numSlices).iterator.map { i =>
    val start = ((i * length) / numSlices).toInt
    val end = (((i + 1) * length) / numSlices).toInt
    (start, end)
  }
}
```

**length**表示数据的长度，**numSlices**表示指定分区的数量。

```scala
// 假设数据为List(1,2,3,4,5,6,7)，分区为3
// length为7，numSlices为3
// 第一个分区索引：[0,2) => (1,2)
// 第二个分区索引：[2,4) => (3,4)
// 第三个分区索引：[4,7) => (5,6,7)
```

我们来实验一下：

```scala
package com.local.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
      
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从内存中创建RDD,并制定分区
    val rdd1: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4,5,6,7),3
    )
    rdd1.saveAsTextFile("data/output/result5")

    sc.stop()
  }
}
```

​	输出三个文件，数量没错

​	![image-20210809160521464](./image/SparkCore_RDD_Acc_Bc/image-20210809160521464.png)

查看一下每个文件对应的数据是不是我们计算的那样

​	![image-20210809160658933](./image/SparkCore_RDD_Acc_Bc/image-20210809160658933.png)

​	![image-20210809160710048](./image/SparkCore_RDD_Acc_Bc/image-20210809160710048.png)

​	![image-20210809160723725](./image/SparkCore_RDD_Acc_Bc/image-20210809160723725.png)

和我们计算的结果一样。

若不指定分区数量，会有一个默认分区数量，它等同于你设置Spark参数时“local[*]”设置的cpu核数，\*就是你电脑的全部cpu核数。



#### 文件数据

读取**文件数据**时，使用textFile进行读取，默认也可以进行设定分区。minPartitions：最小分区数量。查看它的值math.min(defaultParallelism, 2)，所以此时默认的分区数量为2。

我们也可以通过textFile第二个参数指定分区数minPartitions

为什么叫**minPartitions**，因为Spark读取文件底层是Hadoop的读取方式，遵循1.1倍原则，若（剩余块大小/块大小）>0.1，就会产生一个新的分区，这就是为什么叫做最小分区数。

```scala
/**假设有一个文件有7个字节，设置最小分区数量为2
 * 7/2 = 3.....1
 * 1/3 = 0.33 > 0.1
 * 则会产生3个分区
 */
```



数据分区完成之后，怎么切割数据？

1. 数据以行为单位进行读取

   Spark读取数据，采用的是Hadoop的方式进行读取，所以一行一行的读，和字节数没有关系。

   

2. 数据读取时以偏移量为单位，不会重复读取

   假设有一个文件1.txt，它的内容如下：

   ​	![image-20210809173717179](./image/SparkCore_RDD_Acc_Bc/image-20210809173717179.png)

   则它的字节数为7（加上回车键），偏移量：

   ```scala
   /**文件    偏移量
    * 1@@  => 012
    * 2@@  => 345
    * 3    => 6
    */
   ```

   设置最小分区数为2，最后产生的分区数为3。

   

3. 数据分区的偏移量范围的计算

   ```scala
   /**分区    偏移量范围
    * 0   => [0, 3]
    * 1   => [3, 6]
    * 2   => [6, 7]
    */
   
   ```

   

4. 根据以上原则，可以进行划分数据

   ```scala
   /**分区    数据
    * 0   => [1@@2@@]
    * 1   => [3]
    * 2   => []
    */
   ```

来尝试一下

​	![image-20210809174801119](./image/SparkCore_RDD_Acc_Bc/image-20210809174801119.png)

文件有三个，分区数量一致，查看各个分区的数据是否一致。

​	![image-20210809174914633](./image/SparkCore_RDD_Acc_Bc/image-20210809174914633.png)

​	![image-20210809175014563](./image/SparkCore_RDD_Acc_Bc/image-20210809175014563.png)

​	![image-20210809174951770](./image/SparkCore_RDD_Acc_Bc/image-20210809174951770.png)

和我们计算的结果一致。



具体 Spark 核心源码如下：

```java
public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    long totalSize = 0;	// compute total size
    for (FileStatus file: files) {	// check we have valid files
        if (file.isDirectory()) {
            throw new IOException("Not a file: "+ file.getPath());
        }
        totalSize += file.getLen(); 
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
                                        FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    ...

    for (FileStatus file: files) {

    	...

    if (isSplitable(fs, path)) {
        long blockSize = file.getBlockSize();
        long splitSize = computeSplitSize(goalSize, minSize, blockSize);
    	...
}
protected long computeSplitSize(long goalSize, long minSize,
                                long blockSize) {
    return Math.max(minSize, Math.min(goalSize, blockSize)); 
}
```



## 2.3	RDD 方法

RDD方法分为两类：

1. 转换：功能的补充和封装，将旧的RDD包装成新的RDD
   - 例如：flatMat、map、reduceByKey...
2. 行动：触发任务的调度和作业的执行
   - 例如：collect、foreach.....



RDD的方法又被称为算子。在认知心理学中认为解决问题就是把问题的状态改变。

>  问题（初始） => 操作（算子） => 问题（审核中） => 操作（算子） => 问题（完成）

这里的操作就是Operator，算子。



### 2.3.1	RDD转换算子

RDD 根据数据处理方式的不同将算子整体上分为 Value 类型、双 Value 类型和 Key-Value类型，每种类型都有独特的转换算子。



#### 2.3.1.1	Value类型



- **map**

  - 函数签名

  ```scala
  def map[U: ClassTag](f: T => U): RDD[U]
  ```
  
  - 函数解释
  
  将处理的数据**逐条**进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
  
  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
  val rdd1: RDD[Int] = rdd.map(_ * 2)
  
  val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")
  val rdd1: RDD[String] = rdd.map(_.split(" ")(6))
  ```
  
  
  
- **mapPartitons**

  - 函数签名

  ```scala
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U]
  ```
  
  - 函数说明
  
  将待处理的数据**以分区为单位**发送到计算节点进行处理，这里的处理是指可以进行任意的处理，可以是过滤数据。
  
  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
  val rdd1: RDD[Int] = rdd.mapPartitions(iter => {
    println(">>>>>>>>>>")
    iter.map(_ * 2)
  })
  // 这里会打印两个>>>>>>>>>>>，因为设置了两个分区
  
  
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
  val rdd1: RDD[Int] = rdd.mapPartitions(iter => {
    // iter.filter(_ == iter.max)
    List(iter.max).iterator
  })
  // 这里会将每个分区的最大值拿出来（过滤）
  ```



**map**和**mapPartitions**有什么区别？

1. 数据处理的角度

   map算子是分区内一个一个数据的执行，类似于串行操作。而mapPartitons算子是以分区为单位进行的批处理操作。

2. 功能的角度

   Map 算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。

   MapPartitions 算子需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不变，所以可以增加或减少数据

3. 性能的角度

   Map 算子因为类似于串行操作，所以性能比较低，而是 mapPartitions 算子类似于批处理，所以性能较高。但是 mapPartitions 算子会长时间占用内存，那么这样会导致内存可能不够用，出现内存溢出的错误。所以在内存有限的情况下，不推荐使用。使用 map 操作。

   完成比完美更重要



- **mapPartitionsWithIndex**

  - 函数签名

  ```scala
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U]
  ```
  
  - 函数说明
  
  将待处理的数据以**分区为单位**发送到计算节点进行处理，在处理时同时可以获取当前分区索引。
  
  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
  val rdd1: RDD[Int] = rdd.mapPartitionsWithIndex(
    (index, iter) => {
      if (index == 1) {
        iter
      } else {
        Nil.iterator
      }
    }
  )
  // 将分区编号为1的数据筛选出来
  
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
  val rdd1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
    (index, iter) => {
      iter.map((index, _))
    }
  )
  // 将每个数据前面加上分区编号
  ```



- **flatMap**

  - 函数签名

  ```scala
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
  ```

  - 函数说明

  将处理的数据进行**扁平化**后再进行映射处理，所以此算子也称之为扁平映射。

  ```scala
  val rdd: RDD[List[Int]] = sc.makeRDD(List(
    List(1, 2), List(3, 4)
  ))
  val rdd1: RDD[Int] = rdd.flatMap(l => l)
  // 将数据转换为（1,2,3,4）
  
  
  val rdd: RDD[String] = sc.makeRDD(List(
    "Hello World", "Hello Spark"
  ))
  val rdd1: RDD[String] = rdd.flatMap(l => l.split(" "))
  // 将数据进行切分 （Hello，world，hello，Spark）
  
  
  val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
  val rdd1: RDD[Any] =rdd.flatMap {
    case list: List[_] => list
    case dat => List(dat)
  }
  // 多种数据类型进行扁平化，可以进行条件筛选case
  ```



- **glom**

  - 函数签名

  ```scala
  def glom(): RDD[Array[T]]
  ```

  - 函数说明

  将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
  val glomRDD: RDD[Array[Int]] = rdd.glom()
  glomRDD.collect().foreach(data => println(data.mkString(",")))
  // 1,2   3,4
  
  
  val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
  val glomRDD: RDD[Array[Int]] = rdd.glom()
  val maxRDD: RDD[Int] = glomRDD.map(_.max)
  // 2,4
  ```



- **groupBy**

  - 函数签名

  ```scala
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])]
  ```

  - 函数说明

  将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 **shuffle**。极限情况下，数据可能被分在同一个分区中。

  一个组的数据在一个分区中，但是并不是说一个分区中只有一个组

  ```scala
  val rdd: RDD[String] = sc.makeRDD(List("hadoop","hello","spark","scala"))
  val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
  groupRDD.collect().foreach(println)
  // (h,CompactBuffer(hadoop, hello))
  // (s,CompactBuffer(spark, scala))
  
  
  val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")
  val rdd1: RDD[(String, Int)] = rdd.map(line => (line.split(" ")(3).split(":")(1), 1))
  val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(_._1)
  val groupRDD1: RDD[(String, Int)] = groupRDD.map {
    case (string, iter) => {
      (string, iter.size)
    }
  }
  groupRDD1.collect().foreach(println)
  // 从服务器日志数据 apache.log 中获取每个时间段访问量
  ```

  ​	![image-20210810115201450](./image/SparkCore_RDD_Acc_Bc/image-20210810115201450.png)



- **filter**

  - 函数签名

  ```scala
  def filter(f: T => Boolean): RDD[T]
  ```

  - 函数说明

  将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。

  当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。

  ```scala
  val rdd: RDD[String] = sc.makeRDD(List("hadoop","hello","spark","scala"))
  val filterRDD: RDD[String] = rdd.filter(_.charAt(0).equals('h'))
  // 将第一个字母为h的单词，筛选出来
  
  
  val rdd: RDD[String] = sc.textFile("data/input/data/apache.log")
  rdd.filter(
    line => {
      val time: String = line.split(" ")(3)
      time.startsWith("17/05/2015")
    }
  ).collect().foreach(println)
  // 从服务器日志数据 apache.log 中获取2015年5月17日的请求路径
  ```



- **sample**

  - 函数签名

  ```scala
  def sample(
      withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T]
  ```

  - 函数说明

  根据指定的规则从数据集中**抽取**数据。

  ```scala
  val dataRDD = sc.makeRDD(List(1,2,3,4),1)
  // 抽取数据不放回（伯努利算法）
  // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
  // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
  // 第一个参数：抽取的数据是否放回，false：不放回
  // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
  // 第三个参数：随机数种子
  val dataRDD1 = dataRDD.sample(false, 0.5)
  
  
  // 抽取数据放回（泊松算法）
  // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
  // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
  // 第三个参数：随机数种子
  val dataRDD2 = dataRDD.sample(true, 2)
  ```



- **distinct**

  - 函数签名

  ```scala
  def distinct(): RDD[T]
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
  ```

  - 函数说明

  将数据集中的重复数据去重。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1,2,4,6))
  
  // 去重源码：
  // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
  rdd.distinct().collect().foreach(println)
  ```



- **coalesce**

  - 函数签名

  ```scala
  def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
      		: RDD[T]
  ```

  - 函数说明

  根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率。

  当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少分区的个数，减小任务调度成本。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1,2,4,6),4)
  rdd.coalesce(2).saveAsTextFile("data/output/result5")
  // 减少分区数量
  ```



- **repartition**

  - 函数签名

  ```scala
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
  ```

  - 函数说明

  该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition操作都可以完成，因为无论如何都会经 shuffle 过程。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),2)
  
  // 扩大分区使用repartition，底层使用的是coalesce(numPartitions, shuffle = true)
  // 缩减分区使用coalesce，如果想要数据均衡，可以使用shuffle
  val rdd1: RDD[Int] = rdd.repartition(3)
  ```



- **sortBy**

  - 函数签名

  ```scala
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
  ```

  - 函数说明

  该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一致。中间存在 shuffle 的过程。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 1,2,4,6),1)
  val rdd1: RDD[Int] = rdd.sortBy(num => num)
  // 按照数值进行升序排序
  
  
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)))
  val rdd1: RDD[(String, Int)] = rdd.sortBy(num => num._1.toInt,false)
  // 按照第一个值的降序排序
  ```







#### 2.3.1.2	双Value类型



- **intersection**

  - 函数签名

  ```scala
  def intersection(other: RDD[T]): RDD[T]
  ```

  - 函数说明

  返回这个 RDD 和另一个 RDD 的交集。 输出不会包含任何重复元素，即使输入 RDD 包含。此方法在内部执行 shuffle。



- **union**
  - 函数签名

  ```scala
  def union(other: RDD[T]): RDD[T]
  ```

  - 函数说明

  返回这个 RDD 和另一个 RDD 的并集。 任何相同的元素都会出现多次（可以使用 .distinct() 来消除它们）。 



- **subtract**

  - 函数签名

  ```scala
  def subtract(other: RDD[T]): RDD[T]
  ```

  - 函数说明

  返回一个 RDD，其中包含 this 中不在 other 中的元素。差集。

  

- **zip**

  - 函数签名

  ```scala
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)]
  ```

  - 函数说明

  用另一个 RDD 压缩这个 RDD，返回键值对，键值对中的 Key 为第 1 个 RDD 中的元素，Value 为第 2 个 RDD 中的相同位置的元素。前提：两个 RDD 具有**相同数量的分区**和**相同数量的元素**



```scala
package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Operator_transform {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("transform").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // todo 算子--双value类型
    // 交集、并集和差集的两个集合的数据类型必须相同
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    // 交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    // 并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链
    // 元素数量不同时会报错：
    // Can only zip RDDs with same number of elements in each partition
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()

  }

}
```





#### 2.3.1.3	Key - Value 类型



- **partitionBy**

  - 函数签名

  ```scala
  def partitionBy(partitioner: Partitioner): RDD[(K, V)]
  ```

  - 函数说明

  返回使用指定分区器分区的 RDD 的副本。将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner。

  如果重分区的分区器和当前 RDD 的分区器一样，则会返回它本身。

  除了默认分区器，其他分区器还有RangePartitioner。

  可以按照自己的方法进行数据分区，需要写一个分区器类继承Partitioner，它有两个抽象方法，实现这两个方法就可以了。

  ```scala
  // Partitioner抽象类
  abstract class Partitioner extends Serializable {
    def numPartitions: Int
    def getPartition(key: Any): Int
  }
  ```

  ```scala
  // 例子
  val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
  val rdd: RDD[(Int, Int)] = rdd1.map((_, 1))
  rdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("data/output/result5")
  ```



- **reduceByKey**

  - 函数签名

  ```scala
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
  def reduceByKey(func: (V, V) => V): RDD[(K, V)]
  ```

  - 函数说明

  可以将数据按照相同的 Key 对 Value 进行聚合

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("a", 3), ("c", 4)
  ))
  
  val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x:Int, y:Int)=>{
    println(s"x=${x}, y=${y}")
    x+y
  })
  
  reduceRDD.collect().foreach(println)
  /* 输出：
      x=1, y=2
      x=3, y=3
      (a,6)
      (c,4)
  */
  
  
  ```

  若Key对应的Value只有一个，就不会进行操作。



- **groupByKey**

  - 函数签名

  ```scala
  def groupByKey(): RDD[(K, Iterable[V])]
  ```

  - 函数说明

  将 RDD 中每个键的值分组为一个序列。 使用现有的分区器/并行级别对生成的 RDD 进行哈希分区。 每个组中元素的顺序是不能保证的，甚至在每次运行结果的 RDD 时都可能不同。
  此操作需要落盘处理（加载到本地磁盘中）。 如果分组是为了对每个键执行聚合（例如求和或平均值），则使用 aggregateByKey 或 reduceByKey 将提供更好的性能。

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("a", 3), ("c", 4)
  ))
  val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
  ```



**reduceByKey** 和 **groupByKey** 的区别？

- 从shuffle的角度来说

  reduceByKey会在分组之前进行预聚合（combine），这样shuffle时，和磁盘进行IO操作时，可以减少传输量，增加shuffle效率。
  而groupByKey会直接分组，没有进行预聚合，进行shuffle时，和磁盘进行IO操作时，shuffle效率比较慢。
  
- 从功能的角度来说

  
  reduceByKey包含分组和聚合的操作，groupByKey只有分组的操作，不能聚合。当只需要分组的情况下，使用groupByKey。而当需要分组和聚合的操作时，使用reduceByKey。

​	![image-20210810130350759](./image/SparkCore_RDD_Acc_Bc/image-20210810130350759.png)



​	![image-20210810130501999](./image/SparkCore_RDD_Acc_Bc/image-20210810130501999.png)





- **aggregateByKey**

  - 函数签名

  ```scala
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
        combOp: (U, U) => U): RDD[(K, U)]
  ```

  - 函数说明

  使用给定的组合函数和中性的“零值”聚合每个键的值。 该函数可以返回与此 RDD 中值的类型 V 不同的结果类型 U。

  第二个参数列表：前者用于合并分区内的值，后者用于合并分区之间的值。

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("a", 3), ("a", 4)
  ),2)
  
  // aggregateByKey 算子是函数柯里化，存在两个参数列表
  // aggregateByKey分区内和分区间做不同的操作。
  // 第一个参数列表：初始值
  // 第二个参数列表：
  //    第一个参数：分区内的计算规则
  //    第二个参数：分区间的计算规则
  val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
    (x, y) => math.max(x, y),
    (x, y) => x + y
  )
  ```

  从函数签名中可以看出来，aggregateByKey的最终结果和初始值的类型相同。利用这个规则，我们来计算一下每个Key的平均值。

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("b", 3),
    ("b", 4), ("b", 2), ("a", 3)
  ),2)
  
  // 计算平均值
  val rdd1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
    (t, v) => {
      (t._1 + v, t._2 + 1)
    },
    (t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    }
  )
  
  val rdd2: RDD[(String, Int)] = rdd1.mapValues {
    case (num, cnt) => {
      num / cnt
    }
  }
  ```

  ​	详情步骤如下：

  ​	![image-20210810150834202](./image/SparkCore_RDD_Acc_Bc/image-20210810150834202.png)



- **foldByKey**

  - 函数签名

  ```scala
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
  ```

  - 函数说明

  当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey。但是foldByKey不能改变值的类型。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("b", 3),
    ("b", 4), ("b", 2), ("a", 3)
  ),2)
  
  rdd.foldByKey(0)(_+_).collect().foreach(println)
  ```



- **combineByKey**

  - 函数签名

  ```scala
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C
  	): RDD[(K, C)]
  ```

  - 函数说明

  最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("b", 3),
    ("b", 4), ("b", 2), ("a", 3)
  ),2)
  
  val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
    (_, 1),
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
  
  combineRDD.mapValues{
    case (num,cnt) => num/cnt
  }.collect().foreach(println)
  ```

  详情步骤如下：

  ​	![image-20210810173740969](./image/SparkCore_RDD_Acc_Bc/image-20210810173740969.png)



reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别？


```scala
/**它们的底层函数调用的都是combineByKeyWithClassTag
 * 
 * reduceByKey:
 *    combineByKeyWithClassTag[V](
 *      (v: V) => v,   // 第一个Key的value值不会参与计算
 *      func,        // 分区内计算规则
 *      func,       // 分区内计算规则
 *      partitioner)
 *
 * aggregateByKey:
 *    combineByKeyWithClassTag[U](
 *      (v: V) => cleanedSeqOp(createZero(), v),   // 初始值与第一个Key的value值进行计算
 *      cleanedSeqOp,   // 分区内计算规则
 *      combOp,        // 分区间计算规则
 *      partitioner)
 *
 * foldByKey:
 *    combineByKeyWithClassTag[V](
 *      (v: V) => cleanedFunc(createZero(), v),  // 初始值与第一个Key的value值进行计算
 *      cleanedFunc,        // 分区内数据的处理函数
 *      cleanedFunc,        // 分区间数据的处理函数
 *      partitioner)
 *
 * combineByKey:
 *    combineByKeyWithClassTag(
 *      createCombiner,    // 相同的Key的第一条数据进行处理函数
 *      mergeValue,       // 分区内数据的处理函数
 *      mergeCombiners,   // 分区间数据的处理函数
 *      numPartitioner)
 *
 */
```



- **sortByKey**

  - 函数签名

  ```scala
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)]
  ```

  - 函数说明

  按键对 RDD 进行排序，以便每个分区都包含已排序的元素范围。 在生成的 RDD 上调用 collect 或 save 将返回或输出一个有序的记录列表（在 save 情况下，它们将被写入文件系统中的多个 part-X 文件，按键的顺序）。
  
  在一个(K,V)的 RDD 上调用sortByKey，K 必须实现 Ordered 接口(特质)，返回一个按照 key 进行排序的RDD。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("c", 2), ("b", 3),
  ))
  val sordRDD: RDD[(String, Int)] = rdd.sortByKey()
  ```



- **join**

  - 函数签名

  ```scala
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
  ```

  - 函数说明
  
  返回一个包含所有元素对的 RDD，在 this 和 other 中可以匹配的键。 每对元素将作为 (k, (v1, v2)) 元组返回，其中 (k, v1) 在 this 中，(k, v2) 在 other 中。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("c", 2), ("b", 3),
  ))
  val joinRDD: RDD[(String, (Int, Int))] = rdd.join(rdd)
  ```
  
  若join的RDD的Key大于1，那么每一个key都会进行join，它会进行笛卡尔乘积的匹配。



- **leftOuterJoin**

  - 函数签名

  ```scala
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner): RDD[(K, (V, Option[W]))]
  ```

  - 函数说明
  
  执行 this 和 other 的左外连接。 对于此中的每个元素 (k, v)，生成的 RDD 要么包含其他中 w 的所有对 (k, (v, Some(w)))，要么包含 (k, (v, None)) ，如果 other 中的元素没有键 k，那么连接k的结果就是 (k, (v, None)) 。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("c", 2), ("b", 3),
  ))
  
  val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 4), ("a", 5)
  ))
  
  val leftRDD: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd1)
  leftRDD.collect().foreach(println)
  
  /*
    (a,(1,Some(4)))
    (a,(1,Some(5)))
    (b,(3,None))
    (c,(2,None))
   */
  ```



- **rightOuterJoin**

  - 函数签名

  ```scala
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
        : RDD[(K, (Option[V], W))]
  ```

  - 函数说明
  
  执行 this 和 other 的右外连接。 对于 other 中的每个元素 (k, w)，生成的 RDD 要么包含此中 v 的所有对 (k, (Some(v), w))，要么包含 (k, (None, w)) ，如果 this中的元素没有键 k，那么连接k的结果就是(k, (None, w))。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("c", 2), ("b", 3),
  ))
  
  val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 4), ("a", 5)
  ))
  
  val rightRDD: RDD[(String, (Option[Int], Int))] = rdd.rightOuterJoin(rdd1)
  rightRDD.collect().foreach(println)
  
  /*
    (a,(Some(1),4))
    (a,(Some(1),5))
   */
  ```
  
  



- **cogroup**

  - 函数签名

  ```scala
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
        : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
  ```

  - 函数说明
  
  对于 this 或 other 中的每个键 k，返回一个结果 RDD，其中包含一个元组，其中包含 this 和 other 中该键的值列表。
  
  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("c", 2), ("c", 3),
  ))
  
  val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 4), ("a", 5), ("c",6)
  ))
  
  val cogRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)
  cogRDD.collect().foreach(println)
  
  /*
    (a,(CompactBuffer(1),CompactBuffer(4, 5)))
    (c,(CompactBuffer(2, 3),CompactBuffer(6)))
   */
  ```
  
  

练习：

1. 数据准备

agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

​	![image-20210811093100654](./image/SparkCore_RDD_Acc_Bc/image-20210811093100654.png)

2. 需求描述

统计出每一个省份**每个广告被点击数量**排行的 Top3

分析图解：

​	![image-20210811093422079](./image/SparkCore_RDD_Acc_Bc/image-20210811093422079.png)

```scala
package com.local.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Operator_Transform_text {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("text").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/input/data/agent.log")

    val rdd1: RDD[((String, String), Int)] = rdd.map(line => ((line.split(" ")(1), line.split(" ")(4)), 1))

    val rdd2: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)

    val rdd3: RDD[(String, (String, Int))] = rdd2.map {
      case ((x, y), z) => {
        (x, (y, z))
      }
    }

    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(iter => {
      val tuples: List[(String, Int)] = iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      tuples
    })

    rdd5.collect().foreach(println)

    sc.stop()

  }

}
```

​	![image-20210811095123993](./image/SparkCore_RDD_Acc_Bc/image-20210811095123993.png)





### 2.3.2	RDD行动算子

行动算子，其实就是触发作业（job）执行的方法。
底层代码调用的是环境对象的runjob方法。
底层代码会创建ActiveJob，并提交执行。



- **reduce**

  - 函数签名

  ```scala
  def reduce(f: (T, T) => T): T
  ```
  - 函数说明

  两两聚合，聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据



- **collect**

  - 函数签名

  ```scala
  def collect(): Array[T]
  ```

  - 函数说明

  返回一个包含此 RDD 中所有元素的数组。
  仅当预期结果数组较小时才应使用此方法，因为所有数据都已加载到驱动程序的内存中。



- **count**

  - 函数签名

  ```scala
  def count(): Long
  ```

  - 函数说明

  返回 RDD 中元素的数量。



- **first**

  - 函数签名

  ```scala
  def first(): T
  ```

  - 函数说明

  返回此 RDD 中的第一个元素。



- **take**

  - 函数签名

  ```scala
  def take(num: Int): Array[T]
  ```
  - 函数说明

  取 RDD 的前 num 个元素。 它的工作原理是首先扫描一个分区，然后使用该分区的结果来估计满足限制所需的附加分区的数量。

  由于内部实现的复杂性，如果在 Nothing 或 Null 的 RDD 上调用此方法将引发异常。



- **takeOrdered**

  - 函数签名

  ```scala
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
  ```
  - 函数说明

  返回此 RDD 中由指定的隐式 Ordering[T] 定义的前 k 个（最小）元素并保持排序。 默认是升序。 例如：

      sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
      // 返回数组(2)
      sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
      // 返回数组(2, 3)

  参数：
  num – k，要返回的元素数
  ord – T 的隐式排序
  返回：
  top元素的数组



```scala
package com.local.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    // reduce  两两聚合
    println(rdd.reduce(_ + _))

    // collect  会将不同分区的数据，安装分区顺序，采集到Driver端内存中，形成数组
    val  ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))

    // count 计算rdd中数据的个数
    println(rdd.count())

    // first  取数据源当中的第一个数据
    println(rdd.first())

    // take 获取n个数据
    println(rdd.take(2).mkString(","))

    // takeOrdered 数据排序后，获取n个数据
    println(rdd.takeOrdered(2).mkString(","))

    sc.stop()
  }
}
```





- **aggregate**
  - 函数签名

  ```scala
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
  ```

  - 函数说明

  使用给定的组合函数和中性的“零值”聚合每个分区的元素，然后聚合所有分区的结果。 这个函数可以返回与这个 RDD 的类型 T 不同的结果类型 U。因此，我们需要一个将 T 合并到一个 U 的操作和一个合并两个 U 的操作。 这两个函数都允许修改并返回它们的第一个参数，而不是创建一个新的 U 以避免内存分配。
  参数：
  zeroValue – seqOp 操作符的每个分区的累积结果的初始值，以及来自不同分区的组合结果的初始值对于 combOp 操作符 - 这通常是中性元素（例如 Nil 表示列表连接或 0 表示 总和）
  seqOp – 用于在分区内累积结果的运算符
  combOp – 用于组合来自不同分区的结果的关联运算符

  

  aggregateByKey和aggregate的区别？

  aggregateByKey : 初始值只会参与分区内的计算
  aggregate:初始值不只参与分区内的计算，还会参与分区间的计算

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,4),4)
  
  println(rdd.aggregate(10)(_ + _, _ + _))
  // 64
  ```

  

- **fold**

  - 函数签名

  ```scala
  def fold(zeroValue: T)(op: (T, T) => T): T
  ```

  - 函数说明

  使用给定的关联函数和中性的“零值”聚合每个分区的元素，然后聚合所有分区的结果。函数 op(t1, t2) 允许修改 t1 并将其作为结果值返回以避免对象分配；但是，它不应修改 t2。折叠操作，aggregate 的简化版操作。
  参数：
  zeroValue – op 运算符的每个分区的累积结果的初始值，以及 op 运算符不同分区的组合结果的初始值 - 这通常是中性元素（例如 Nil 表示列表连接或 0 表示总和）
  op – 用于在分区内累积结果并组合来自不同分区的结果的运算符

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,4),4)
  
  println(rdd.fold(10)(_ + _))
  // 64
  ```



- **countByValue**

  - 函数签名

  ```scala
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long]
  ```

  - 函数说明

  返回此 RDD 中每个唯一值的计数作为 (value, count) 对的本地映射。
  要处理非常大的结果，请考虑使用

    rdd.map(x => (x, 1L)).reduceByKey(_ + _)

  它返回一个 RDD[T, Long] 而不是一个映射。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,4),4)
  
  val intToLong: collection.Map[Int, Long] = rdd.countByValue()
  println(intToLong)
  // Map(4 -> 2, 1 -> 1, 2 -> 1, 3 -> 1)
  ```



- **countByKey**
  - 函数签名

  ```scala
  def countByKey(): Map[K, Long]
  ```

  - 函数说明

  计算每个键的元素数量，将结果收集到本地 Map。
  仅当预期结果映射较小时才应使用此方法，因为整个内容已加载到驱动程序的内存中。 要处理非常大的结果，请考虑使用 rdd.mapValues(_ => 1L).reduceByKey(_ + _)，它返回 RDD[T, Long] 而不是映射。

  源码如下：

  ```scala
  mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
  ```

  ```scala
  val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("a", 2), ("x", 2)
  ))
  val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
  println(stringToLong)
  // Map(x -> 1, a -> 2)
  ```



- **foreach**

  - 函数签名

  ```scala
  def foreach(f: T => Unit): Unit
  ```
  - 函数说明

  将函数 f 应用于此 RDD 的所有元素。

  ```scala
  val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
  
  // 收集后打印
  rdd.collect().foreach(println)
  println("******************")
  // 分布式打印（乱序的）
  rdd.foreach(println)
  ```
  
  ​	![image-20210811130638235](image/SparkCore_RDD_Acc_Bc/image-20210811130638235.png)



**保存到文件相关的行动算子**

- **saveAsTextFile**、**saveAsObjectFile**、**saveAsSequenceFile**

  - 函数签名

  ```scala
  def saveAsTextFile(path: String): Unit
  def saveAsObjectFile(path: String): Unit
  def saveAsSequenceFile(
        path: String,
        codec: Option[Class[_ <: CompressionCodec]] = None): Unit
  ```

  - 函数说明

  将数据保存到不同格式的文件中

  ```scala
  val rdd: RDD[(String, Int)] = sc.makeRDD(List(
    ("a", 1), ("b", 2), ("c", 3)
  ))
  
  rdd.saveAsTextFile("data/output/output1")
  rdd.saveAsObjectFile("data/output/output2")
  rdd.saveAsSequenceFile("data/output/output3")
  ```





## 2.4	RDD序列化

1. 闭包检测

   从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor端执行。那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给 Executor端执行，就会发生错误，所以需要在执行任务计算前，**检测闭包内的对象是否可以进行序列化**，这个操作我们称之为**闭包检测**。Scala2.12 版本后闭包编译方式发生了改变

2. 序列化方法和属性

   ```scala
   package com.local.rdd.action
   
   import org.apache.spark.rdd.RDD
   import org.apache.spark.{SparkConf, SparkContext}
   
   object Spark07_RDD_Operator_Action {
     def main(args: Array[String]): Unit = {
       val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("action")
       val sc = new SparkContext(conf)
   
       val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)
   
       val user = new User()
   
       rdd.foreach(
         num => {
           println("age = "+(user.age+num))
         }
       )
   
       sc.stop()
     }
       // 继承Serializable 或者 创建一个case class
   //  class User() extends Serializable {
   //    val age:Int = 30;
   //  }
   
     case class User(){
       val age:Int = 30;
     }
   
   }
   ```

   ​	![image-20210811130557835](image/SparkCore_RDD_Acc_Bc/image-20210811130557835.png)

3. Kryo 序列化框架

   参考地址: https://github.com/EsotericSoftware/kryo



```scala
package com.root.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "asd"))

    val search = new Search("h")

    search.getMatch(rdd).collect().foreach(println)

    sc.stop()
  }

  case class Search(query:String) {

    def isMatch(s:String): Boolean ={
      s.contains(query)
    }

    def getMatch(rdd: RDD[String]): RDD[String]={
      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] ={
      rdd.filter(x=>x.contains(query))
    }
  }

}

```





## 2.5	RDD依赖关系

​	![image-20210811130747705](image/SparkCore_RDD_Acc_Bc/image-20210811130747705.png)

### 2.5.1	RDD血缘关系

​		RDD 只支持粗粒度转换，即在大量记录上执行的单个操作。将创建 RDD 的一系列 Lineage（血统）记录下来，以便恢复丢失的分区。RDD 的 Lineage 会记录 RDD 的元数据信息和转换行为，当该 RDD 的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的数据分区。

​	![image-20210811130825112](image/SparkCore_RDD_Acc_Bc/image-20210811130825112.png)

相邻的两个RDD的关系称之为依赖关系，多个连续的RDD的依赖关系，称之为**血缘关系**。每个RDD会保存血缘关系。

​	![image-20210811131046266](image/SparkCore_RDD_Acc_Bc/image-20210811131046266.png)



​	![image-20210811131108440](image/SparkCore_RDD_Acc_Bc/image-20210811131108440.png)

```scala
val lines: RDD[String] = sc.textFile("data/input/text.txt")
println(lines.toDebugString)
println("*******************")

val words: RDD[String] = lines.flatMap(_.split(" "))
println(words.toDebugString)
println("*******************")

val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
println(wordAndOnes.toDebugString)
println("*******************")

val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)
println(result.toDebugString)
println("*******************")

result.collect().foreach(println)
```

```makefile
(2) data/input/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_dep.scala:12 []
 |  data/input/words.txt HadoopRDD[0] at textFile at Spark01_RDD_dep.scala:12 []
*******************
(2) MapPartitionsRDD[2] at flatMap at Spark01_RDD_dep.scala:16 []
 |  data/input/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_dep.scala:12 []
 |  data/input/words.txt HadoopRDD[0] at textFile at Spark01_RDD_dep.scala:12 []
*******************
(2) MapPartitionsRDD[3] at map at Spark01_RDD_dep.scala:20 []
 |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_dep.scala:16 []
 |  data/input/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_dep.scala:12 []
 |  data/input/words.txt HadoopRDD[0] at textFile at Spark01_RDD_dep.scala:12 []
*******************
(2) ShuffledRDD[4] at reduceByKey at Spark01_RDD_dep.scala:24 []
 +-(2) MapPartitionsRDD[3] at map at Spark01_RDD_dep.scala:20 []
    |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_dep.scala:16 []
    |  data/input/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_dep.scala:12 []
    |  data/input/words.txt HadoopRDD[0] at textFile at Spark01_RDD_dep.scala:12 []
*******************
```



### 2.5.2	RDD依赖关系

这里所谓的依赖关系，其实就是两个相邻 RDD 之间的关系。

```scala
val lines: RDD[String] = sc.textFile("data/input/text.txt")
println(lines.dependencies)
println("*******************")

val words: RDD[String] = lines.flatMap(_.split(" "))
println(words.dependencies)
println("*******************")

val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
println(wordAndOnes.dependencies)
println("*******************")

val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)
println(result.dependencies)
println("*******************")

result.collect().foreach(println)
```

```makefile
List(org.apache.spark.OneToOneDependency@671d1157)
*******************
List(org.apache.spark.OneToOneDependency@372ca2d6)
*******************
List(org.apache.spark.OneToOneDependency@10b1a751)
*******************
List(org.apache.spark.ShuffleDependency@14fded9d)
*******************
```



### 2.5.3	RDD窄依赖

窄依赖表示每一个父(上游)RDD 的 Partition 最多被子（下游）RDD 的一个 Partition 使用，窄依赖我们形象的比喻为独生子女。

```scala
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd)
```

​	![image-20210811132316316](image/SparkCore_RDD_Acc_Bc/image-20210811132316316.png)

​	![image-20210811132559901](image/SparkCore_RDD_Acc_Bc/image-20210811132559901.png)





### 2.5.4	RDD宽依赖

宽依赖表示同一个父（上游）RDD 的 Partition 被多个子（下游）RDD 的 Partition 依赖，会引起 Shuffle，总结：宽依赖我们形象的比喻为多生。

```scala
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
 @transient private val _rdd: RDD[_ <: Product2[K, V]],
 val partitioner: Partitioner,
 val serializer: Serializer = SparkEnv.get.serializer,
 val keyOrdering: Option[Ordering[K]] = None,
 val aggregator: Option[Aggregator[K, V, C]] = None,
 val mapSideCombine: Boolean = false)
 extends Dependency[Product2[K, V]]
```

​	![image-20210811132440320](image/SparkCore_RDD_Acc_Bc/image-20210811132440320.png)

​	![image-20210811132642629](image/SparkCore_RDD_Acc_Bc/image-20210811132642629.png)



### 2.5.5	RDD阶段划分

DAG（Directed Acyclic Graph）有向无环图是由点和线组成的拓扑图形，该图形具有方向，不会闭环。例如，DAG 记录了 RDD 的转换过程和任务的阶段。

​	![image-20210811160558518](image/SparkCore_RDD_Acc_Bc/image-20210811160558518.png)	![image-20210811160611914](image/SparkCore_RDD_Acc_Bc/image-20210811160611914.png)

当算子的转换需要shuffle，就会创建新的阶段。



阶段划分源码：

```scala
try {
  // New stage creation may throw an exception if, for example, jobs are run on a
  // HadoopRDD whose underlying HDFS files have been deleted.
  finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
} catch {
  case e: Exception =>
    logWarning("Creating new stage failed due to exception - job: " + jobId, e)
    listener.jobFailed(e)
    return
}

……

private def createResultStage(
  rdd: RDD[_],
  func: (TaskContext, Iterator[_]) => _,
  partitions: Array[Int],
  jobId: Int,
  callSite: CallSite): ResultStage = {
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
}

……

private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
  getShuffleDependencies(rdd).map { shuffleDep =>
    getOrCreateShuffleMapStage(shuffleDep, firstJobId)
  }.toList
}

……

private[scheduler] def getShuffleDependencies(rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
  val parents = new HashSet[ShuffleDependency[_, _, _]]
  val visited = new HashSet[RDD[_]]
  val waitingForVisit = new Stack[RDD[_]]
  waitingForVisit.push(rdd)
  while (waitingForVisit.nonEmpty) {
    val toVisit = waitingForVisit.pop()
    if (!visited(toVisit)) {
      visited += toVisit
      toVisit.dependencies.foreach {
        case shuffleDep: ShuffleDependency[_, _, _] => parents += shuffleDep
        case dependency => waitingForVisit.push(dependency.rdd)
      }
    }
  }
  parents
}
```





### 2.5.6	RDD任务划分

RDD 任务切分中间分为：Application、Job、Stage 和 Task

- Application：初始化一个 SparkContext 即生成一个 Application； 

- Job：一个 Action 算子就会生成一个 Job； 

- Stage：Stage 等于宽依赖(ShuffleDependency)的个数加 1； 

- Task：一个 Stage 阶段中，最后一个 RDD 的分区个数就是 Task 的个数。

注意：Application  ->  Job  ->  Stage  ->  Task 每一层都是 1 对 n 的关系。

​	![image-20210811161129272](image/SparkCore_RDD_Acc_Bc/image-20210811161129272.png)



任务划分源码：

```scala
val tasks: Seq[Task[_]] = try {
   stage match {
       case stage: ShuffleMapStage =>
       partitionsToCompute.map { id =>
         val locs = taskIdToLocations(id)
         val part = stage.rdd.partitions(id)
         new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, stage.latestInfo.taskMetrics, properties, Option(jobId),
          Option(sc.applicationId), sc.applicationAttemptId)
       }
     case stage: ResultStage =>
       partitionsToCompute.map { id =>
         val p: Int = stage.partitions(id)
         val part = stage.rdd.partitions(p)
         val locs = taskIdToLocations(id)
         new ResultTask(stage.id, stage.latestInfo.attemptId,
          taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics,
          Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
       }
   }

……

val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

……

override def findMissingPartitions(): Seq[Int] = {
mapOutputTrackerMaster
  .findMissingPartitions(shuffleDep.shuffleId)
  .getOrElse(0 until numPartitions) 
}
```





## 2.6	RDD持久化



### 2.6.1	RDD Cache 缓存

RDD 通过 Cache 或者 Persist 方法将前面的计算结果缓存，默认情况下会把数据以缓存在 JVM 的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的 action 算子时，该 RDD 将会被缓存在计算节点的内存中，并供后面重用。`StorageLevel.MEMORY_ONLY`

缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD 的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。通过基于 RDD 的一系列转换，丢失的数据会被重算，由于 RDD 的各个 Partition 是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部 Partition。

​	![image-20210812100928437](image/SparkCore_RDD_Acc_Bc/image-20210812100928437.png)

​	![image-20210812100949883](image/SparkCore_RDD_Acc_Bc/image-20210812100949883.png)

Spark 会自动对一些 Shuffle 操作的中间数据做持久化操作(比如：reduceByKey)。这样做的目的是为了当一个节点 Shuffle 失败了避免重新计算整个输入。但是，在实际使用的时候，如果想重用数据，仍然建议调用 persist 或 cache。

```scala
package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 持久化
    // 缓存
//    rdd2.cache()
    // 保存为临时文件
    rdd2.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
```



### 2.6.2	RDD CheckPoint 检查点

所谓的检查点其实就是通过将 RDD 中间结果写入磁盘

由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。

对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。

```scala
package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/output/result5")

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 设置检查点 checkpoint 需要落盘，需要指定检查点保存路径setCheckpointDir
    // 检查点保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式储存系统中：HDFS
    rdd2.checkpoint()

    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
```



### 2.6.3	缓存和检查点区别

**cache**: 将数据储存在内存中进线数据重用 会在血缘关系中，添加新的依赖，一旦出现问题，可以从头读取数据。
**persist**: 将数据临时储存在磁盘文件中进行数据重用 涉及到磁盘IO，性能比较低，但是数据安全 如果作业执行完毕，临时保存的数据文件就会丢失。
**checkpoint**: 将数据长久地保存在磁盘文件当中进行数据重用 涉及到磁盘IO，性能比较低，但是数据安全 为了保证数据安全，一般情况下，会独立执行作业 为了能够提高效率，**一般需要和cache联合使用**，这样就不会产生独立作业 执行过程中，会切断血缘关系，重新建立血缘关系 会改变数据源。



```scala
package com.local.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("data/output/result5")

    val list = List("hello scala", "hello spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    rdd2.cache()
    rdd2.checkpoint()

    println(rdd2.toDebugString)
    val reduceRDD: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println(rdd2.toDebugString)
      
    sc.stop()
  }
}
```

输出结果：

```makefile
(4) MapPartitionsRDD[2] at map at Spark03_RDD_Persist.scala:36 [Memory Deserialized 1x Replicated]
 |  MapPartitionsRDD[1] at flatMap at Spark03_RDD_Persist.scala:34 [Memory Deserialized 1x Replicated]
 |  ParallelCollectionRDD[0] at makeRDD at Spark03_RDD_Persist.scala:33 [Memory Deserialized 1x Replicated]
(spark,1)
(scala,1)
(hello,2)
(4) MapPartitionsRDD[2] at map at Spark03_RDD_Persist.scala:36 [Memory Deserialized 1x Replicated]
 |       CachedPartitions: 4; MemorySize: 432.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
 |  ReliableCheckpointRDD[4] at collect at Spark03_RDD_Persist.scala:46 [Memory Deserialized 1x Replicated]
```





## 2.7	RDD分区器



Spark 目前支持 Hash 分区和 Range 分区，和用户自定义分区。Hash 分区为当前的默认分区。分区器直接决定了 RDD 中分区的个数、RDD 中每条数据经过 Shuffle 后进入哪个分区，进而决定了 Reduce 的个数。

- 只有 Key-Value 类型的 RDD 才有分区器，非 Key-Value 类型的 RDD 分区的值是 None
- 每个 RDD 的分区 ID 范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。



**自定义分区**

1. 继承Partitioner
2. 重写方法



```scala
package com.local.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "aaaaaaaa"),
      ("cba", "aaaaaaaa"),
      ("wnba", "aaaaaaaa"),
      ("nba", "aaaaaaaa"),
      ("cba", "aaaaaaaa")
    ))
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitione)

    partRDD.saveAsTextFile("data/output/result5")


    sc.stop()
  }

  /**
   * 重写Partitioner
   * 1、继承Partitioner
   * 2、重写方法
   */
  class MyPartitione extends Partitioner{

    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
```





## 2.8	RDD文件的读取和保存

Spark 的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。

- 文件格式分为：text 文件、csv 文件、sequence 文件以及 Object 文件；

- 文件系统分为：本地文件系统、HDFS、HBASE 以及数据库。



**save**

```scala
package com.local.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_rdd_save {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("c", 1),
      ("b", 1)
    ))

    rdd.saveAsTextFile("data/output/result1")
    rdd.saveAsObjectFile("data/output/result2")
    rdd.saveAsSequenceFile("data/output/result3")

    sc.stop()
  }

}
```



**load**

```scala
package com.local.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_rdd_load {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/output/result1")
    val rdd2: RDD[(String,Int)] = sc.objectFile("data/output/result2")
    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("data/output/result3")

    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop()
  }

}
```







# 三、累加器和广播变量

## 3.1	累加器介绍

累加器用来把 Executor 端变量信息聚合到 Driver 端。在 Driver 程序中定义的变量，在Executor 端的每个 Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后，传回 Driver 端进行 merge。 

如果我们想实现一个将（1,2,3,4）累加的功能，可以使用`rdd.reduce(_+_)`来计算，现在我们想简化一点，创建一个sum，用于存放数据。

```scala
package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val i: Int = rdd.reduce(_ + _)
    println(i)

    var sum = 0
    rdd.foreach(
      num=>{
        sum += num
      }
    )
    println("sum = "+sum)

    sc.stop()
  }
}
```

我们可以查看运行之后的sum值：

```makefile
10
sum = 0
```

发现sum为0，这是为什么？

我们来图解一下：

​	![image-20210812101019587](image/SparkCore_RDD_Acc_Bc/image-20210812101019587.png)

这样就需要累加器，去累加。

​	![image-20210812101138617](image/SparkCore_RDD_Acc_Bc/image-20210812101138617.png)



## 3.2	使用累加器



### 3.2.1	系统累加器

使用累加器，首先需要创建一个累加器，`sc.longAccumulator("sum")`，累加器中有一个`add`方法可以添加数据。

```scala
package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 创建累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(
      num =>{
        // 使用累加器
        sumAcc.add(num)
      }
    )

    println(sumAcc.value)

    sc.stop()
  }

}
```

### 3.2.2	自定义累加器

除了使用系统自带的累加器，还可以自定义累加器，用于多种不同的需求。

自定义累加器步骤

1. 继承 AccumulatorV2，并设定泛型（数据结构）
2. 重写累加器的抽象方法

使用自定义累加器，需要先向Spark注册一下，`sc.register`

下面我们自定义一个累加器，用于实现WordCount功能：

```scala
package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object Spark04_Acc_WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello","spark","hello"))

    // 创建累加器
    val wcAcc = new MyAccumulator()
    sc.register(wcAcc,"WordConutAcc")

    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )
    println(wcAcc.value)

    sc.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Long]]{

    private var mapAcc  = mutable.Map[String,Long]()

    // 判断初始状态
    override def isZero: Boolean = {
      mapAcc.isEmpty
    }

    // 复制出来一个
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 清空
    override def reset(): Unit = {
      mapAcc.clear()
    }

    // 获取累加器的值
    override def add(v: String): Unit = {
      val newnum: Long = mapAcc.getOrElse(v, 0L) + 1
      mapAcc.update(v,newnum)
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      other.value.foreach{
        case (word,count)=>{
          val newCount: Long = mapAcc.getOrElse(word,0L)+count
          mapAcc.update(word,newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      mapAcc
    }
  }
}
```





## 3.3	广播变量介绍

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个 Spark 操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark 会为每个任务分别发送。

假设一个场景，我们需要将（("a", 1), ("b", 2), ("c", 3)），（("a", 4), ("b", 5), ("c", 6)），连接成为

(a,(1,4))
(b,(2,5))
(c,(3,6))

我们可以使用join，但是能不能使用map来将它们连接起来呢？来试一下

```scala
package com.local.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    val rdd2: mutable.Map[String, Int] = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))

    val rdd1: RDD[(String, (Int, Int))] = rdd.map {
      case (w, c) => {
        val i: Int = rdd2.getOrElse(w, 0)
        (w, (c, i))
      }
    }

    rdd1.collect().foreach(println)

    sc.stop()
  }
}
```

这样运行起来，可以完成连接。

但是这样有没有什么问题？

假设有四个任务，每个任务都需要map，这样每个任务都会复制出来一个map出来。

​	![image-20210812112745943](image/SparkCore_RDD_Acc_Bc/image-20210812112745943.png)

闭包数据，都是以Task为单位发送的，每个任务中包含闭包数据。这样可能会导致，一个Executor中含有大量重复的数据，并且占用大量的内存

Executor其实就一个JVM，所以在启动时，会自动分配内存。完全可以将任务中的闭包数据放置在Executor的内存中，达到共享的目的。

Spark中的广播变量就可以将闭包的数据保存到Executor的内存中，Spark中的广播变量不能够更改 ： 分布式共享只读变量。

​	![image-20210812112932801](image/SparkCore_RDD_Acc_Bc/image-20210812112932801.png)



## 3.4	广播变量使用

使用`sc.broadcast`可以封装一个广播变量，`bc.value`可以获取到广播变量的值。



```scala
package com.local.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    val map: mutable.Map[String, Int] = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    val rdd1: RDD[(String, (Int, Int))] = rdd.map {
      case (w, c) => {
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
      }
    }
    rdd1.collect().foreach(println)

    sc.stop()
  }
}
```

