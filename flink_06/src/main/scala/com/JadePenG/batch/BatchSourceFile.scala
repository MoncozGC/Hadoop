package com.JadePenG.batch

/**
  *
  * 需求:
  * 1. Flink Source数据源 处理的数据--fromCollection readTextFile readCsvFile
  * 2. sink下沉地  将数据输出到哪  --collect writeAsText writeAsCsv
  * execute执行
  *
  * @author Peng
  */
object BatchSourceFile {
  def main(args: Array[String]): Unit = {
    //1. 基于本地集合的source
    import org.apache.flink.api.scala._
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val collectionDataSet: DataSet[String] = env.fromCollection(
      List("hadoop hive spark", "fink mr hbase", "spark hive flume")
    )
    println("---------fromCollection本地集合----------")
    collectionDataSet.print()
    //将数据保存到内存中
    collectionDataSet.collect()

    //2. 基于本地文件的source
    val textFileDataSet = env.readTextFile("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\input")
    println("---------readTextFile本地文件----------")
    textFileDataSet.print()
    //将数据保存到本地文件系统上
    textFileDataSet.writeAsText("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\output_local")

    //3. 基于HDFS的source
    val hdfsFileDataset = env.readTextFile("hdfs://node01/myText/wordCount_day03/input/wordcount.txt")
    println("---------readTextFile--hdfs文件----------")
    hdfsFileDataset.print()

    //将数据保存到HDFS文件系统上
    hdfsFileDataset.writeAsText("hdfs://node01/myText/wordCount_day03/output_flink")


    //用于映射csv文件的样例类
    case class Student(id: Int, name: String)
    //4. 基于csv文件的source
    val csvFileDataSet: DataSet[Student] = env.readCsvFile[Student]("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\score.csv");
    println("---------readCsvFile--CSV文件----------")
    csvFileDataSet.print()
    //保存csv文件到本地文件系统
    csvFileDataSet.writeAsCsv("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\output_csv")


    //5. 基于压缩文件的source
    val gzDataSet = env.readTextFile("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\wordcount.txt.gz")
    println("---------readCsvFile--压缩文件文件----------")
    gzDataSet.print()
    //保存gz文件到本地文件系统  输出不出来
    textFileDataSet.writeAsText("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\output_gz")
  }
}
