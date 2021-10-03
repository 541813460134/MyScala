
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.graph.{Graph => GraphStream}


object App {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().appName("spark demo").master("local[3]").getOrCreate();
    val sc = spark.sparkContext;
    spark.sparkContext.setLogLevel("ERROR")



    //git test
    //git test 2
    //hotfix test
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String)
    System.out.println("master test")
    System.out.println("hot-fix test")
    System.out.println("git test1")
    val vertexArray = Array(
      (1L, ("孙少平")),
      (2L, ("孙少安")),
      (3L, ("孙玉厚")),
      (4L, ("王世才")),
      (5L, ("惠英嫂")),
      (6L, ("田晓霞")),
      (7L, ("孙少安")),
      (8L, ("郝红梅")),
      (9L, ("顾养民")),
      (10L, ("金秀")),
      (11L, ("孙兰香")),
      (12L, ("吴仲平")),
      (13L, ("孙兰花")),
      (14L, ("王满银")),
      (15L, ("侯玉英")),
      (16L, ("奶奶")),
      (17L, ("孙玉婷")),
      (18L, ("贺凤英")),
      (19L, ("何秀莲")),
      (20L, ("李向前")),
      (21L, ("田润叶"))
    )
    //边的数据类型ED:String
    val edgeArray = Array(
      Edge(1L, 5L, "师娘"),
      Edge(1L, 6L, "精神伴侣，互相爱慕"),
      Edge(2L, 21L, "青梅竹马"),
      Edge(2L, 1L, "兄弟"),
      Edge(3L, 17L, "兄弟"),
      Edge(3L, 1L, "二儿子"),
      Edge(3L, 2L, "大儿子"),
      Edge(4L, 5L, "夫妻"),
      Edge(4L, 1L, "煤矿师徒"),
      Edge(7L, 6L, "追求者"),
      Edge(8L, 1L, "患难与共，互生情愫"),
      Edge(8L, 9L, "爱慕虚荣，偷窃分手"),
      Edge(9L, 10L, "大学情侣"),
      Edge(10L, 1L, "欣赏品质，单生爱慕"),
      Edge(11L, 10L, "闺蜜"),
      Edge(12L, 11L, "大学情侣"),
      Edge(14L, 13L, "夫妻"),
      Edge(3L, 13L, "大女儿"),
      Edge(3L, 11L, "二女儿"),
      Edge(15L, 1L, "以身相许"),
      Edge(16L, 3L, "大儿子"),
      Edge(16L, 17L, "二儿子"),
      Edge(17L, 18L, "夫妻"),
      Edge(18L, 19L, "亲戚"),
      Edge(19L, 2L, "一见钟情"),
      Edge(20L, 21L, "夫妻")
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(VertexId, (String))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[String]] = sc.parallelize(edgeArray)



//    val userGraph = Graph(vertexRDD,edgeRDD)
//    userGraph.vertices.collect.foreach(println)


    //构造图Graph[VD,ED]
    val graph: Graph[(String), String] = Graph(vertexRDD, edgeRDD)
    graph.vertices.collect.foreach(println)
    //***********************************************************************************
    //***************************  图的属性    ****************************************
    //**********************************************************************************    println("***********************************************")

    println("**********************************************************")


    println("该图所具有的边数为："+graph.numEdges)

      graph.triplets.map{v=>(v.dstAttr+" 是 "+v.srcAttr+" 的 "+v.attr)}.collect.foreach(println)



    println("**********************************************************")
    println("输出所有关系为夫妻")
    //输出所有关系为夫妻
    graph.edges.filter{case Edge(src,dst,attr) => attr.endsWith("夫妻")}.collect.foreach(println)




    println("**********************************************************")
    println("获取顶点数量")
    //获取顶点数量
    graph.numVertices

    println("**********************************************************")
    println("获取属性图的所有顶点的入度")
    //获取属性图的所有顶点的入度
    graph.inDegrees.collect.foreach(println)


    println("**********************************************************")
    println("输出所有顶点")
    //输出所有顶点
    graph.vertices.collect.foreach(println)

    println("**********************************************************")
    println("输出所有边")
    //输出所有边
    graph.edges.collect.foreach(println)



    val graphdemo: SingleGraph = new SingleGraph("graphDemo")

    // Set up the visual attributes for graph visualization
    graphdemo.setAttribute("ui.stylesheet","url(D:/stylesheet)")
    graphdemo.setAttribute("ui.quality")
    graphdemo.setAttribute("ui.antialias")


    val srcGraph  = Graph(vertexRDD, edgeRDD)
    // Given the egoNetwork, load the graphX vertices into GraphStream
    for ((id,name) <- srcGraph.vertices.collect()) {
      val node = graphdemo.addNode(id.toString).asInstanceOf[SingleNode]
      node.addAttribute("ui.label",name)
    }
    // Load the graphX edges into GraphStream edges
    for (Edge(x,y,relation) <- srcGraph.edges.collect()) {
      val edge = graphdemo.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
      edge.addAttribute("ui.label",relation)
    }

    graphdemo.display()







  }

}
