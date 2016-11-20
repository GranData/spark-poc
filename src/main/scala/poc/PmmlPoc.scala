package poc

import java.io.{FileInputStream, InputStream}

import org.jpmml.evaluator.Evaluator
import org.jpmml.evaluator.tree.TreeModelEvaluator

/**
  * Created by marcos on 11/19/16.
  */
object PmmlPoc extends App {


  val modelFile = new FileInputStream("./src/main/resources/single_iris_dectree.xml")
  val pmml = org.jpmml.model.PMMLUtil.unmarshal(modelFile);


  val modelEvaluator: Evaluator = new TreeModelEvaluator(pmml);

  println(modelEvaluator.getInputFields)
  println(modelEvaluator.getTargetFields)
  println(modelEvaluator.getOutputFields)


  val inputFields = modelEvaluator.getInputFields

  val sepal_length = inputFields.get(0)
  val sepal_width = inputFields.get(1)
  val petal_length = inputFields.get(2)
  val petal_width = inputFields.get(3)

  val v1 = sepal_length.prepare(0.5D)
  val v2 = sepal_width.prepare(0.5D)
  val v3 = petal_length.prepare(0.5D)
  val v4 = petal_width.prepare(0.5D)

  val evalmap = Map(sepal_length.getName -> v1,
    sepal_width.getName -> v2,
    petal_length.getName -> v3,
    petal_width.getName -> v4)

  import scala.collection.JavaConverters._

  val eval = modelEvaluator.evaluate(evalmap.asJava)

  println(eval)
}
