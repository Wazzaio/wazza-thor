package wazza.thor.jobs

trait ChildJob {

  var dependencies: List[String] = Nil
  var completedDependencies: List[String] = Nil

  def addDependency(d: String) = dependencies = dependencies :+ d
  def dependenciesCompleted = dependencies == completedDependencies
  def execute()

  /**
    TODO
    - update dependencies and completedDepedencies
    - check if all dependencies are done
  **/
}
