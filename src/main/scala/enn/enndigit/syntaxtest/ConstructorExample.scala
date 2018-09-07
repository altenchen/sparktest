package enn.enndigit.syntaxtest

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/7
  * @Project:sparktest
  * @Package:enn.enndigit.syntaxtest
  */
class ConstructorExample {

}
//在类名 Test 后定义添加由普通参数构成的主构造器
class Test(nor: Int, pa: Int) {
  private var dpa = pa * 2

  def normal = nor * 2
  def doublepa = dpa
}
