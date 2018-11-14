package org.apache.zjffdu.flink;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class Person {
  private String name;
  private int age;
  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public int getAge() {
    return age;
  }

  @Override
  public String toString() {
    return this.name + " " + this.age;
  }
}


interface MyInterface {
  void f();

  default void g(){
    System.out.println("g");
  }
}

public class JavaExample {

  public static void main(String[] args) {
    Person p1 = new Person("jeff", 23);
    Person p2 = new Person("andy", 34);
    Person p3 = new Person("hello", 22);

    List<Person> personList = Arrays.asList(p1,p2, p3);

    Map<String, Long> map = Arrays.asList("name", "jeff", "andy").parallelStream()
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    List<Integer> list = new ArrayList<>();
    for (int i=0;i<100;++i) {
      list.add(i);
    }

    list.parallelStream().forEach(System.out::println);
  }
}
