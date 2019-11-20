package com.example.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Objects;

@RestController
public class TestController {

    @RequestMapping("/index")
    public String index(){
        return "hello world!!!";
    }

    public static void main(String[] args) {

        HashMap<String, User> stringUserHashMap = new HashMap<>();
        User user = new User();
        user.setAge(10);
        user.setName("aaaa");
        User aaa = stringUserHashMap.put("aaa", user);
        System.out.println(aaa);

        User user1 = new User();
        user1.setName("bbb");
        user1.setAge(20);
        User bbb = (User)stringUserHashMap.put("bbb", user1);
        System.out.println(bbb);
        User bbb1 = stringUserHashMap.put("bbb", null);
        System.out.println(bbb1.toString());



    }

}





class User{


    private String name;
    private Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return Objects.equals(name, user.name) &&
                Objects.equals(age, user.age);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, age);
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}