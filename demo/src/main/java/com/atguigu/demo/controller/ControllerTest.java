package com.atguigu.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
//@Controller+@ResponseBody
public class ControllerTest {

    @RequestMapping("test")
    public String test() {
        System.out.println("123");
        return "success";
    }

    @RequestMapping("test2")
//    @ResponseBody
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") Integer age) {
        System.out.println("123");
        return "name:" + name + " " + "age:" + age;
    }
}
