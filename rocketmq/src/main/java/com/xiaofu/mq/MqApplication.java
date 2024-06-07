package com.xiaofu.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author fuzhouling
 * @date 2024/06/07
 * @program middle_ware_group
 * @description RocketMQ启动类
 **/
@SpringBootApplication
public class MqApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqApplication.class, args);
    }
}
