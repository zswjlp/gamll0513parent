package com.atguigu.gmall0513.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
@AllArgsConstructor
@Data
public class Stat {
    String title;
    List<Option> options;
}
