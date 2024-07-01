package com.other;


import java.util.*;

/**
 * Created by wuqing on 18/5/21.
 */
public class TTT {

    public static void main(String[] args) {
        int s = 876000 * 3600 * 1000;
        System.out.println(s);
        Scanner sc = new Scanner(System.in);
        List<String> list = new ArrayList<>();
        Collections.sort(list, (String s1, String s2) -> {
            return s1.compareTo(s2);
        });

    }

}
