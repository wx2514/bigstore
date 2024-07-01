package com.wuqing.test.client.bigstore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Scanner;

public class ReaderTest {

    public static void main(String[] args) {
        test();
    }

    private static void test() {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String s = reader.readLine();
            System.out.println(s);
        } catch(Exception e) {

        }

        Scanner in = new Scanner(System.in);
        in.hasNext();

    }
}
