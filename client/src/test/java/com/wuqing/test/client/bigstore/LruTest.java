package com.wuqing.test.client.bigstore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LruTest {

    private Map<Integer, Integer> map = new HashMap();
    private Queue<Integer> queue = new LinkedList();
    private int capacity = 0;

    public LruTest(int capacity) {
        this.capacity = capacity;
    }

    public int get(int key) {
        Integer v = map.get(key);
        if (v == null) {
            return -1;
        } else {
            queue.remove(key);
            queue.add(key);
            return v;
        }
    }

    public void put(int key, int value) {
        map.put(key, value);
        queue.remove(key);
        queue.add(key);
        if (queue.size() > this.capacity) {
            int k = queue.poll();
            map.remove(k);
        }
        if (map.size() != queue.size()) {
            System.out.println(map.size() + ":" + queue.size());
        }

    }

    private static boolean search(char[][] grid, int indexX, int indexY, int hang, int lie) {
        if (indexX < 0 || indexX >= hang) {
            return false;
        }
        if (indexY < 0 || indexY >= lie) {
            return false;
        }
        if (grid[indexX][indexY] == '0') {
            return false;
        }
        grid[indexX][indexY] = '0';
        search(grid, indexX + 1, indexY, hang, lie);
        search(grid, indexX - 1, indexY, hang, lie);
        search(grid, indexX, indexY + 1, hang, lie);
        search(grid, indexX, indexY - 1, hang, lie);
        return true;
    }


    public static void main(String[] args) {
        /*char[][] grid = new char[][] {
                {'1', '1', '1', '1', '0'},
                {'1', '1', '0', '1', '0'},
                {'1', '1', '0', '0', '0'},
                {'0', '0', '0', '0', '0'}
        };
        int lie = grid.length;
        int hang = grid[0].length;
        search(grid, 0, 0, hang, lie);*/

        /*LruTest t = new LruTest(2);
        t.put(1, 1);
        t.put(2, 2);
        t.get(1);
        t.put(3, 3);
        t.get(2);
        t.put(4, 4);;
        t.get(1);
        t.get(3);
        t.get(4);*/
        //["LRUCache","put","put","get","put","get","put","get","get","get"]
        //[[1,1],[2,2],[1],[3,3],[2],[4,4],[1],[3],[4]]
        Integer left = null;
        Integer right = 2;
        Integer root = null;
        Integer res = left == null ? right : right == null ? left : root;
        System.out.println(res);
    }

}
