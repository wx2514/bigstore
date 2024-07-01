package com.wuqing.test.client.bigstore;

import java.io.*;
import java.util.*;
import java.lang.*;


public class TestLink {

    public static void main(String[] args) {
        /*ListNode head = new ListNode(1);
        ListNode h2 = new ListNode(2);
        head.next = h2;
        ListNode h3 = new ListNode(3);
        h2.next = h3;
        ListNode h4 = new ListNode(4);
        h3.next = h4;
        ListNode h5 = new ListNode(5);
        h4.next = h5;
        reverseKGroup(head, 2);*/
        /*int[] nums = new int[] {3,2,3,1,2,4,5,6,5};

        int k = 2;

        sort(nums, 0, nums.length);
        for (int i = 0; i < nums.length; i++) {
            System.out.print(nums[i] + ",");
        }*/
        ListNode headA = new ListNode(2);
        headA.next = new ListNode(6);
        headA = headA.next;
        headA.next = new ListNode(4);

        ListNode headB = new ListNode(1);
        headA.next = new ListNode(5);

        getIntersectionNode(headA, headB);
        int[] a = new int[10];
        /*Arrays.sort(a, (int i1, int i2) -> {
            return 0;
        });*/


    }

    public static ListNode getIntersectionNode(ListNode headA, ListNode headB) {
        ListNode A = headA, B = headB;
        while (A != B) {
            A = A != null ? A.next : headB;
            B = B != null ? B.next : headA;
        }
        return A;

    }

    public static int findKthLargest(int[] nums, int k) {
        sort(nums, 0, nums.length);
        int count = 0;
        for (int i = nums.length - 1; i >= 0; i--) {
            count++;
            if (count == k) {
                return nums[i];
            }
        }
        return -1;
    }

    private static void sort(int[] nums, int l, int r) {
        if (l >= r) {
            return;
        }
        int i = l, j = r;
        int jizhun = nums[l];
        while (true) {
            while (i < r - 1 && nums[++i] < jizhun);
            while (j > 0 && nums[--j] > jizhun);
            if (i >= j) {
                break;
            }
            swap(nums, i, j);
        }
        swap(nums, l, j);
        sort(nums, l, j);
        sort(nums, j + 1, r);

    }

    private static void swap(int[] nums, int i, int j) {
        int k = nums[i];
        nums[i] = nums[j];
        nums[j] = k;
    }

    public static ListNode reverseKGroup(ListNode head, int k) {
        ListNode firstFinal = null;
        ListNode last = null;
        ListNode first = null;
        Stack<ListNode> stack = new Stack();
        int i = 0;
        ListNode ts = head;
        i++;
        stack.push(ts);
        while (ts != null) {
            i++;
            ts = ts.next;
            if (ts == null) {
                break;
            }
            if (i % k == 0) {
                ListNode change = ts;
                first = change;
                if (firstFinal == null) {
                    firstFinal = first;
                }
                ListNode next = change.next;
                while(!stack.empty()) {
                    ListNode pop = stack.pop();
                    change.next = pop;
                    change = pop;
                }
                if (last != null) {
                    last.next = first;
                }
                change.next = next;
                last = change;
                i++;
                ts = next;
                stack.push(ts);
            } else {
                stack.push(ts);
            }
            System.out.println("s2");
        }
        return firstFinal;

    }

    public static class ListNode {
        int val;
        ListNode next;
        ListNode() {}
        ListNode(int val) { this.val = val; }
        ListNode(int val, ListNode next) {
            this.val = val; this.next = next;
        }
        int[] a  = new int[10];
    }

}
