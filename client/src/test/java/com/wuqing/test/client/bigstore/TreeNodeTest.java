package com.wuqing.test.client.bigstore;

import java.util.Arrays;

public class TreeNodeTest {

    public static void main(String[] args) {
        int[] preorder = new int[] {1, 2, 3};
        int[] inorder = new int[] {2, 3, 1};
        TreeNode root = buildTree(preorder, inorder);
        System.out.println(root);
    }

    public static TreeNode buildTree(int[] preorder, int[] inorder) {
        if (preorder.length == 0 || inorder.length == 0) {
            return null;
        }
        if (preorder.length == 1) {
            return new TreeNode(preorder[0]);
        }
        int rootVal = preorder[0];
        TreeNode root = new TreeNode(rootVal);
        int i = 0;
        for (int k = inorder.length; i < k; i++) {
            if (inorder[i] == rootVal) {
                break;
            }
        }
        int[] preorderLeft = Arrays.copyOfRange(preorder, 1, i + 1);
        int[] inorderLeft = Arrays.copyOfRange(inorder, 0, i);

        root.left = buildTree(preorderLeft, inorderLeft);

        int[] preorderRight = new int[] {};
        int[] inorderRight = new int[] {};
        if (i + 1 < preorder.length) {
            preorderRight = Arrays.copyOfRange(preorder, i + 1, preorder.length);
        }
        if (i + 1 < inorder.length) {
            inorderRight = Arrays.copyOfRange(inorder, i + 1, inorder.length);
        }
        root.right = buildTree(preorderRight, inorderRight);

        return root;
    }


    public static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;
        TreeNode() {}
        TreeNode(int val) { this.val = val; }
        TreeNode(int val, TreeNode left, TreeNode right) {
            this.val = val;
            this.left = left;
            this.right = right;
        }
    }
}
