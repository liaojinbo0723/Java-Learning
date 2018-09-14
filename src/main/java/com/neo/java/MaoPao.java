package com.neo.java;

public class MaoPao {

    public static void main(String[] args) {



        System.out.println((int)(Math.floor(21943/10000)) + 1);
        int[] array = {3, 6, 2, 8, 9, 1};
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array.length - 1; j++) {
                if (array[j] > array[j + 1]) {
                    int temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                }
            }
            for (int a : array) {
                //System.out.print(a + "\t");
            }
            //System.out.println();
        }
    }
}
