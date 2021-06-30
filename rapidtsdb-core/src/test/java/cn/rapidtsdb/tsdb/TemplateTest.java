package cn.rapidtsdb.tsdb;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;

public class TemplateTest {

    @Test
    public void testLinkedList() {
        LinkedList<Integer> list = Lists.newLinkedList();
        for (int i = 0; i < 10; i++) {
            list.add(i);
            if (i % 3 == 2) {
                list.add(i);
            }
        }
        System.out.println(list);
        Iterator<Integer> iter = list.iterator();
        Iterator<Integer> iter1 = list.iterator();
        while(iter.hasNext()) {
            iter.next();
            iter1.next();
            iter1.remove();
        }
        list.get(12);
        System.out.println(list);
    }
}
