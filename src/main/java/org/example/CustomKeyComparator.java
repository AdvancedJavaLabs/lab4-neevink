package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;


public class CustomKeyComparator extends WritableComparator {

    protected CustomKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Text key1 = (Text) a;
        Text key2 = (Text) b;
        // Ваше кастомное сравнение ключей
        return key1.toString().compareTo(key2.toString());
    }
}



