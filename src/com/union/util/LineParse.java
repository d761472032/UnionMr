package com.union.util;

import java.util.Arrays;
import java.util.List;

public class LineParse {

    private List<String> result;

    public LineParse() {
    }

    public LineParse(String org) {
        result = Arrays.asList(org.split(Contants.split));
    }

    public void parse(String org) {
        result = Arrays.asList(org.split(Contants.split));
    }

    public String getValueByIndex(int i) {
        try {
            return result.get(i);
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
    }

    public int getCount() {
        return result.size();
    }

    public void clear() {
        result.clear();
    }

}
