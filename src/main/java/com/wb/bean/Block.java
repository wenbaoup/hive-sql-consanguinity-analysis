package com.wb.bean;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 解析的SQL块
 */
public class Block {
    private String condition;
    private Set<String> colSet = new LinkedHashSet<String>();

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public Set<String> getColSet() {
        return colSet;
    }

    public void setColSet(Set<String> colSet) {
        this.colSet = colSet;
    }
}