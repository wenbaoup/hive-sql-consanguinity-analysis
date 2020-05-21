package com.wb.bean;

import java.util.List;
import java.util.Set;

public class SQLResult {
    private Set<String> outputTables;
    private Set<String> inputTables;
    private List<ColLine> colLineList;

    public Set<String> getOutputTables() {
        return outputTables;
    }

    public void setOutputTables(Set<String> outputTables) {
        this.outputTables = outputTables;
    }

    public Set<String> getInputTables() {
        return inputTables;
    }

    public void setInputTables(Set<String> inputTables) {
        this.inputTables = inputTables;
    }

    public List<ColLine> getColLineList() {
        return colLineList;
    }

    public void setColLineList(List<ColLine> colLineList) {
        this.colLineList = colLineList;
    }

}
