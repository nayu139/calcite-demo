package com.gerardnico.calcite.schema.hr;


import java.util.List;

public  class HrDepartment {
    public final int deptno;
    public final String name;

    @org.apache.calcite.adapter.java.Array(component = HrEmployee.class)
    public final List<HrEmployee> hrEmployees;
    private final HrLocation location;


    public HrDepartment(int deptno, String name, List<HrEmployee> hrEmployees,
                        HrLocation location) {
        this.deptno = deptno;
        this.name = name;
        this.hrEmployees = hrEmployees;
        this.location = location;
    }

    @Override
    public String toString() {
        return "Department [deptno: " + deptno + ", name: " + name
                + ", employees: " + hrEmployees + ", location: " + location + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || obj instanceof HrDepartment
                && deptno == ((HrDepartment) obj).deptno;
    }
}