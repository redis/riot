package com.redis.riot.core.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractCollectionMapOperationBuilder extends AbstractMapOperationBuilder {

    private String memberSpace;

    private List<String> memberFields;

    public void setMemberSpace(String memberSpace) {
        this.memberSpace = memberSpace;
    }

    public void setMemberFields(List<String> memberFields) {
        this.memberFields = memberFields;
    }

    protected Function<Map<String, Object>, String> member() {
        return idFunction(memberSpace, memberFields);
    }

}
