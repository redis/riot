package com.redis.riot.core.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractCollectionOperationBuilder<B extends AbstractCollectionOperationBuilder<B>>
        extends AbstractOperationBuilder<B> {

    private String memberSpace;

    private List<String> memberFields;

    @SuppressWarnings("unchecked")
    public B members(List<String> fields) {
        this.memberFields = fields;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B memberSpace(String memberSpace) {
        this.memberSpace = memberSpace;
        return (B) this;
    }

    protected Function<Map<String, Object>, String> member() {
        return idFunction(memberSpace, memberFields);
    }

}
