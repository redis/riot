package com.redislabs.riot.transfer;

import com.redislabs.riot.transfer.TransferExecution.TransferExecutionBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.ArrayList;
import java.util.List;

public class Transfer<I, O> implements Flow.Listener<I, O> {

    @Getter
    private final List<Flow<I, O>> flows;
    private final List<Listener> listeners = new ArrayList<>();

    @Builder
    private Transfer(Flow<I, O> flow) {
        this.flows = new ArrayList<>();
        this.flows.add(flow);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public Transfer<I, O> add(Flow<I, O> flow) {
        flows.add(flow);
        return this;
    }

    public TransferExecution<I, O> execute() {
        flows.forEach(f -> f.addListener(this));
        TransferExecutionBuilder<I, O> builder = TransferExecution.builder();
        flows.forEach(f -> builder.flow(f.execute()));
        return builder.build();
    }

    @Override
    public void onOpen(Flow<I, O> flow) {
        boolean allOpen = true;
        for (Flow<I, O> f : flows) {
            allOpen &= f.isOpen();
        }
        if (allOpen) {
            listeners.forEach(Listener::onOpen);
        }
    }

    @Override
    public void onClose(Flow<I, O> flow) {
        boolean allClosed = true;
        for (Flow<I, O> f : flows) {
            allClosed &= !f.isOpen();
        }
        if (allClosed) {
            listeners.forEach(Listener::onClose);
        }
    }

    public interface Listener {
        void onOpen();

        void onClose();
    }

}