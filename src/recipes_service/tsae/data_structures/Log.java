/*
 * Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This file is part of the practical assignment of Distributed Systems course.
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This code is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this code.  If not, see <http://www.gnu.org/licenses/>.
 */

package recipes_service.tsae.data_structures;

import lsim.library.api.LSimCoordinator;
import lsim.library.api.LSimFactory;
import recipes_service.data.Operation;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 */
public class Log implements Serializable {
    // Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
    // Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

    private static final long serialVersionUID = -4864990265268259700L;
    /**
     * This class implements a log, that stores the operations
     * received  by a client.
     * They are stored in a ConcurrentHashMap (a hash table),
     * that stores a list of operations for each member of
     * the group.
     */
    private final ConcurrentHashMap<String, List<Operation>> log = new ConcurrentHashMap<>();

    public Log(List<String> participants) {
        // create an empty log
        for (Iterator<String> it = participants.iterator(); it.hasNext(); ) {
            log.put(it.next(), new Vector<Operation>());
        }
    }

    private String getOperationHostId(Operation op) {
        if (op == null) {
            return null;
        }

        return op.getTimestamp().getHostid();
    }

    private boolean isOperationNewer(Operation op) {
        if (op == null) {
            return false;
        }

        List<Operation> allOperations = log.get(getOperationHostId(op));

        Operation lastOperation = allOperations.get(allOperations.size() - 1);

        return lastOperation.getTimestamp().compare(op.getTimestamp()) < 0;
    }

    /**
     * inserts an operation into the log. Operations are
     * inserted in order. If the last operation for
     * the user is not the previous operation than the one
     * being inserted, the insertion will fail.
     *
     * @param op
     * @return true if op is inserted, false otherwise.
     */
    public boolean add(Operation op) {
        // ....
        List<Operation> allOperations = log.get(getOperationHostId(op));

        if (allOperations.isEmpty() || isOperationNewer(op)) {
            return allOperations.add(op);
        }

        // return generated automatically. Remove it when implementing your solution
        return false;
    }

    /**
     * Checks the received summary (sum) and determines the operations
     * contained in the log that have not been seen by
     * the proprietary of the summary.
     * Returns them in an ordered list.
     *
     * @param sum
     * @return list of operations
     */
    public List<Operation> listNewer(TimestampVector sum) {

        // return generated automatically. Remove it when implementing your solution
        return null;
    }

    /**
     * Removes from the log the operations that have
     * been acknowledged by all the members
     * of the group, according to the provided
     * ackSummary.
     *
     * @param ack: ackSummary.
     */
    public void purgeLog(TimestampMatrix ack) {
    }

    /**
     * equals
     */
    @Override
    public synchronized boolean equals(Object obj) {
        if (!(obj instanceof Log)) {
            return false;
        }

        Log thatLog = (Log) obj;

        for (Map.Entry<String, List<Operation>> stringListEntry : this.log.entrySet()) {
            List<Operation> thisOperationList = stringListEntry.getValue();
            List<Operation> thatOperationList = thatLog.log.get(stringListEntry.getKey());

            if (thisOperationList.size() != thatOperationList.size()) {
                return false;
            }

            return thisOperationList.equals(thatOperationList);
        }

        // return generated automatically. Remove it when implementing your solution
        return false;
    }

    /**
     * toString
     */
    @Override
    public synchronized String toString() {
        StringBuilder name = new StringBuilder();
        for (Enumeration<List<Operation>> en = log.elements();
             en.hasMoreElements(); ) {
            List<Operation> sublog = en.nextElement();
            for (Operation operation : sublog) {
                name.append(operation.toString()).append("\n");
            }
        }

        return name.toString();
    }
}
