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

import java.io.Serializable;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 */
public class TimestampMatrix implements Serializable {

    private static final long serialVersionUID = 3331148113387926667L;
    ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<>();

    public TimestampMatrix(List<String> participants) {
        // create and empty TimestampMatrix
        for (String participant : participants) {
            timestampMatrix.put(participant, new TimestampVector(participants));
        }
    }

    public TimestampMatrix(ConcurrentHashMap<String, TimestampVector> timestampMatrix) {
        // use copy constructor to copy all elements from timestampMatrix into this.timestampMatrix
        this.timestampMatrix = new ConcurrentHashMap<>(timestampMatrix);
    }

    /**
     * @param node
     * @return the timestamp vector of node in this timestamp matrix
     */
    TimestampVector getTimestampVector(String node) {
        return timestampMatrix.get(node);
    }

    /**
     * Merges two timestamp matrix taking the elementwise maximum
     *
     * @param tsMatrix
     */
    public void updateMax(TimestampMatrix tsMatrix) {
        // For each tsVector in timestampMatrix
        // Update the max comparing to this.timestampMatrix vector
        tsMatrix.timestampMatrix
                .forEach((host, timestampVector) -> timestampVector.updateMax(tsMatrix.getTimestampVector(host)));
    }

    /**
     * substitutes current timestamp vector of node for tsVector
     *
     * @param node
     * @param tsVector
     */
    public synchronized void update(String node, TimestampVector tsVector) {
        timestampMatrix.put(node, tsVector);
    }

    /**
     * @return a timestamp vector containing, for each node,
     * the timestamp known by all participants
     */
    public TimestampVector minTimestampVector() {

        // return generated automatically. Remove it when implementing your solution
        return null;
    }

    /**
     * clone
     */
    public synchronized TimestampMatrix clone() {
        return new TimestampMatrix(this.timestampMatrix);
    }

    /**
     * equals
     */
    @Override
    public boolean equals(Object obj) {

        // return generated automatically. Remove it when implementing your solution
        return false;
    }


    /**
     * toString
     */
    @Override
    public synchronized String toString() {
        StringBuilder all = new StringBuilder();
        if (timestampMatrix == null) {
            return all.toString();
        }
        for (Enumeration<String> en = timestampMatrix.keys(); en.hasMoreElements(); ) {
            String name = en.nextElement();
            if (timestampMatrix.get(name) != null)
                all.append(name).append(":   ").append(timestampMatrix.get(name)).append("\n");
        }
        return all.toString();
    }
}
