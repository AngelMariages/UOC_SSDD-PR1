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


import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joan-Manuel Marques
 * December 2012
 */
public class TimestampVector implements Serializable {
    // Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
    // Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

    private static final long serialVersionUID = -765026247959198886L;
    /**
     * This class stores a summary of the timestamps seen by a node.
     * For each node, stores the timestamp of the last received operation.
     */

    private ConcurrentHashMap<String, Timestamp> timestampVector = new ConcurrentHashMap<>();

    public TimestampVector(List<String> participants) {
        // create and empty TimestampVector
        for (String id : participants) {
            // when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
            timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
        }
    }

    public TimestampVector(ConcurrentHashMap<String, Timestamp> timestampVector) {
        // Use copy constructor to copy all elements from timestampVector into this.timestampVector
        this.timestampVector = new ConcurrentHashMap<>(timestampVector);
    }

    /**
     * Updates the timestamp vector with a new timestamp.
     *
     * @param timestamp the new timestamp
     */
    public void updateTimestamp(Timestamp timestamp) {
        LSimLogger.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: " + timestamp);

        this.timestampVector.put(timestamp.getHostid(), timestamp);
    }

    /**
     * merge in another vector, taking the elementwise maximum
     *
     * @param tsVector (a timestamp vector)
     */
    public void updateMax(TimestampVector tsVector) {
        for (String participant : tsVector.getParticipants()) {
            Timestamp last = this.getLast(participant);
            Timestamp tsVectorLast = tsVector.getLast(participant);

            // If this last does not exist or, this last is older than tsVectorLast
            // then update the last timestamp for the current participant
            if (last == null || last.compare(tsVectorLast) < 0) {
                timestampVector.put(participant, tsVectorLast);
            }
        }
    }

    /**
     * @param node node to get last from
     * @return the last timestamp issued by node that has been
     * received.
     */
    public Timestamp getLast(String node) {
        return timestampVector.get(node);
    }

    /**
     * merges local timestamp vector with tsVector timestamp vector taking
     * the smallest timestamp for each node.
     * After merging, local node will have the smallest timestamp for each node.
     *
     * @param tsVector (timestamp vector)
     */
    public void mergeMin(TimestampVector tsVector) {
        for (String participant : tsVector.getParticipants()) {
            Timestamp last = this.getLast(participant);
            Timestamp tsVectorLast = tsVector.getLast(participant);

            // If this last does not exist or, this last is newer than tsVectorLast
            // then update the last timestamp for the current participant
            if (last == null || last.compare(tsVectorLast) > 0) {
                timestampVector.put(participant, tsVectorLast);
            }
        }
    }

    public List<String> getParticipants() {
        List<String> participants = new ArrayList<>();
        Enumeration<String> keys = timestampVector.keys();

        while (keys.hasMoreElements()) {
            participants.add(keys.nextElement());
        }

        return participants;
    }

    /**
     * clone
     */
    public TimestampVector clone() {
        return new TimestampVector(this.timestampVector);
    }

    /**
     * equals
     */
    public synchronized boolean equals(TimestampVector ts) {
        if (ts == null) {
            return false;
        }

        for (Map.Entry<String, Timestamp> stringTimestampEntry : this.timestampVector.entrySet()) {
            Timestamp thatTimestamp = ts.timestampVector.get(stringTimestampEntry.getKey());
            Timestamp thisTimestamp = stringTimestampEntry.getValue();

            if (!thisTimestamp.equals(thatTimestamp)) {
                return false;
            }
        }

        return true;
    }

    /**
     * toString
     */
    @Override
    public synchronized String toString() {
        StringBuilder all = new StringBuilder();
        if (timestampVector == null) {
            return all.toString();
        }

        Enumeration<String> en = timestampVector.keys();

        while (en.hasMoreElements()) {
            String name = en.nextElement();
            if (timestampVector.get(name) != null)
                all.append(timestampVector.get(name)).append("\n");
        }
        return all.toString();
    }
}
