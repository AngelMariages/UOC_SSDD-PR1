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

package recipes_service.tsae.sessions;


import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.Recipe;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import java.io.IOException;
import java.net.Socket;

/**
 * @author Joan-Manuel Marques
 * December 2012
 */
public class TSAESessionPartnerSide extends Thread {

    private final Socket socket;
    private final ServerData serverData;

    public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
        super("TSAEPartnerSideThread");
        this.socket = socket;
        this.serverData = serverData;
    }

    public void run() {

        Message msg;

        int current_session_number = -1;
        try {
            ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
            ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

            // receive request from originator and update local state
            // receive originator's summary and ack
            msg = (Message) in.readObject();
            current_session_number = msg.getSessionNumber();
            LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] TSAE session");
            LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
            if (msg.type() == MsgType.AE_REQUEST) {
                // Compare local log and partner's summary to find missing operations
                MessageAErequest AERequest = (MessageAErequest) msg;

                TimestampVector originatorSummary = AERequest.getSummary();
                TimestampMatrix originatorAck = AERequest.getAck();

                // send operations
                for (Operation operation : serverData.getLog().listNewer(originatorSummary)) {
                    MessageOperation messageOperation = new MessageOperation(operation);
                    messageOperation.setSessionNumber(current_session_number);
                    out.writeObject(messageOperation);
                    LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] " +
                            "sent operations: " + messageOperation);
                }

                // send to originator: local's summary and ack
                synchronized (serverData) {
                    TimestampVector localSummary = serverData.getSummary().clone();
                    TimestampMatrix localAck = serverData.getAck().clone();

                    serverData.getAck().update(serverData.getId(), localSummary);

                    msg = new MessageAErequest(localSummary, localAck);
                    msg.setSessionNumber(current_session_number);
                    out.writeObject(msg);
                    LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);
                }

                // receive operations
                msg = (Message) in.readObject();
                LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
                while (msg.type() == MsgType.OPERATION) {
                    Operation op = ((MessageOperation) msg).getOperation();

                    // Add operation to the log
                    serverData.getLog().add(op);

                    // If it's an ADD operation, get recipe and add it to the list.
                    if (op.getType() == OperationType.ADD) {
                        Recipe receivedRecipe = ((AddOperation) op).getRecipe();
                        serverData.getRecipes().add(receivedRecipe);
                    }

                    msg = (Message) in.readObject();
                    LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
                }

                // receive message to inform about the ending of the TSAE session
                if (msg.type() == MsgType.END_TSAE) {
                    synchronized (serverData) {
                        serverData.getSummary().updateMax(originatorSummary);
                        serverData.getAck().updateMax(originatorAck);
                    }

                    // send and "end of TSAE session" message
                    out.writeObject(endTSAEMessage(current_session_number));
                }

            }
            socket.close();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            LSimLogger.log(Level.FATAL, "[TSAESessionPartnerSide] [session: " + current_session_number + "]" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
        }

        LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] End TSAE session");
    }

    private MessageEndTSAE endTSAEMessage(int current_session_number) {
        MessageEndTSAE msg = new MessageEndTSAE();
        msg.setSessionNumber(current_session_number);
        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);
        return msg;
    }
}
