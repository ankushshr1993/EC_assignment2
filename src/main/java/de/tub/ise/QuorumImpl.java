package de.tub.ise;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreStub;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import io.grpc.*;


public class QuorumImpl extends KeyValueStoreGrpc.KeyValueStoreImplBase {
    private final int qwritesize;
    private final int qreadsize;
    private final HashMap<String, String> otherNodes;
    static Logger logger = Logger.getLogger(QuorumImpl.class.getName());
    private KeyValueStoreGrpc.KeyValueStoreStub stub;
    private KeyValueStoreGrpc.KeyValueStoreBlockingStub stub1;
    private ArrayList<KeyValueStoreStub> stubs = new ArrayList<KeyValueStoreStub>();
    private ArrayList<Response> responses = new ArrayList<Response>();
    int flag;


    /**
     * Constructor of Quorum Service
     */
    QuorumImpl() {
        this.qwritesize = KVNodeMain.config.getWriteQuorum();
        this.qreadsize = KVNodeMain.config.getReadQuorum();
        this.otherNodes = KVNodeMain.config.getOtherNodes(KVNodeMain.config.thisNode());
        for (HashMap.Entry<String, String> entry : otherNodes.entrySet()) {
            String node = entry.getKey();
            String host = entry.getValue().split(":")[0];
            int port = Integer.parseInt(entry.getValue().split(":")[1]);
            stub = KeyValueStoreGrpc.newStub(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
            stubs.add(stub);
        }

    }

    /**
     * Implementation of put method specified in the .proto file. Handles write
     * requests from the client, produces response with success boolean and key
     * (optional)
     */
    @Override
    public void put(de.tub.ise.KeyValuePair request,
                    io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();
        Response response;

        logger.debug("Received put request with key " + key);

        if (replicateData(key, value)) {
            response = Response.newBuilder().setSuccess(true).setKey(key).build();
            logger.debug("Telling the client that we replicated");
        } else {
            response = Response.newBuilder().setSuccess(false).setKey(key).build();
            logger.warn("Uh oh, replication not possible :(");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of get method specified in the .proto file. Handles read
     * requests from the client, produces response with success boolean, key and
     * value
     */
    @Override
    public void get(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        String key = request.getKey();
        Response response;

        logger.debug("Received get request with key" + key);

        KeyValuePair data = gatherdata(key);
        if (data == null) {
            response = Response.newBuilder().setSuccess(false).setKey(key).build();
            logger.warn("Uh oh, couldn't get data :(");
        } else {
            response = Response.newBuilder().setSuccess(true).setKey(data.getKey()).setValue(data.getValue()).build();
            logger.debug("Giving client the requested data");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of delete method specified in the .proto file. Handles read
     * requests from the client, produces response with success boolean and original
     * key (optional)
     */
    @Override
    public void delete(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO delete request akin to put request
        String key = request.getKey();
        Response response;

        logger.debug("Received delete request with key" + key);
        KeyValuePair data = KeyValuePair.newBuilder().setKey(key).setValue(Memory.get(key)).build();
        if (deleteData(key)) {
            response = Response.newBuilder().setSuccess(true).clearKey().clearValue().build();
            System.out.println(response);
            logger.debug("Deleting requested data");
        } else {
            response = Response.newBuilder().setSuccess(false).build();
            logger.warn("Uh oh, couldn't get data :(");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of replicate method specified in the .proto file. You can use
     * this for communication between nodes
     */
    @Override
    public void replicate(de.tub.ise.KeyValuePair request,
                          io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle replication requests from other nodes
        String key = request.getKey();
        String value = request.getValue();
        Response response;

        logger.debug("Received replicate request with key " + key);
        Memory.put(key,value);
        response = Response.newBuilder().setSuccess(true).setKey(key).build();

        responseObserver.onNext(response);
    }

    /**
     * Implementation of getReplica method specified in the .proto file. You can use
     * this for communication between nodes
     */
    @Override
    public void getReplica(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle request for local replica from ther nodes
        String key = request.getKey();
        Response response;

        logger.debug("Received get replicate request with key " + key);
        Memory.get(key);
        response = Response.newBuilder().setSuccess(true).setKey(key).build();
        responseObserver.onNext(response);
    }

    /**
     * Implementation of deleteReplica method specified in the .proto file. You can
     * use this for communication between nodes
     */
    @Override
    public void deleteReplica(de.tub.ise.Key request,
                              io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle delete requests from other nodes, akin to write requests
        String key = request.getKey();
        Response response;

        logger.debug("Received delete replicate request with key " + key);
        Memory.delete(key);
        response = Response.newBuilder().setSuccess(true).setKey(key).build();

        responseObserver.onNext(response);

    }

    /**
     * Method to check if quorum replication has been achieved.
     *
     * You are free to change this method as you see fit
     */
    private boolean replicateData(String key, String value) {
        Memory.put(key, value);
        KeyValuePair req = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
        List<Response> repResponses = new ArrayList<>();
        List<Response> receivedResponses = new ArrayList<>();
        StreamObserver<Response> repObs = new StreamObserver<Response>() {
            @Override
            public void onNext(Response response) {
                repResponses.add(response);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                System.out.println("printing rep");
            }
        };
        if (qwritesize > 1) {
            for(int i=0;i<stubs.size();i++) {
                stubs.get(i).replicate(req,repObs);
            }

            try {
                int temp =0;
                int cnt=1;
                synchronized (repResponses){
                    long startTime = System.currentTimeMillis();

                    while (repResponses.size()<=(qwritesize) & cnt<=(qwritesize)){
                      // System.out.println("waiting");
                        if (temp<repResponses.size()){
                            cnt++;
                        }
                        temp = repResponses.size();
                        //Thread.sleep(21000);
                        long endTime   = System.currentTimeMillis();
                        float sec = (endTime - startTime) / 1000F;
                        if (cnt==(qwritesize) & sec<20){
                            System.out.println(sec + " seconds");
                            System.out.println("final size:"+repResponses.size());
                            return true;
                            //break;

                        }
                        if (sec>20){
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        } else
            // TODO send async. replication requests to nodes
            stubs.get(0).replicate(req,repObs);
            stubs.get(1).replicate(req,repObs);
            Memory.put(key, value);
            return true;

    }

    /**
     * Method to fetch value from memory.
     *
     * If quorum bigger than 1 is required, fetch replica values from other nodes.
     * You should also check if the returned values from the replicas are
     * consistent.
     *
     * You are free to change this method if you want
     */
    private KeyValuePair gatherdata(String key) {
        Key req = Key.newBuilder().setKey(key).build();
        Response response;
        List<Response> gatResponses = new ArrayList<>();
        StreamObserver<Response> gatRes = new StreamObserver<Response>() {
            @Override
            public void onNext(Response response) {
                gatResponses.add(response);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                System.out.println("printing gat");
            }
        };
        stubs.get(0).getReplica(req,gatRes);
        stubs.get(1).getReplica(req,gatRes);

        if (qreadsize > 1) {

            try {
                synchronized (gatResponses){
                    long startTime = System.currentTimeMillis();
                    int temp =0;
                    int cnt =1;
                    while (gatResponses.size()<=(qreadsize) & cnt<=(qreadsize)){
                       // System.out.println("waiting");
                        if (temp<gatResponses.size()){
                            cnt++;
                        }
                        temp = gatResponses.size();
                        long endTime   = System.currentTimeMillis();
                        float sec = (endTime - startTime) / 1000F;
                        if (cnt==(qreadsize) & sec<20){
                            System.out.println(sec + " seconds");
                            System.out.println(gatResponses.size() + " responses");
                            String data = Memory.get(key);
                            return KeyValuePair.newBuilder().setKey(key).setValue(data).build();
                            //break;
                        }
                        if (sec>20){
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        } else{
            // TODO send async. replication requests to nodes
            String data = Memory.get(key);
            if (data == null) {
                logger.warn("Couldn't find data for key " + key);
                return null;
            } else
                return KeyValuePair.newBuilder().setKey(key).setValue(data).build();
        }

    }

    private boolean deleteData(String key) {
        Key req = Key.newBuilder().setKey(key).build();
        List<Response> delResponses = new ArrayList<>();
        StreamObserver<Response> delRes = new StreamObserver<Response>() {
            @Override
            public void onNext(Response response) {
                delResponses.add(response);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                System.out.println("printing del");
            }
        };
        if (qwritesize > 1) {
            Memory.delete(key);
            for(int i=0;i<stubs.size();i++) {
                stubs.get(i).deleteReplica(req,delRes);
            }

            try {
                int temp =0;
                int cnt=1;
                synchronized (delResponses){
                    long startTime = System.currentTimeMillis();

                    while (delResponses.size()<=(qreadsize) & cnt<=(qreadsize)){
                        //System.out.println("waiting");
                        if (temp<delResponses.size()){
                            cnt++;
                        }
                        temp = delResponses.size();
                        //Thread.sleep(21000);
                        long endTime   = System.currentTimeMillis();
                        float sec = (endTime - startTime) / 1000F;
                        if (cnt==(qreadsize) & sec<20){
                            System.out.println(sec + " seconds");
                            System.out.println("final size:"+delResponses.size());
                            return true;
                            //break;
                        }
                        if (sec>20){
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return false;
        } else
            // TODO send async. replication requests to nodes
            stubs.get(0).deleteReplica(req,delRes);
            stubs.get(1).deleteReplica(req,delRes);
            Memory.delete(key);
           return true;
    }


}