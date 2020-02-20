package de.tub.ise;

import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.FileWriter;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
/**
 * Exemplary client for you to try your implementation while developing
 *
 * Seperate class with own main method, it doesn't get referenced anywhere else in
 * the code... You can extend and run the main method to try out different
 * requests
 */
public class Client {

  private final ManagedChannel channel;
  private final KeyValueStoreBlockingStub blockingStub;
  private static ArrayList<Boolean> responses = new ArrayList<Boolean>();
  /** Construct client connecting to server at {@code host:port}. */
  public Client(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
  }

  /**
   * Construct client for accessing server using the existing channel. Create
   * blockingStub for synchronous communication
   */
  Client(ManagedChannel channel) {
    this.channel = channel;
    //System.out.println(this.channel);
    blockingStub = KeyValueStoreGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Example put request with synchronous gRPC interface {@code blockingStub}. */
  public void put(String key, String value) {
    System.out.println("\nWriting data...");
    KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
    Response response;
    try {
      response = blockingStub.put(request);
    } catch (StatusRuntimeException e) {
      System.out.println("error");
      return;
    }
    System.out.println("Success? " + response.getSuccess());
    responses.add(response.getSuccess());
  }

  /** Example get request. */
  public void get(String key) {
    System.out.println("\nGetting data... ");
    Key request = Key.newBuilder().setKey(key).build();
    Response response;
    try {
      response = blockingStub.get(request);
    } catch (StatusRuntimeException e) {
      return;
    }
    System.out.println("Success? " + response.getSuccess());
    responses.add(response.getSuccess());
    if (response.getSuccess()) {
      System.out.println("Value: " + response.getValue());
    }
    System.out.println("\n");
  }

  public void delete(String key) {
    System.out.println("\nDeleting data... ");
    Key request = Key.newBuilder().setKey(key).build();
    Response response;
    try {
      response = blockingStub.delete(request);
    } catch (StatusRuntimeException e) {
      return;
    }
    System.out.println("Success? " + response.getSuccess());
    responses.add(response.getSuccess());
    System.out.println("\n");
  }


  public static void main(String[] args) throws Exception {

    // Change client host and port accordingly
    Client client1 = new Client("localhost", 8084);
    Client client2 = new Client("localhost", 8082);
    Client client3 = new Client("localhost", 8083);
    Faker faker = new Faker();
    List<String> names= new ArrayList<>();
    List<String> ids= new ArrayList<>();
    Random rand = new Random();
    for (int i=0;i<200;i++){
      String name = faker.name().fullName();
      int id = rand.nextInt(900) + 100;
      ids.add(Integer.toString(id));
      names.add(name);
    }
    CSVWriter writer = null;

    {
      try {
        writer = new CSVWriter(new FileWriter("./1results331.csv"));
        String[] header = { "type","size" ,"time","status"};
        writer.writeNext(header);
        for (int i=0;i<200;i++) {
          long startTime = System.currentTimeMillis();
          client1.put(ids.get(i), names.get(i));
          long endTime   = System.currentTimeMillis();
          String[] data = { "qwrite","3",Float.toString((endTime-startTime)/ 1000F), String.valueOf(responses.get(i))};
          writer.writeNext(data);
        }
        Thread.sleep(20000);
        //get
        for (int i=0;i<100;i++){
          int getid = rand.nextInt(200);
          long startTime = System.currentTimeMillis();
          client1.get(ids.get(getid));
          long endTime   = System.currentTimeMillis();
          String[] data = { "qread","1",Float.toString((endTime-startTime)/ 1000F), String.valueOf(responses.get(i+200))};
          writer.writeNext(data);
        }
        Thread.sleep(20000);
        /*delete
        for (int i=0;i<100;i++){
          int getid = rand.nextInt(200);
          long startTime = System.currentTimeMillis();
          client1.delete(ids.get(getid));
          long endTime   = System.currentTimeMillis();
          String[] data = { "qdel","3",Float.toString((endTime-startTime)/ 1000F), String.valueOf(responses.get(i+300))};
          writer.writeNext(data);
        }*/
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          client1.shutdown();
          client2.shutdown();
          client3.shutdown();
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    /*try {

      //put

      //get



      client1.put("123", "Maria Borges, TU Berlin");
      long endTime   = System.currentTimeMillis();
      Thread.sleep(20000);
      client3.get("123");
      Thread.sleep(20000);
      client2.delete("123");
      //client2.put("456", "abc, TU Berlin");
      //client1.get("123");
      Thread.sleep(20000);
      client2.get("123");
      Thread.sleep(20000);
      client1.delete("123");
      Thread.sleep(20000);
      client1.get("123");
      Thread.sleep(20000);
      client2.get("123");
      Thread.sleep(20000);
      client3.get("123");
      client2.get("456");
    } finally {
      client1.shutdown();
      client2.shutdown();
      client3.shutdown();
    }*/

  }
}