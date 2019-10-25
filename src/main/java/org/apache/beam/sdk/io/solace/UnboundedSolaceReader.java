package org.apache.beam.sdk.io.solace;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import com.google.common.annotations.VisibleForTesting;
import com.solacesystems.jcsmp.*;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;



/**
 * Unbounded Reader to read messages from a Solace Router.
 */
@VisibleForTesting
class UnboundedSolaceReader<T> extends UnboundedSource.UnboundedReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceReader.class);

  // The closed state of this {@link UnboundedSolaceReader}. If true, the reader
  // has not yet been closed,
  AtomicBoolean active = new AtomicBoolean(true);

  private final UnboundedSolaceSource<T> source;
  private JCSMPSession session;
  protected FlowReceiver flowReceiver;
  private boolean isAutoAck;
  private boolean useSenderTimestamp;
  private boolean useSenderMessageId;
  private String clientName;
  private Topic sempShowTopic;
  private byte[] currentMessageId;
  private T current;
  private Instant currentTimestamp;
  private final long statsPeriodMs = 120000;
  private final EndpointProperties endpointProps = new EndpointProperties();
  public final SolaceReaderStats readerStats;
  private ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
  public AtomicLong watermark = new AtomicLong(0);
  public AtomicBoolean isActive = new AtomicBoolean(true); // Only set to false after timeout
  public AtomicBoolean endMonitor = new AtomicBoolean(false);


  /**
   * Queue to place advanced messages before {@link #getCheckpointMark()} be
   * called non concurrent queue, should only be accessed by the reader thread A
   * given {@link UnboundedReader} object will only be accessed by a single thread
   * at once.
   */
    private java.util.Queue<Message> wait4cpQueue = new LinkedList<>();

  public class PrintingPubCallback implements JCSMPStreamingPublishEventHandler {
    public void handleError(String messageId, JCSMPException cause, long timestamp) {
      LOG.error("Error occurred for Solace queue depth request message: " + messageId);
      cause.printStackTrace();
    }

    // This method is only invoked for persistent and non-persistent
    // messages.
    public void responseReceived(String messageId) {
      LOG.error("Unexpected response to Solace queue depth request message: " + messageId);
    }
  }

  private class ActivityMonitor<T> extends Thread {
    private UnboundedSolaceReader<T> reader;
    private int timeout;
    private final int debounce = 60;

    protected ActivityMonitor (UnboundedSolaceReader<T> reader, int timeout) {
      this.reader = reader;
      this.timeout = timeout;
    }
    public void run() {
      while (!reader.endMonitor.get()) {
        try {
          reader.readerStats.incrementMonitorChecks();
          Thread.sleep(timeout * debounce);
          if (!reader.isActive.get()) {
            reader.flowReceiver.close();
          }
          reader.isActive.set(false);
        } catch (Exception ex) {
          ex.printStackTrace();
          throw new RuntimeException(ex);
        }
      }
    }
  }

  public UnboundedSolaceReader(UnboundedSolaceSource<T> source) {
    this.source = source;
    this.current = null;
    watermark.getAndSet(System.currentTimeMillis());
    this.readerStats = new SolaceReaderStats();
  }

  @Override
  public boolean start() throws IOException {
    LOG.info("Starting UnboundSolaceReader for Solace queue: {} ...", source.queue.get());
    SolaceIO.Read<T> spec = source.getSpec();
    try {
      SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration().get();
      final JCSMPProperties properties = new JCSMPProperties();
      properties.setProperty(JCSMPProperties.HOST, cc.getHost()); // host:port
      properties.setProperty(JCSMPProperties.USERNAME, cc.getUsername()); // client-username
      properties.setProperty(JCSMPProperties.PASSWORD, cc.getPassword()); // client-password
      properties.setProperty(JCSMPProperties.VPN_NAME, cc.getVpn()); // message-vpn

      if (cc.getClientName() != null) {
        properties.setProperty(JCSMPProperties.CLIENT_NAME, cc.getClientName()); // message-vpn
      }

      session = JCSMPFactory.onlyInstance().createSession(properties);
      session.getMessageProducer(new PrintingPubCallback());
      XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener)null); consumer.start();
      clientName = (String) session.getProperty(JCSMPProperties.CLIENT_NAME);
      session.connect();

      String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);
      final String sempShowTopicString = String.format("#SEMP/%s/SHOW", routerName);
      sempShowTopic = JCSMPFactory.onlyInstance().createTopic(sempShowTopicString);

      // do NOT provision the queue, so "Unknown Queue" exception will be threw if the
      // queue is not existed already
      final Queue queue = JCSMPFactory.onlyInstance().createQueue(source.queue.get());

      // Create a Flow be able to bind to and consume messages from the Queue.
      flow_prop.setEndpoint(queue);
      isAutoAck = cc.isAutoAck();
      isAutoAck = spec.connectionConfiguration().get().isAutoAck();
      if (isAutoAck) {
        // auto ack the messages
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
      } else {
        // will ack the messages in checkpoint
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      }

      this.useSenderTimestamp = cc.isSenderTimestamp();
      this.useSenderMessageId = cc.isSenderMessageId();

      readerStats.setLastReportTime(Instant.now());
     
      // bind to the queue, passing null as message listener for no async callback
      flowReceiver = session.createFlow(null, flow_prop, endpointProps);
      // Start the consumer
      flowReceiver.start();
      LOG.info("Binding Solace session [{}] to queue[{}]...", clientName, source.queue.get());

      // Create Monitor Thread
      ActivityMonitor myMonitor = new ActivityMonitor(this, cc.getTimeoutInMillis());
      myMonitor.start();
      return advance();

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new IOException(ex);
    }
  }

  @Override
  public boolean advance() throws IOException {
    //LOG.debug("Advancing Solace session [{}] on queue [{}]...", this.clientName, source.getQueueName());
    Instant timeNow = Instant.now();
    this.isActive.set(true);
    readerStats.setCurrentAdvanceTime(timeNow);
    long deltaTime = timeNow.getMillis() - readerStats.getLastReportTime().getMillis();
    if (deltaTime >= this.statsPeriodMs) {
      LOG.info("Stats for Queue [{}] : {} from client [{}]", source.getQueueName(), readerStats.dumpStatsAndClear(true), this.clientName);
      readerStats.setLastReportTime(timeNow);
    }
    SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration().get();
    try {
      BytesXMLMessage msg = flowReceiver.receive(cc.getTimeoutInMillis());
      if (msg == null) {
        readerStats.incrementEmptyPoll();
        return false;
      }
      readerStats.incrementMessageReceived();
      current = this.source.getSpec().messageMapper().mapMessage(msg);

      // if using sender timestamps use them, else use current time.
      if (useSenderTimestamp) {
        Long solaceTime = msg.getSenderTimestamp();
        if (solaceTime == null) {
            currentTimestamp = Instant.now();
          } else {
            currentTimestamp = new Instant(solaceTime.longValue());
          }
      } else {
        currentTimestamp = Instant.now();
      }

      //Get messageID for de-dupping, best to use Producer Sequence Number as MessageID resets over connections
      if (useSenderMessageId) {
        Long seqNum = msg.getSequenceNumber();
        if (seqNum == null) {
          currentMessageId = msg.getMessageId().getBytes();
        } else {
          currentMessageId = Longs.toByteArray(seqNum);
        }
      } else {
        currentMessageId = msg.getMessageId().getBytes();
      }

      // add message to checkpoint ack if not autoack
      if (!isAutoAck) {
        wait4cpQueue.add(new Message(msg, currentTimestamp));
      }
    } catch (JCSMPException ex) {
      try {
        LOG.info("JCSMPException for from client [{}] : {}", this.clientName, ex.getMessage());
        flowReceiver.close();
        flowReceiver = session.createFlow(null, flow_prop, endpointProps);
        flowReceiver.start();
      } catch (JCSMPException restartEx) {
        LOG.error("Unrecoverable JCSMPException for from client [{}] : {}", this.clientName, ex.getMessage());
        throw new IOException(restartEx);
      }
      return false;
  //  } catch (JCSMPException ex) {
  //    LOG.warn("JCSMPException for from client [{}] : {}", this.clientName, ex.getMessage());
  //    return false;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Close the Solace session [{}] on queue[{}]...", clientName, source.queue.get());
    active.set(false);
    try {
      if (flowReceiver != null) {
        flowReceiver.close();
      }
      if (session != null) {
        session.closeSession();
      }
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    // TODO: add finally block to close session.
  }

    /**
     * Direct Runner will call this method on every second
     */
    @Override
    public Instant getWatermark() {
        if (watermark == null){
            return new Instant(0);
        }
        return new Instant(watermark.get());
    }

    static class Message implements Serializable {
      private static final long serialVersionUID = 42L;
      BytesXMLMessage message;
      Instant time;

      public Message(BytesXMLMessage message, Instant time) {
        this.message = message;
        this.time = time;
      }
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        if (!isAutoAck) {
            // put all messages in wait4cp to safe2ack
            // and clean the wait4cp queue in the same time
            BlockingQueue<Message> ackQueue = new LinkedBlockingQueue<>();
            try {
                Message msg = wait4cpQueue.poll();
                while (msg != null) {
                    ackQueue.put(msg);
                    msg = wait4cpQueue.poll();
                }
            } catch (Exception e) {
                LOG.error("Got exception while putting into the blocking queue: {}", e);
            }
            readerStats.setCurrentCheckpointTime(Instant.now());
            readerStats.incrCheckpointReadyMessages(new Long(ackQueue.size()));
            return new SolaceCheckpointMark(this, clientName, ackQueue);
        } else {
            return new UnboundedSource.CheckpointMark() {
                public void finalizeCheckpoint() throws IOException {
                    // nothing to do
                }
            };
        }
    }

  @Override
  public T getCurrent() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public Instant getCurrentTimestamp() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return currentTimestamp;
  }

  @Override
  public UnboundedSolaceSource<T> getCurrentSource() {
    return source;
  }

  private String queryRouter(String queryString, String searchString) throws Exception {
    Requestor requestor = session.createRequestor();
    ByteArrayInputStream input;
    BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
    requestMsg.writeAttachment(queryString.getBytes());
    BytesXMLMessage replyMsg = requestor.request(requestMsg, 5000, sempShowTopic);
    byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
    replyMsg.readAttachmentBytes(bytes);
    input = new ByteArrayInputStream(bytes);
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(input);
    XPath xpath = XPathFactory.newInstance().newXPath();
    String expression = searchString;
    Node node = (Node) xpath.compile(expression).evaluate(doc, XPathConstants.NODE);
    return node.getTextContent();
  }

  private long queryQueueBytes(String queueName, String vpnName) {
    LOG.debug("Enter queryQueueBytes() Queue: [{}] VPN: [{}]",  queueName, vpnName);
    long queueBytes = UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    String sempShowQueue = "<rpc><show><queue><name>" + queueName + "</name>";
    sempShowQueue += "<vpn-name>" + vpnName + "</vpn-name></queue></show></rpc>";
    String queryString = "/rpc-reply/rpc/show/queue/queues/queue/info/current-spool-usage-in-bytes";
    try {
      String queryResults = queryRouter(sempShowQueue, queryString);
      queueBytes = Long.parseLong(queryResults);
    } catch (JCSMPException ex) {
      LOG.error("Encountered a JCSMPException querying queue depth: {}", ex.getMessage());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    } catch (Exception ex) {
      LOG.error("Encountered a Parser Exception querying queue depth: {}", ex.toString());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }
    return queueBytes;
  }

  @Override
  public long getSplitBacklogBytes() {
    LOG.debug("Enter getSplitBacklogBytes()");
    SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration().get();
     long backlogBytes = queryQueueBytes(source.getQueueName(), cc.getVpn());
    if (backlogBytes == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN) {
      LOG.error("getSplitBacklogBytes() unable to read bytes from: {}", source.getQueueName());
      return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
    }
    readerStats.setCurrentBacklog(new Long(backlogBytes));
    LOG.debug("getSplitBacklogBytes() Reporting backlog bytes of: {} from queue {}", 
        Long.toString(backlogBytes), source.getQueueName());
    return backlogBytes;
  }

  @Override
  public byte[] getCurrentRecordId() {
    return currentMessageId;
  }

  @Override
  protected void finalize() throws Throwable {
    this.endMonitor.set(true);
    session.closeSession();
  }
}
