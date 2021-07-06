package com.solace.connector.beam;

import com.google.common.annotations.VisibleForTesting;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


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
	@Nullable
	protected FlowReceiver flowReceiver;
	private MsgBusSempUtil msgBusSempUtil;
	private boolean useSenderTimestamp;
	private String clientName;
	private T current;
	private Instant currentTimestamp;
	private final EndpointProperties endpointProps = new EndpointProperties();
	final SolaceReaderStats readerStats;
	private ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
	AtomicLong watermark = new AtomicLong(0);
	private AtomicBoolean isActive = new AtomicBoolean(true); // Only set to false after timeout
	private AtomicBoolean endMonitor = new AtomicBoolean(false);

	private static final long statsPeriodMs = 120000;


	/**
	 * Queue to place advanced messages before {@link #getCheckpointMark()} be
	 * called non concurrent queue, should only be accessed by the reader thread A
	 * given {@link UnboundedReader} object will only be accessed by a single thread
	 * at once.
	 */
	private java.util.Queue<Message> wait4cpQueue = new LinkedList<>();

	private static class ActivityMonitor<T> extends Thread {
		private UnboundedSolaceReader<T> reader;
		private int timeout;

		private static final int debounce = 300;

		protected ActivityMonitor(UnboundedSolaceReader<T> reader, int timeout) {
			this.reader = reader;
			this.timeout = timeout;
		}

		public void run() {
			while (!reader.endMonitor.get()) {
				try {
					reader.readerStats.incrementMonitorChecks();
					Thread.sleep(timeout * debounce);
					if (!reader.isActive.get()) {
						if (reader.flowReceiver != null) {
							reader.flowReceiver.close();
						}
						reader.readerStats.incrementMonitorFlowClose();
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

	private boolean prepareFlow() throws IOException {
		if (flowReceiver == null) {
			try {
				// bind to the queue, passing null as message listener for no async callback
				flowReceiver = session.createFlow(null, flow_prop, endpointProps);
				// Start the consumer
				flowReceiver.start();
				LOG.info("Binding Solace session [{}] to queue[{}]...", this.clientName, source.getQueueName());
			} catch (JCSMPException ex) {
				LOG.error(String.format("JCSMPException from client [%s] : %s", this.clientName, ex.getMessage()), ex);
				throw new IOException(ex);
			}
		}
		return true;
	}

	@Override
	public boolean start() throws IOException {
		LOG.info("Starting UnboundSolaceReader for Solace queue: {} ...", source.getQueueName());
		try {
			final JCSMPProperties properties = source.getSpec().jcsmpProperties();
			session = JCSMPFactory.onlyInstance().createSession(properties);
			clientName = (String) session.getProperty(JCSMPProperties.CLIENT_NAME);
			session.connect();

			msgBusSempUtil = new MsgBusSempUtil(session);
			msgBusSempUtil.start();

			// do NOT provision the queue, so "Unknown Queue" exception will be threw if the
			// queue is not existed already
			final Queue queue = JCSMPFactory.onlyInstance().createQueue(source.getQueueName());

			// Create a Flow be able to bind to and consume messages from the Queue.
			flow_prop.setEndpoint(queue);

			// will ack the messages in checkpoint
			flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

			this.useSenderTimestamp = source.getSpec().useSenderTimestamp();

			readerStats.setLastReportTime(Instant.now());

			prepareFlow();

			// Create Monitor Thread
			ActivityMonitor<T> myMonitor = new ActivityMonitor<>(this, source.getSpec().advanceTimeoutInMillis());
			myMonitor.start();
			return advance();

		} catch (Exception ex) {
			String msg = String.format("Failed to start UnboundSolaceReader for Solace session %s for queue: %s",
					clientName, source.getQueueName());
			LOG.error(msg, ex);
			throw new IOException(msg, ex);
		}
	}

	@Override
	public boolean advance() throws IOException {
		prepareFlow();

		LOG.debug("Advancing Solace session [{}] on queue [{}]...", this.clientName, source.getQueueName());
		Instant timeNow = Instant.now();
		this.isActive.set(true);
		readerStats.setCurrentAdvanceTime(timeNow);
		long deltaTime = timeNow.getMillis() - readerStats.getLastReportTime().getMillis();
		if (deltaTime >= statsPeriodMs) {
			LOG.info("Stats for Queue [{}] : {} from client [{}]", source.getQueueName(), readerStats.dumpStatsAndClear(true), this.clientName);
			readerStats.setLastReportTime(timeNow);
		}
		try {
			BytesXMLMessage msg = flowReceiver.receive(source.getSpec().advanceTimeoutInMillis());
			if (msg == null) {
				readerStats.incrementEmptyPoll();
				return false;
			}
			readerStats.incrementMessageReceived();
			current = this.source.getSpec().inboundMessageMapper().map(msg);

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

			// add message to checkpoint ack
			wait4cpQueue.add(new Message(msg, currentTimestamp));
		} catch (JCSMPException ex) {
			try {
				LOG.info("JCSMPException from client [{}] : {}", this.clientName, ex.getMessage());
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
		LOG.info("Close the Solace session [{}] on queue[{}]...", clientName, source.getQueueName());
		active.set(false);
		try {
			if (flowReceiver != null) {
				flowReceiver.close();
			}

			if (msgBusSempUtil != null) {
				msgBusSempUtil.close();
			}
		} catch (Exception ex) {
			throw new IOException(ex);
		} finally {
			if (session != null && !session.isClosed()) {
				session.closeSession();
			}
		}
	}

	/**
	 * Direct Runner will call this method on every second
	 */
	@Override
	public Instant getWatermark() {
		if (watermark == null) {
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
			LOG.error(String.format("Got exception while putting into the blocking queue: %s", e.toString()), e);
		}
		readerStats.setCurrentCheckpointTime(Instant.now());
		readerStats.incrCheckpointReadyMessages((long) ackQueue.size());

		// pass off flowReceiver to the created CheckpointMark and set it to null
		SolaceCheckpointMark mark = new SolaceCheckpointMark(this, flowReceiver, clientName, ackQueue);
		flowReceiver = null;
		return mark;
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

	private long queryQueueBytes(String queueName, String vpnName) {
		LOG.info("Enter queryQueueBytes() Queue: [{}] VPN: [{}]", queueName, vpnName);
		long queueBytes = UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
		String sempShowQueue = String.format("<rpc><show><queue><name>%s</name><vpn-name>%s</vpn-name></queue></show></rpc>",
				queueName, vpnName);
		String queryString = "/rpc-reply/rpc/show/queue/queues/queue/info/num-messages-spooled";

		// start the reader if it isn't started (this is an issue with the Beam SDK 2.25+ because the getSplitBacklogBytes caller creates but doesn't start the reader)
		boolean startSession = (msgBusSempUtil == null);
		try {
			if (startSession) { start(); }
			String queryResults = msgBusSempUtil.queryRouter(sempShowQueue, queryString);
			queueBytes = Long.parseLong(queryResults);
		} catch (JCSMPException e) {
			// downgraded from error because the worker still returns a valid value -- unknown amount of data left
			// mainly affects the scaling responsiveness of the pipeline in Dataflow and requires a large number of messages remaining consistently before scaling would be triggered
			// it instead distracts from other errors because once this triggers (e.g. due to having more clients that expected), it will keep popping up
			LOG.warn(String.format("Encountered a JCSMPException querying queue depth: %s", e.getMessage()), e);
			return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
		} catch (Exception e) {
			// see above
			LOG.warn(String.format("Encountered a Parser Exception querying queue depth: %s", e.toString()), e);
			return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
		} finally {
			try {
				// if the session was started as a result of this function, close it to avoid leaking consumers
				if (startSession) { close(); }
			}
			catch (Exception e) {
				LOG.error("Failed to close started session.", e);
			}
		}
		return queueBytes;
	}

	@Override
	public long getSplitBacklogBytes() {
		LOG.debug("Enter getSplitBacklogBytes()");
		long backlogBytes = queryQueueBytes(source.getQueueName(),
				source.getSpec().jcsmpProperties().getStringProperty(JCSMPProperties.VPN_NAME));
		if (backlogBytes == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN) {
			LOG.warn("getSplitBacklogBytes() unable to read bytes from: {}", source.getQueueName());
			return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
		}
		readerStats.setCurrentBacklog(backlogBytes);
		LOG.debug("getSplitBacklogBytes() Reporting backlog bytes of: {} from queue {}",
				Long.toString(backlogBytes), source.getQueueName());
		return backlogBytes;
	}

	@Override
	protected void finalize() throws Throwable {
		this.endMonitor.set(true);
		if (session != null && !session.isClosed()) {
			LOG.info("Finalizing Solace session [{}] on queue [{}]...", clientName, source.getQueueName());
			session.closeSession();
		}
	}
}
