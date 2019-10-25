package org.apache.beam.sdk.io.solace;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import com.solacesystems.jcsmp.BytesXMLMessage;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import java.io.Serializable;

import java.util.List;

import javax.annotation.Nullable;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class SolaceIO {

  //private static final Logger LOG = LoggerFactory.getLogger(SolaceIO.class);

  /**
   * Read Solace message as a STRING.
   */
  public static Read<String> readAsString() {
    return new AutoValue_SolaceIO_Read.Builder<String>()
        .setMaxReadTime(null).setMaxNumRecords(Long.MAX_VALUE)
        .setCoder(StringUtf8Coder.of()).setMessageMapper(new StringMessageMapper()).build();
  }

  /**
   * Read Solace message, the user must set the message maper and coder.
   */
  public static <T> Read<T> readMessage() {
    return new AutoValue_SolaceIO_Read.Builder<T>().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  private SolaceIO() {
  }

  /**
   * An interface used by {@link SolaceIO.Read} for converting each Solace Message
   * into an element of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface MessageMapper<T> extends Serializable {
    T mapMessage(BytesXMLMessage message) throws Exception;
  }

  /** A POJO describing a Solace SMF connection. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {
    private static final long serialVersionUID = 42L;

    @Nullable
    abstract String getHost();

    @Nullable
    abstract String getClientName();

    @Nullable
    abstract String getVpn();

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();
    
    abstract boolean isAutoAck();
    
    abstract boolean isSenderTimestamp();

    abstract boolean isSenderMessageId();

  

    // The timeout in milliseconds while try to receive a messages from Solace
    // broker
    abstract int getTimeoutInMillis();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);

      abstract Builder setClientName(String clientName);

      abstract Builder setVpn(String vpn);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setAutoAck(boolean autoAck);

      abstract Builder setSenderTimestamp(boolean useSenderTimestamp);

      abstract Builder setSenderMessageId(boolean useSenderMessageId);


      abstract Builder setTimeoutInMillis(int timeoutInMillis);
      

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Solace connection configuration from provided hostname and queue list.
     */
    public static ConnectionConfiguration create(String host) {
      checkArgument(host != null, "host can not be null");

      return new AutoValue_SolaceIO_ConnectionConfiguration.Builder()
          .setHost(host)
          .setAutoAck(false)
          .setSenderTimestamp(false)
          .setSenderMessageId(false)  
          .setTimeoutInMillis(500)
          .build();
    }

    /**
     * Set the Solace connection configuration messaging VPN.
     */
    public ConnectionConfiguration withVpn(String vpn) {
      return builder().setVpn(vpn).build();
    }

    /**
     * Set the Solace connection configuration username and VPN.
     */
    public ConnectionConfiguration withUsername(String username) {
      ConnectionConfiguration.Builder bldr = builder();
      String[] args = username.split("@");
      if (args.length == 2) {
        bldr = bldr.setVpn(args[1]);
      } else {
        bldr = bldr.setVpn("default");
      }
      return bldr.setUsername(args[0]).build();
    }

    public ConnectionConfiguration withPassword(String password) {
      return builder().setPassword(password).build();
    }

    public ConnectionConfiguration withAutoAck(boolean autoAck) {
      return builder().setAutoAck(autoAck).build();
    }

    public ConnectionConfiguration withSenderTimestamp(boolean useSenderTimestamp) {
      return builder().setSenderTimestamp(useSenderTimestamp).build();
    }

    public ConnectionConfiguration withSenderMessageId(boolean useSenderMessageId) {
      return builder().setSenderMessageId(useSenderMessageId).build();
    }

    public ConnectionConfiguration withTimeout(int timeoutInMillis) {
      return builder().setTimeoutInMillis(timeoutInMillis).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("host", getHost()));
      builder.add(DisplayData.item("vpn", getVpn()));
      builder.addIfNotNull(DisplayData.item("clientName", getClientName()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }
  }

  /**
   *  A {@link PTransform} to read from a Solace broker. 
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    private static final long serialVersionUID = 42L;

    @Nullable
    abstract ValueProvider<ConnectionConfiguration> connectionConfiguration();

    @Nullable
    abstract List<ValueProvider<String>> queues();

    abstract long maxNumRecords();

    @Nullable
    abstract Duration maxReadTime();

    @Nullable
    abstract MessageMapper<T> messageMapper();

    @Nullable
    abstract Coder<T> coder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ValueProvider<ConnectionConfiguration> config);

      abstract Builder<T> setQueues(List<ValueProvider<String>> queues);

      abstract Builder<T> setMaxNumRecords(long maxNumRecords);

      abstract Builder<T> setMaxReadTime(Duration maxReadTime);

      abstract Builder<T> setMessageMapper(MessageMapper<T> mesageMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Read<T> build();
    }

    public Read<T> withConnectionConfiguration(ValueProvider<ConnectionConfiguration> configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
    }

    public Read<T> withQueues(List<ValueProvider<String>> queues) {
      checkArgument(queues != null && queues.size()>0, "queues can not be null or empty");
      return builder().setQueues(queues).build();
    }
    /**
     * Define the max number of records received by the {@link Read}. When this max
     * number of records is lower than {@code Long.MAX_VALUE}, the {@link Read} will
     * provide a bounded {@link PCollection}.
     */
    public Read<T> withMaxNumRecords(long maxNumRecords) {
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive
     * messages. When this max read time is not null, the {@link Read} will provide
     * a bounded {@link PCollection}.
     */
    public Read<T> withMaxReadTime(Duration maxReadTime) {
      return builder().setMaxReadTime(maxReadTime).build();
    }

    public Read<T> withMessageMapper(MessageMapper<T> messageMapper) {
      checkArgument(messageMapper != null, "messageMapper can not be null");
      return builder().setMessageMapper(messageMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");
      checkArgument(messageMapper() != null, "withMessageMapper() is required");
      checkArgument(coder() != null, "withCoder() is required");

      org.apache.beam.sdk.io.Read.Unbounded<T> unbounded = org.apache.beam.sdk.io.Read
          .from(new UnboundedSolaceSource<T>(this));

      PTransform<PBegin, PCollection<T>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
       // cannot do at graph construction time, since we don't have the queues yet
//     connectionConfiguration().get().populateDisplayData(builder);
      if (maxNumRecords() != Long.MAX_VALUE) {
        builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      }
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }
  }

}
