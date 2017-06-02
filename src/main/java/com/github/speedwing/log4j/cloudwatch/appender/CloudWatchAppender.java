package digitalfusion.studio.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;
import digitalfusion.shared.concurrent.NamedThreadFactory;
import digitalfusion.shared.di.DiSession;
import digitalfusion.studio.StudioDiContext;
import digitalfusion.studio.authentication.AuthenticationDiContext;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import static java.util.stream.Collectors.toList;

/**
 * CloudWatchAppender<p>
 *
 * Based on https://github.com/speedwing/log4j-cloudwatch-appender, customized for DF Studio
 */
public class CloudWatchAppender extends AppenderSkeleton {

  /**
   * The queue used to buffer log entries
   */
  private LinkedBlockingQueue<LoggingEvent> loggingEventsQueue;

  /**
   * the AWS CloudWatch Logs API client
   */
  private AWSLogs awsLogsClient;

  private final AtomicReference<String> lastSequenceToken = new AtomicReference<>();

  /**
   * The AWS CloudWatch Log group name
   */
  private String logGroupName;

  /**
   * The AWS CloudWatch Log stream name
   */
  private String logStreamName;

  /**
   * The queue / buffer size
   */
  private int queueLength = 1024;

  /**
   * The maximum number of log entries to send in one go to the AWS CloudWatch Log service
   */
  private int messagesBatchSize = 128;

  private final AtomicBoolean cloudwatchAppenderInitialized = new AtomicBoolean(false);

  private static final Logger log = Logger.getLogger(CloudWatchAppender.class);


  public CloudWatchAppender() {
    super();
  }

  public CloudWatchAppender(Layout layout, String logGroupName, String logStreamName) {
    super();
    this.setLayout(layout);
    this.setLogGroupName(logGroupName);
    this.setLogStreamName(logStreamName);
    this.activateOptions();
  }

  public void setLogGroupName(String logGroupName) {
    this.logGroupName = logGroupName;
  }

  public void setLogStreamName(String logStreamName) {
    this.logStreamName = logStreamName;
  }

  public void setQueueLength(int queueLength) {
    this.queueLength = queueLength;
  }

  public void setMessagesBatchSize(int messagesBatchSize) {
    this.messagesBatchSize = messagesBatchSize;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (cloudwatchAppenderInitialized.get()) {
      loggingEventsQueue.offer(event);
    } else {
      // just do nothing
    }
  }

  private synchronized void sendMessages() {
    LoggingEvent polledLoggingEvent;

    List<LoggingEvent> loggingEvents = new ArrayList<>();

    try {
      while ((polledLoggingEvent = loggingEventsQueue.poll()) != null && loggingEvents.size() <= messagesBatchSize) {
        loggingEvents.add(polledLoggingEvent);
      }

      List<InputLogEvent> inputLogEvents = loggingEvents.stream()
          .map(loggingEvent -> new InputLogEvent().withTimestamp(loggingEvent.getTimeStamp()).withMessage(layout.format(loggingEvent)))
          .collect(toList());

      if (!inputLogEvents.isEmpty()) {

        PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(
            logGroupName,
            logStreamName,
            inputLogEvents);

        try {
          putLogEventsRequest.setSequenceToken(lastSequenceToken.get());
          PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
          lastSequenceToken.set(result.getNextSequenceToken());
        } catch (InvalidSequenceTokenException invalidSequenceTokenException) {
          putLogEventsRequest.setSequenceToken(invalidSequenceTokenException.getExpectedSequenceToken());
          PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
          lastSequenceToken.set(result.getNextSequenceToken());
          if (log.isDebugEnabled()) {
            log.debug(invalidSequenceTokenException, invalidSequenceTokenException);
          }
        }
      }
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug(e, e);
      }
    }
  }

  @Override
  public void close() {
    while (loggingEventsQueue != null && !loggingEventsQueue.isEmpty()) {
      this.sendMessages();
    }
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  @Override
  public void activateOptions() {
    super.activateOptions();
    DiSession diSession = StudioDiContext.DI_CONTEXT.newSession(getClass());
    try {
      AWSCredentialsProvider awsCredentialsProvider = diSession.getInstance(AWSCredentialsProvider.class);
      if (isBlank(logGroupName)) {
        logGroupName = "dfstudio-" + diSession.getInstance(StudioDiContext.KEY_CLUSTER_NAME, String.class);
      }
      if (isBlank(logStreamName)) {
        String streamName = "app-" + diSession.getInstance(StudioDiContext.KEY_INSTANCE_NAME, String.class);
        String instanceId = diSession.tryInstance(AuthenticationDiContext.KEY_INSTANCE_ID, "");
        if (!isBlank(instanceId)) {
          streamName += ("-" + instanceId);
        }
        logStreamName = streamName;
      }
      if (isBlank(logGroupName) || isBlank(logStreamName)) {
        log.error("Could not initialize CloudWatchAppender because one or both of LogGroupName(" + logGroupName + ")"
            + " and LogStreamName(" + logStreamName + ") are null or empty");
        this.close();
      } else {
        this.awsLogsClient = AWSLogsClientBuilder.standard().withCredentials(awsCredentialsProvider).build();
        loggingEventsQueue = new LinkedBlockingQueue<>(queueLength);
        initializeCloudWatchLoggingResources();
        initScheduledExecutor();
        cloudwatchAppenderInitialized.set(true);
      }
    } catch (Exception e) {
      log.error("Could not initialize CloudWatch Logs for LogGroupName: " + logGroupName
          + " and LogStreamName: " + logStreamName, e);

    } finally {
      diSession.release();
    }
  }

  private void initScheduledExecutor() {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, NamedThreadFactory.background("cloudwatch-appender"));
    executorService.scheduleAtFixedRate(() -> {
      try {
        if (loggingEventsQueue.size() > 0) {
          sendMessages();
        }
      } catch (Exception e) {
        log.warn(e.getMessage(), e);
      }
    }, 5, 5, TimeUnit.SECONDS);
  }

  private void initializeCloudWatchLoggingResources() {

    DescribeLogGroupsRequest describeLogGroupsRequest = new DescribeLogGroupsRequest();
    describeLogGroupsRequest.setLogGroupNamePrefix(logGroupName);

    Optional<LogGroup> logGroupOptional = awsLogsClient
        .describeLogGroups(describeLogGroupsRequest)
        .getLogGroups()
        .stream()
        .filter(logGroup -> logGroup.getLogGroupName().equals(logGroupName))
        .findFirst();

    if (!logGroupOptional.isPresent()) {
      CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest().withLogGroupName(logGroupName);
      if (log.isInfoEnabled()) log.info("Creating LogGroup: " + logGroupName);
      awsLogsClient.createLogGroup(createLogGroupRequest);
    }

    DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest()
        .withLogGroupName(logGroupName)
        .withLogStreamNamePrefix(logStreamName);

    Optional<LogStream> logStreamOptional = awsLogsClient
        .describeLogStreams(describeLogStreamsRequest)
        .getLogStreams()
        .stream()
        .filter(logStream -> logStream.getLogStreamName().equals(logStreamName))
        .findFirst();

    if (!logStreamOptional.isPresent()) {
      if (log.isInfoEnabled()) log.info("Creating LogStream: " + logStreamName + " in LogGroup: " + logGroupName);
      CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest()
          .withLogGroupName(logGroupName)
          .withLogStreamName(logStreamName);
      awsLogsClient.createLogStream(createLogStreamRequest);
    }

  }

  private boolean isBlank(String string) {
    return null == string || string.trim().length() == 0;
  }

}
