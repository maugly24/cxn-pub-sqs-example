/*
  Queue handling example heavily influenced by the official AWS SDK examples
 */
package cxn.pub.sqs.example;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SendReceiveMessages
{
    private static final String QUEUE_NAME = "testQueue.fifo";

    public static void main(String[] args)
    {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        try {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("FifoQueue", "true");
            CreateQueueRequest req = new CreateQueueRequest().withQueueName(QUEUE_NAME).withAttributes(attributes);
            CreateQueueResult create_result = sqs.createQueue(req);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }

        String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("hello world")
                .withMessageGroupId("TestGroup")
                .withMessageDeduplicationId("JustTest01");
        sqs.sendMessage(send_msg_request);


        // Send multiple messages to the queue
        SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(
                        new SendMessageBatchRequestEntry(
                                "msg_1", "Hello from message 1")
                                .withMessageGroupId("Hello")
                                .withMessageDeduplicationId("Hello01"),
                        new SendMessageBatchRequestEntry(
                                "msg_2", "Hello from message 2")
                                .withMessageGroupId("Hello")
                                .withMessageDeduplicationId("Hello02"));
        sqs.sendMessageBatch(send_batch_request);

        // receive messages from the queue
        ReceiveMessageRequest req = new ReceiveMessageRequest().withQueueUrl(QUEUE_NAME).withMaxNumberOfMessages(10);
        List<Message> messages = sqs.receiveMessage(req).getMessages();

        // delete messages from the queue
        for (Message m : messages) {
            System.out.println(m.getBody());
            System.out.println(m.getAttributes());
            System.out.println(m.getMessageAttributes());
            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }

//        sqs.deleteQueue(QUEUE_NAME);
    }
}
