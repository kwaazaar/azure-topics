using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PublishToSB
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Connect
            // Endpoint=sb://sb-sbb-poc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Cy1XqTOptinUgrjAWn29p5M8y8pgfYkFBELA6XES9yU=
            var sasKeyName = "RootManageSharedAccessKey";
            var sasKeyValue = "Cy1XqTOptinUgrjAWn29p5M8y8pgfYkFBELA6XES9yU=";
            var sbNamespace = "sb-sbb-poc";

            // Connect to SB
            var credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(sasKeyName, sasKeyValue);
            var sbUri = ServiceBusEnvironment.CreateServiceUri("sb", sbNamespace, string.Empty);
            var nsClient = new NamespaceManager(sbUri, credentials);

            // Create topic if not yet exists
            var topicName = "TopicRobert";
            var topic = nsClient.TopicExists(topicName)
                ? nsClient.GetTopic(topicName)
                : nsClient.CreateTopic(new TopicDescription(topicName)
                {
                    DefaultMessageTimeToLive = TimeSpan.FromDays(3),
                    EnablePartitioning = true,
                    MaxSizeInMegabytes = 5 * 1024, // 5GB
                    SupportOrdering = true,
                });

            // ReCreate subscription
            var subscriptionName = "SubRobert";
            var visibilityTimeout = TimeSpan.FromSeconds(2);
            if (nsClient.SubscriptionExists(topicName, subscriptionName))
                nsClient.DeleteSubscription(topicName, subscriptionName); // Delete existing to quickly clear all existing messages as well
            var subscription = nsClient.CreateSubscription(new SubscriptionDescription(topicName, subscriptionName)
                {
                    LockDuration = visibilityTimeout,
                    MaxDeliveryCount = 10, // retries
                    RequiresSession = true,
                });

            // Create Topic and Subscription clients
            var msgFactory = MessagingFactory.Create(sbUri, credentials);
            var topicClient = msgFactory.CreateTopicClient(topicName);
            var subClient = msgFactory.CreateSubscriptionClient(topicName, subscriptionName, ReceiveMode.PeekLock);
            #endregion

            // Send message to topic
            topicClient.Send(CreateMessage("A", $"1-{DateTime.UtcNow.Ticks}"));
            topicClient.Send(CreateMessage("A", $"2-{DateTime.UtcNow.Ticks}"));
            topicClient.Send(CreateMessage("B", $"1-{DateTime.UtcNow.Ticks}"));

            // Get message from subscription
            var sessions = subClient.GetMessageSessions();

            sessions.AsParallel().ForAll(sesBrowser =>
            {
                var ses = subClient.AcceptMessageSession(sesBrowser.SessionId, TimeSpan.FromSeconds(1));
                if (ses != null)
                {
                    Console.WriteLine($"Session: {ses.SessionId}");
                    var msg = ses.Receive(TimeSpan.FromSeconds(1));
                    Console.WriteLine($"{ses.SessionId}-{msg.Label}-{msg.MessageId} (1)");
                    msg = ses.Receive(TimeSpan.FromSeconds(1));
                    if (msg != null)
                        Console.WriteLine($"{ses.SessionId}-{msg.Label}-{msg.MessageId} (2)");

                    ses.Close();
                }
            });

            //var msg1 = subClient.Receive(TimeSpan.FromSeconds(1));
            //var msg2 = subClient.Receive(TimeSpan.FromSeconds(1));
            //var msg3 = subClient.Receive(TimeSpan.FromSeconds(1));


            #region Poll continuously
            /*
            if (subscriptionMsg != null)
            {
                using (var outputStream = File.OpenWrite("profielfoto-ontvangen.gif.txt"))
                {
                    subscriptionMsg.GetBody<Stream>().CopyTo(outputStream);
                    outputStream.Close();
                }
                subscriptionMsg.Complete();
            }
            */

            /*
            // Poll continuously for new messages
            var options = new OnMessageOptions
            {
                AutoComplete = false,
                AutoRenewTimeout = TimeSpan.FromMinutes(1),
            };
            options.ExceptionReceived += (object sender, ExceptionReceivedEventArgs e) => Console.WriteLine($"Error on receive: {e.Exception}");
            subscriptionClient.OnMessage((message) =>
            {
                try
                {
                    Console.WriteLine($"Got the message with ID {message.MessageId} and partition key {message.PartitionKey} and SessionId {message.SessionId}.");
                    message.Complete(); // Remove message from subscription.
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception during processing: {ex}");
                    message.Abandon(); // Failed. Leave the message for retry or max deliveries is exceeded and it dead letters.
                }
            }, options);

            Console.WriteLine("Press Ctrl+C to stop the worker");
            var handle = new ManualResetEvent(false);
            Console.CancelKeyPress += (s, e) =>
            {
                handle.Set();
                e.Cancel = true;
            }; // Cancel must be true, to make sure the process is not killed and we can clean up nicely below

            // Now that it's polling for new messages, lets publish some...
            //topicClient.Send(CreateMessage("1"));
            //topicClient.Send(CreateMessage("2"));
            //topicClient.Send(CreateMessage("3"));
            //topicClient.Send(CreateMessage("1"));

            handle.WaitOne(); // Wait until Ctrl-C is pressed
            */
            #endregion

            #region Disconnect
            subClient.Close();
            topicClient.Close();
            #endregion
        }

        public static BrokeredMessage CreateMessage(string functionalKey = "5", string stringContent = null)
        {
            Stream contentStream = (stringContent != null)
                ? (Stream)new MemoryStream(Encoding.UTF8.GetBytes(stringContent))
                : File.OpenRead("profielfoto.gif");

            var topicMsg = new BrokeredMessage(contentStream, true)
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = stringContent ?? "ProfielFoto",
                SessionId = functionalKey,
                PartitionKey = functionalKey,
            };
            topicMsg.Properties["FunctionalKey"] = functionalKey;
            return topicMsg;
        }
    }
}
