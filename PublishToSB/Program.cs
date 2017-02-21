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
            // Endpoint=sb://sb-sbb-poc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Cy1XqTOptinUgrjAWn29p5M8y8pgfYkFBELA6XES9yU=
            var sasKeyName = "RootManageSharedAccessKey";
            var sasKeyValue = "Cy1XqTOptinUgrjAWn29p5M8y8pgfYkFBELA6XES9yU=";
            var sbNamespace = "sb-sbb-poc";

            // Create management credentials
            TokenProvider credentials = TokenProvider.CreateSharedAccessSignatureTokenProvider(sasKeyName, sasKeyValue);

            // Create SB service uri
            var sbUri = ServiceBusEnvironment.CreateServiceUri("sb", sbNamespace, string.Empty);

            // Create namespace client
            NamespaceManager namespaceClient = new NamespaceManager(sbUri, credentials);

            // Create topic
            var topicName = "ProfielfotoMutaties";
            var topic = namespaceClient.TopicExists(topicName)
                ? namespaceClient.GetTopic(topicName)
                : namespaceClient.CreateTopic(new TopicDescription(topicName)
                {
                    DefaultMessageTimeToLive = TimeSpan.FromDays(3),
                    EnablePartitioning = true,
                    MaxSizeInMegabytes = 5 * 1024, // 5GB
                    SupportOrdering = true,
                });

            // Create subscription
            var subscriptionName = "ProfielfotoMutatiesNaarBackoffice";
            var subscription = namespaceClient.SubscriptionExists(topicName, subscriptionName)
                ? namespaceClient.GetSubscription(topicName, subscriptionName)
                : namespaceClient.CreateSubscription(new SubscriptionDescription(topicName, subscriptionName)
                {
                    LockDuration = TimeSpan.FromSeconds(120),
                    MaxDeliveryCount = 10, // retries
                });

            // Create Topic and Subscription clients
            var msgFactory = MessagingFactory.Create(sbUri, credentials);
            var topicClient = msgFactory.CreateTopicClient(topicName);
            var subscriptionClient = msgFactory.CreateSubscriptionClient(topicName, subscriptionName, ReceiveMode.PeekLock);

            // Send message to topic
            topicClient.Send(CreateMessage());

            // Get message from subscription
            var subscriptionMsg = subscriptionClient.Receive(TimeSpan.FromSeconds(5));
            if (subscriptionMsg != null)
            {
                using (var outputStream = File.OpenWrite("profielfoto-ontvangen.gif"))
                {
                    subscriptionMsg.GetBody<Stream>().CopyTo(outputStream);
                    outputStream.Close();
                }
                subscriptionMsg.Complete();
            }

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
            topicClient.Send(CreateMessage("1"));
            topicClient.Send(CreateMessage("2"));
            topicClient.Send(CreateMessage("3"));
            topicClient.Send(CreateMessage("1"));

            handle.WaitOne(); // Wait until Ctrl-C is pressed
            subscriptionClient.Close();

            // Publish a last message
            topicClient.Send(CreateMessage("4"));
        }

        public static BrokeredMessage CreateMessage(string id = "5")
        {
            var topicMsg = new BrokeredMessage(File.OpenRead("profielfoto.gif"), true)
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = "Profielfoto",
                SessionId = id,
                PartitionKey = id,
            };
            topicMsg.Properties["UserID"] = "5";
            return topicMsg;
        }
    }
}
