using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PubSub
{
    class SubStub
    {
        private string myURL;
        private List<BrokerReceiveBroker> brokers;


        public SubStub(string myURL, List<string> p5)
        {

            brokers = new List<BrokerReceiveBroker>();
            this.myURL = myURL;
            fillList(p5);
        }

        public void fillList(List<String> p5)
        {
            foreach (string s in p5)
            {
                brokers.Add((BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), s + "BrokerCommunication"));
            }
        }

        public void send_subscription(string topic)
        {
            foreach (BrokerReceiveBroker b in brokers)
            {
                b.receiveSub(topic, myURL);
            }
        }

        public void send_unsubscription(string topic)
        {

            foreach (BrokerReceiveBroker b in brokers)
            {
                b.receiveUnsub(topic, myURL);
            }
            
        }
    }
}
