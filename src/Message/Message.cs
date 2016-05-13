using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    [Serializable]
    public class Message
    {
        private string topic;
        private string content;
        private int seqNum;
        private int totalNumber;

        public string Topic
        {
            get { return topic; }
            set { topic = value; }
        }

        public string Content
        {
            get { return content; }
            set { content = value; }
        }
        public int SeqNum
        {
            get { return seqNum; }
            set { seqNum = value; }
        }
        public int TotalNumber
        {
            get { return totalNumber; }
            set { totalNumber = value; }
        }
        public string author { get; private set; }

        public Message(string t, string c, int sN, string au)
        {
            totalNumber = 0;
            topic = t;
            content = c;
            seqNum = sN;
            author = au;
        }

    }
}
