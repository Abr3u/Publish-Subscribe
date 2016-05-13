using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    public interface PubInterface {//usada pelo MPM

        void publish(string number,string topic, string secs,int filter,int order, int eventNumber,int logMode);
        void receivePubOK(string senderURL, int eventN);
        void status();
    }

    public interface LogInterface
    {//usada pelo MPM

        void scriptLog(string line);
        void log(string selfName, string pubName, string topicName, int eventNumber,string id);

    }

    public interface SubInterface {//usado pelo MPM

        void subscribe(string topic);
        void unsubscribe(string topic);
        void status();

    }

    public interface PuppetInterface {//usada pelo MPM
       // void createProcess(TreeNode t, string role, string n, string s, string u,string urlBroker);
        void createProcess(TreeNode t, string role, string n, string s, string u, List<string> urlReplica, List<string> urlPai, string urlPup);
    }

    public interface SubscriberNotify {//usada pelo Broker
        void notify(Message m, int eventNumber);
    }

    public interface BrokerReceivePub {//usada pelo Pub
        void receivePublication(Message m, string pubName);
    }

    public interface BrokerReceiveSubUnSub//usada pelo Sub
    {
        void receiveSub(string topic, string subName);
        void receiveUnsub(string topic, string subName);
    }

    public interface BrokerReceiveBroker//usada pelo Broker - forward&filter passou de broker para nome do broker
    {
        void iamleader(string url);
        string alive();
        void receiveOK(string senderURL);
        void updateStructsPub(string pubName, int seqN, List<Message> lstMsg, Dictionary<string, List<int>> lstMsgTO, Boolean t, int tCount, int espCount);
        void receiveOKPub(string pubName_seqN, string senderURL);
        string[] electLeader(string brokerName, string topic, bool isSub);
        string[] electLeaderPub(Message m, String brokerName, int eventN, int order, int logMode, Boolean filtering);
        void receiveDeadSub(string subName, string topic, string dead, Boolean sub);
        void receiveDeadPub(string pubName, Message m, string morto, int filter, int order, int eventNumber, int logMode);
        Boolean isRoot();
        void forwardFlood(Message m, string brokerName, int eventNumber, int order, int logMode);
        void forwardFilter(Message m, string brokerName, int eventNumber, int order, int logMode);
        void forwardSub(string topic, string brokerName);
        void forwardUnsub(string topic, string brokerName);
        string receiveSub(string topic, string subName);
        string receiveUnsub(string topic, string subName);
        void receivePublication(Message m, string pubName, string pubURL, int filter, int order, int eventNumber, int logMode);
        void status();
        void freeze();
        void unfreeze();
        void updateSequenceNumbers(List<string> lst, int TO);
        void brokerCrashedSub(string bCrash, string topic, bool isSub);
    }

    public interface IProcessCmd // chamada pelo MPM - executa cmd crash, freeze, unfreeze
    {
        void crash();
        void freeze();
        void unfreeze();
    }
}
