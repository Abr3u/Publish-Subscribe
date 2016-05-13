using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization.Formatters;
using System.Collections;

namespace PubSub
{
    public class Subscriber
    {
        
        private string url;
        private string name;
        private string site;
        private static string proj_path = Directory.GetParent(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName).FullName;

        public string URL
        {
            get { return url; }
            set { url = value; }
        }
        public string Name
        {
            get { return name; }
            set { name = value; }
        }
        public string Site
        {
            get { return site; }
            set { site = value; }
        }



        public Subscriber(string u, string n, string s/* Broker b*/)
        {
            URL = u;
            Name = n;
            Site = s;
        }

        static void Main(string[] args)
        {
            Console.WriteLine("@Subscriber !!! args -> {0}", args[0]);

            string[] arguments = args[0].Split(';');//arguments[0]->port; arguments[1]->url; arguments[2]->nome; arguments[3]->site; arguments[4]-> urlPup; arguments[5]->urlBroker; 

            //Console.WriteLine("URL do pupetmaster "+arguments[5]);

            List<string> urlBrokerList = new List<string>();

            for (int i=5 ; i< arguments.Length; i++)
            {
                urlBrokerList.Add(arguments[i]);
            }
            RemotingConfiguration.Configure(proj_path + @"\subscriber\App.config", false);

            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            provider.TypeFilterLevel = TypeFilterLevel.Full;
            IDictionary props = new Hashtable();
            props["port"] = Int32.Parse(arguments[0]);
            TcpChannel chan = new TcpChannel(props, null, provider);
            ChannelServices.RegisterChannel(chan, false);

            MPMSubImplementation subUnsub = new MPMSubImplementation(arguments[3],arguments[1],arguments[2],arguments[0],arguments[4],urlBrokerList);
            RemotingServices.Marshal(subUnsub, "MPMSubUnsub", typeof(MPMSubImplementation));

            SubNotify notify = new SubNotify(arguments[2],arguments[4]);
            RemotingServices.Marshal(notify, "Notify", typeof(SubNotify));

            MPMSubscriberCmd processCmd = new MPMSubscriberCmd();
            RemotingServices.Marshal(processCmd, "MPMProcessCmd", typeof(MPMSubscriberCmd));

            Console.ReadLine();
        }

    }

    class MPMSubImplementation : MarshalByRefObject, SubInterface
    {
        private string site;
        private string url;
        private string urlPup;
        //private string urlMyBroker;
        private List<string> urlMyBroker;
        private string nome;
        private string myPort;
        private List<string> subscriptions;
        private int okBrokers;
        private int liveBrokers;
        List<string> alive;
        private int TIMEOUT = 5000;


        // vida infinita !!!!
        public override object InitializeLifetimeService()
        {
            return null;
        }

        public delegate string SubUnsubRemoteAsyncDelegate(string t, string n);
        public void SubUnsubRemoteAsyncCallBack(IAsyncResult ar)
        {
            SubUnsubRemoteAsyncDelegate del = (SubUnsubRemoteAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            //Console.WriteLine("Sub Unsub: OK -> {0}", del.EndInvoke(ar));     

            string x = del.EndInvoke(ar);
            if (!x.Equals(""))
            {
                alive.Add(x);
                okBrokers++;
            }
            return;
        }

        public delegate void BrokerCrashedAsyncDel(string bCrash,string topic, bool isSub);
        public delegate void DeadAsyncDelegate(string subName, string topic, string dead,Boolean b);

        public void DeadAsyncDelegateCallback(IAsyncResult ar)
        {
            DeadAsyncDelegate del = (DeadAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            return;
        }

        public MPMSubImplementation(string p1, string p2, string p3,string p4,string p5, List<string> p6)
        {
            okBrokers = 0;
            this.site = p1;
            this.url = p2;
            this.nome = p3;
            this.myPort = p4;
            urlPup = p5;
            this.urlMyBroker = p6;
            liveBrokers = urlMyBroker.Count;
            subscriptions = new List<string>();
            alive = new List<string>();
           
            
        }
        
        public void subscribe(string topic)
        {
            string urlRemote = this.url.Substring(0, this.url.Length - 8);//retira XXXX/subscriber
            string myURL = urlRemote + myPort;

            Console.WriteLine("subscribing on topic {0} o meu url e {1}", topic, myURL);

            subscriptions.Add(topic);
            foreach (var broker in urlMyBroker)
            {
                
                BrokerReceiveBroker subunsub = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), broker + "BrokerCommunication");
                try
                {
                    SubUnsubRemoteAsyncDelegate remoteDel = new SubUnsubRemoteAsyncDelegate(subunsub.receiveSub);
                    AsyncCallback RemoteCallBAck = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                    IAsyncResult remAr = remoteDel.BeginInvoke( topic, myURL, RemoteCallBAck, null );
                }
                catch(SocketException)
                {
                    Console.WriteLine("Broker crashed "+broker);
                    urlMyBroker.Remove(broker);
                    liveBrokers--;

                    foreach(var b in urlMyBroker) {
                        BrokerReceiveBroker bcrash = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b + "BrokerCommunication");
                        BrokerCrashedAsyncDel bcDel = new BrokerCrashedAsyncDel(bcrash.brokerCrashedSub);
                        
                        IAsyncResult bcAr = bcDel.BeginInvoke(broker, topic, true, null, null);
                        bcAr.AsyncWaitHandle.WaitOne();
                        bcDel.EndInvoke(bcAr);
                    }
                }
            }
            
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Boolean ok = false;
            // enquanto não timeout
            while (sw.ElapsedMilliseconds < TIMEOUT)
            {
                //se ja recebi todos os callbacks
                if (okBrokers == liveBrokers)
                {
                    ok = true;
                    break;
                }
                
            }
            if (!ok) {
            
                Console.WriteLine("Deu timeout!!");

                List<string> deadBrokers;

                if (alive.Count != 0)
                {
                    deadBrokers = new List<string>(urlMyBroker.Except(alive));
                }
                else {
                    deadBrokers = new List<string>(urlMyBroker);
                }

                Console.WriteLine("Existem {0} mortos", deadBrokers.Count);

                //avisar os outros
                foreach (string url in alive)
                {
                    Console.WriteLine("Mandar para o " + url);

                    BrokerReceiveBroker broker = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), url + "BrokerCommunication");
                    
                    foreach (var d in deadBrokers)
                    {
                        Console.WriteLine("Morto a enviar " + d);

                        //envia para todos a dizer que ha um que nao responde
                        DeadAsyncDelegate deadDel = new DeadAsyncDelegate(broker.receiveDeadSub);
                        AsyncCallback deadCallBack = new AsyncCallback(DeadAsyncDelegateCallback);
                        IAsyncResult deadAr = deadDel.BeginInvoke(this.nome, topic, d,true, deadCallBack, null);
                    }
                }
            }
            sw.Stop();
            alive.Clear();
            okBrokers = 0;
        }

        public void unsubscribe(string topic)
        {
            string urlRemote = url.Substring(0, url.Length - 8);//retirar XXXX/subscriber
            string myURL = urlRemote + myPort;

            subscriptions.Remove(topic);

            Console.WriteLine("unsubscribing on topic {0} o meu url e {1}", topic, myURL);

            foreach (var broker in urlMyBroker)
            {
                BrokerReceiveBroker subunsub = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), broker + "BrokerCommunication");
                try
                {
                    SubUnsubRemoteAsyncDelegate remoteDel = new SubUnsubRemoteAsyncDelegate(subunsub.receiveUnsub);
                    AsyncCallback RemoteCallBack = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                    IAsyncResult remAr = remoteDel.BeginInvoke(topic, myURL, RemoteCallBack, null);
                }
                catch(SocketException)
                {
                    Console.WriteLine("Broker crashed "+broker);
                    urlMyBroker.Remove(broker);
                    liveBrokers--;

                    foreach (var b in urlMyBroker)
                    {
                        BrokerReceiveBroker bcrash = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b + "BrokerCommunication");
                        BrokerCrashedAsyncDel bcDel = new BrokerCrashedAsyncDel(bcrash.brokerCrashedSub);
                        IAsyncResult bcAr = bcDel.BeginInvoke(broker, topic, true, null, null);
                        bcAr.AsyncWaitHandle.WaitOne();
                        bcDel.EndInvoke(bcAr);
                    }
                }
            }

            Stopwatch sw = new Stopwatch();
            sw.Start();

            Boolean ok = false;
            // enquanto não timeout
            while (sw.ElapsedMilliseconds < TIMEOUT)
            {
                //se ja recebi todos os callbacks
                if (okBrokers == liveBrokers)
                {
                    ok = true;
                    break;
                }

            }
            if (!ok)
            {

                Console.WriteLine("Deu timeout!!");

                List<string> deadBrokers;

                if (alive.Count != 0)
                {
                    deadBrokers = new List<string>(urlMyBroker.Except(alive));
                }
                else
                {
                    deadBrokers = new List<string>(urlMyBroker);
                }

                Console.WriteLine("Existem {0} mortos", deadBrokers.Count);

                //avisar os outros
                foreach (string urlB in alive)
                {
                    Console.WriteLine("Mandar para o " + url);

                    BrokerReceiveBroker broker = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlB + "BrokerCommunication");

                    foreach (var d in deadBrokers)
                    {
                        Console.WriteLine("Morto a enviar " + d);

                        //envia para todos a dizer que ha um que nao responde
                        DeadAsyncDelegate deadDel = new DeadAsyncDelegate(broker.receiveDeadSub);
                        AsyncCallback deadCallBack = new AsyncCallback(DeadAsyncDelegateCallback);
                        IAsyncResult deadAr = deadDel.BeginInvoke(this.nome, topic, d,false, deadCallBack, null);
                    }
                }
            }
            sw.Stop();
            alive.Clear();
            okBrokers = 0;

        }

        public void status()
        {
            Console.WriteLine("I'm subscriber {0} at url {1} and port {2} and I'm interested in the following topics", nome, url, myPort);

            foreach (string s in subscriptions)
            {
                Console.WriteLine("\t" + s);
            }
        }


    }

    class SubNotify : MarshalByRefObject, SubscriberNotify {

        private string name;
        private string urlPup;

        public SubNotify(string n, string pup) {
            this.name = n;
            this.urlPup = pup;
        }

        public void notify(Message m, int eventNumber)
        {
            
            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
            log.log(this.name, m.author, m.Topic, eventNumber, "subscriber");
            Console.WriteLine("@SubNotify received a notification on topic {0} ----> TO {1}, seqNum {2}", m.Topic, m.TotalNumber,m.SeqNum);
        }
    }

    public class MPMSubscriberCmd : MarshalByRefObject, IProcessCmd
    {

        public void crash()
        {
            // sledgehammer solution -> o mesmo que unplug
            Environment.Exit(1);
        }

        public void freeze()
        {
            throw new NotImplementedException();
        }

        public void unfreeze()
        {
            throw new NotImplementedException();
        }
    }
}

