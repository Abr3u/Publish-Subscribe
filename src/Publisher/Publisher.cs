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
using System.Collections;
using System.Runtime.Serialization.Formatters;

namespace PubSub
{
    public class Publisher
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

        public Publisher(string u, string n, string s)
        {
            URL = u;
            Name = n;
            Site = s;
        }

        static void Main(string[] args)
        {
            RemotingConfiguration.Configure(proj_path + @"\Publisher\App.config", false);
            Console.WriteLine("@publisher !!! porto -> {0}", args[0]);



            string[] arguments = args[0].Split(';');//arguments[0]->port; arguments[1]->url; arguments[2]->nome; arguments[3]->site;arguments[4] -> urlPup; arguments[5]->urlBroker; 
            List<string> urlBrokerList = new List<string>();

            //Console.WriteLine("URL do pupetmaster " + arguments[5]);

            for (int i = 5; i < arguments.Length; i++)
            {
                urlBrokerList.Add(arguments[i]);
            }
            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            provider.TypeFilterLevel = TypeFilterLevel.Full;
            IDictionary props = new Hashtable();
            props["port"] = Int32.Parse(arguments[0]);
            TcpChannel channel = new TcpChannel(props, null, provider);
            ChannelServices.RegisterChannel(channel, false);

            MPMPubImplementation MPMpublish = new MPMPubImplementation(arguments[0], arguments[1], arguments[3], arguments[2], arguments[4], urlBrokerList);
            
            RemotingServices.Marshal(MPMpublish, "PMPublish", typeof(MPMPubImplementation));

            MPMPublisherCmd processCmd = new MPMPublisherCmd();
            RemotingServices.Marshal(processCmd, "MPMProcessCmd", typeof(MPMPublisherCmd));

            Console.ReadLine();
        }

    }
    class MPMPubImplementation : MarshalByRefObject, PubInterface
    {
        private static object myLock = new Object();
        private string myPort;
        private string urlPup;
        private string url;
        private string site;
        private string name;
        //private string urlMyBroker;
        private List<string> urlMyBroker;
        private int count;
        private Dictionary<string, int> topic_number;//#seq por topico
        
        private int liveBrokers;
        Dictionary<string,List<int>> broker_callback;
        Dictionary<string, bool> broker_alive; 

        private int TIMEOUT = 5000;

        // vida infinita !!!!
        public override object InitializeLifetimeService()
        {
            return null;
        }
        
        
        public void PubRemoteAsyncCallBack(IAsyncResult ar)
        {
            PubRemoteAsyncDelegate del = (PubRemoteAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public delegate void BrokerCrashedAsyncDel(string bCrash, string topic, bool isSub);
        public delegate void DeadAsyncDelegate(string pubName, Message m, string morto, int filter, int order, int eventNumber, int logMode);
        public delegate string AliveDelegate();
        public delegate void ReceiveSub();
        public delegate void PubRemoteAsyncDelegate(Message m, string pubName, string pubURL, int filter, int order, int eventNumber, int logMode);

        public void DeadAsyncDelegateCallback(IAsyncResult ar)
        {
            DeadAsyncDelegate del = (DeadAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public void AliveDelegateCallback(IAsyncResult ar)
        {
            AliveDelegate del = (AliveDelegate)((AsyncResult)ar).AsyncDelegate;

            string f = del.EndInvoke(ar);

            Console.WriteLine("Recebi ALIVE do {0}", f);

            broker_alive[f] = true;

            return;
        }

        public MPMPubImplementation(string p1, string p2, string p3, string p4, string p5, List<string> p6)
        {
            this.myPort = p1;
            this.url = p2;
            this.site = p3;
            this.name = p4;
            this.count = 1;
            this.urlPup = p5;
            this.urlMyBroker=p6;
            topic_number = new Dictionary<string, int>();
            liveBrokers = urlMyBroker.Count;
            broker_alive = new Dictionary<string, bool>();

            broker_callback = new Dictionary<string, List<int>>();

            foreach (var b in urlMyBroker) {

                broker_callback.Add(b, new List<int>());
                broker_alive.Add(b, false);
            }

        }

        public void receivePubOK(string senderURL, int eventN)
        {
            //Console.WriteLine("Recebi ok do {0} -> {1}", senderURL, eventN);
            broker_callback[senderURL.Substring(0,senderURL.Length-6)].Add(eventN);

        }

        public void publish(string number, string topic, string secs, int filter, int order, int eventNumber, int logMode)
        {
            string urlRemote = url.Substring(0, url.Length - 8);//retirar XXXX/publisher
            string myURL = urlRemote + myPort;

            Console.WriteLine("@MPMPubImplementatio - {0} publishing events, on topic {1}", myURL, topic);

            if (!topic_number.ContainsKey(topic))
            {
                topic_number.Add(topic, Int32.Parse(number));
            }
            else
            {
                topic_number[topic] = topic_number[topic] + Int32.Parse(number);
            }


            for (int i = 0; i < Int32.Parse(number); i++)
            {
                Console.WriteLine("Publicar {0} seqNumber {1} modo {2}", topic, count, filter);
                Message maux = new Message(topic, i.ToString(), count, name);
                count++;

                LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                log.log(this.name, this.name, topic, eventNumber, "publisher");

                System.Threading.Thread.Sleep(Int32.Parse(secs));

                foreach (var broker in urlMyBroker)
                {
                    BrokerReceiveBroker pub = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), broker + "BrokerCommunication");
                    try
                    {
                        PubRemoteAsyncDelegate remoteDel = new PubRemoteAsyncDelegate(pub.receivePublication);
                        AsyncCallback RemoteCallBack = new AsyncCallback(PubRemoteAsyncCallBack);
                        IAsyncResult remAr = remoteDel.BeginInvoke(maux, this.name, this.url.Substring(0,this.url.Length-3), filter, order, eventNumber, logMode, RemoteCallBack, null);

                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
                eventNumber++;
            }
            
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Dictionary<string, List<int>> dummy = new Dictionary<string, List<int>>(broker_callback);

            
            // enquanto não timeout
            while (sw.ElapsedMilliseconds < TIMEOUT)
            {
                foreach (var j in broker_callback)
                {
                    //se ja recebi todos os callbacks de determinado broker
                    if (j.Value.Count == count - 1)
                    {
                        if (dummy.ContainsKey(j.Key))
                        {
                            //Console.WriteLine("Recebi todos os CB de " + j.Key);
                            dummy.Remove(j.Key);
                        }
                    }
                }
            }

            if (dummy.Count != 0)
            {
                

                Console.WriteLine("Deu timeout!!");
                BrokerReceiveBroker b;

                sw.Stop();
                
                foreach (var a in dummy)
                {

                    Console.WriteLine("Enviar ALIVE para {0}", a.Key);
                    b = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), a.Key + "BrokerCommunication");

                    AliveDelegate aliveDel = new AliveDelegate(b.alive);
                    AsyncCallback aliveCallBack = new AsyncCallback(AliveDelegateCallback);

                    IAsyncResult deadAr = aliveDel.BeginInvoke(aliveCallBack, null);
                }

                sw = new Stopwatch();
                sw.Start();

                List<string> responses = new List<string>();

                while (sw.ElapsedMilliseconds < 2 * TIMEOUT)
                {
                    foreach (var z in broker_alive.ToArray()) {
                        //se ja tiver recebido alive daquele broker
                        if (z.Value) {
                            if (!responses.Contains(z.Key))
                            {
                                //Console.WriteLine("Adicionei {0} as respostas", z.Key);
                                responses.Add(z.Key);
                            }
                        }
                    }
                }
                sw.Stop();
                if (responses.Count != liveBrokers)
                {
                    List<string> deadBrokers;
                    if (responses.Count != 0)
                    {
                        deadBrokers = new List<string>(urlMyBroker.Except(responses));
                    }
                    else
                    {
                        deadBrokers = new List<string>(urlMyBroker);
                    }
                    //Console.WriteLine("Houve quem nao respondesse ao Alive, vou considera-lo/s morto/s");

                    foreach (var r in deadBrokers) {
                        //Console.WriteLine("Este mano esta morto -> " + r);
                    }

                    urlMyBroker = new List<string>(responses);
                    liveBrokers = urlMyBroker.Count;

                    
                    //numseq que recebi de cada broker
                    foreach (var e in dummy) {
                        foreach(var morto in deadBrokers)
                        {
                            if (e.Key.Equals(morto))
                            {
                                for(int i = 1; i< count; i++)
                                {
                                    //nao recebi i-esima callback deste broker
                                    if (!e.Value.Contains(i))
                                    {
                                        foreach (var destino in urlMyBroker) {
                                            //Console.WriteLine("Nao tinha recebido a callback #{0}, enviar para {1}", i, destino);

                                            b = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), destino + "BrokerCommunication");

                                            DeadAsyncDelegate deadDel = new DeadAsyncDelegate(b.receiveDeadPub);
                                            AsyncCallback deadCallBack = new AsyncCallback(DeadAsyncDelegateCallback);

                                            Message rep = new Message(topic, i.ToString(), i, this.name);
                                            IAsyncResult deadAr = deadDel.BeginInvoke(this.name, rep,morto, filter, order, eventNumber, logMode, deadCallBack, null);
                                            eventNumber++;
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
                else
                {
                    //Console.WriteLine("Recebi todos os ALIVE");
                }
            }
            else
            {
                Console.WriteLine("Nao deu timeout");
            }
        }

        public void status()
        {
            Console.Write("I'm publisher {0} at url {1} and port {2} and I've published:\n", name, url, myPort);
            foreach (KeyValuePair<string, int> t in topic_number)
            {
                Console.WriteLine("\t" + t.Value + " event(s) on topic " + t.Key + "\r\n");
            }
        }
    }

    public class MPMPublisherCmd : MarshalByRefObject, IProcessCmd
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
