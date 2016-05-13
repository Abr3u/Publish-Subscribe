using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{

    public class Broker
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

        public Broker(string u, string n, string s)
        {
            URL = u;
            Name = n;
            Site = s;
        }

        static void Main(string[] args)
        {
            // Console.WriteLine("@broker !!! porto -> {0}", args[0]);
            RemotingConfiguration.Configure(proj_path + @"\Broker\App.config", false);

            string[] arguments = args[0].Split(';');//arguments[0]->port; arguments[1]->url; arguments[2]->nome; arguments[3]->site; arguments[4]->replicas; arguments[5]->urlPai; arguments[6]-> urlMPM

            //Console.WriteLine("URL do pupetmaster " + arguments[4]);

            int vizinhos = arguments.Length - 8;//numero de vizinhos
            List<Broker> lstaux = new List<Broker>();
            //iniciar lista de vizinhos
            for (int i = 7; i < vizinhos + 7; i++)
            {
                string[] atr = arguments[i].Split('%');//atr[0]-name, atr[1]-site, atr[2]-url
                Broker b = new Broker(atr[2], atr[0], atr[1]);
                lstaux.Add(b);
            }

            string aux = arguments[4].Substring(0, arguments[4].Length - 1); // retirar # do fim da string "replica#replica#"
            string[] rep = aux.Split('#');
            List<string> replicas = new List<string>();
            //iniciar lista de replicas
            foreach (var r in rep)
            {
                replicas.Add(r);
            }


            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            provider.TypeFilterLevel = TypeFilterLevel.Full;
            IDictionary props = new Hashtable();
            props["port"] = Int32.Parse(arguments[0]);
            TcpChannel chan = new TcpChannel(props, null, provider);
            ChannelServices.RegisterChannel(chan, false);

            BrokerCommunication brokerbroker = new BrokerCommunication(lstaux, arguments[2], replicas, arguments[0], arguments[5], arguments[6], arguments[1], arguments[3]);
            RemotingServices.Marshal(brokerbroker, "BrokerCommunication", typeof(BrokerCommunication));

            MPMBrokerCmd processCmd = new MPMBrokerCmd();
            RemotingServices.Marshal(processCmd, "MPMProcessCmd", typeof(MPMBrokerCmd));


            Console.ReadLine();
        }
    }

    //IMPLEMENTATIONS

    public class BrokerCommunication : MarshalByRefObject, BrokerReceiveBroker
    {
        // vida infinita !!!!
        public override object InitializeLifetimeService()
        {
            return null;
        }

        public delegate void FilterFloodRemoteAsyncDelegate(Message m, string t, int eventNumber, int order, int logMode);
        public delegate void SubUnsubRemoteAsyncDelegate(string t, string n);
        public delegate void LeaderDelegate(string t);
        public delegate void SendOKAsyncDelegate(string brokerURL);
        public delegate string[] ElectLeaderAsyncDelegate(string brokerName, string topic, bool isSub);
        public delegate void NotifyPubAsyncDelegate(string pubName, int seqN, List<Message> lstMsg, Dictionary<string, List<int>> lstMsgTO, Boolean t, int tCount, int espCount);
        public delegate void ReceivePubOKAsyncDelegate(string pubName_seqN, string senderSite);
        public delegate string[] ElectLeaderPubAsyncDelegate(Message m, string brokerName, int eventN, int order, int logMode, Boolean filtering);
        public delegate void SendPubOK(string senderURL, int eventN);

        private static object myLock = new Object();
        private static object myLockOK = new Object();
        private string name;
        private List<Broker> lstVizinhos;
        private Dictionary<string, List<string>> lstSubsTopic; //quem subscreveu neste no, a quE
        private Dictionary<Broker, List<string>> routingTable; //vizinho,subscrições atingiveis atraves desse vizinho
        private Dictionary<string, int> pubCount; //publisher%topico->numSeq
        private Dictionary<string, int> seqPub; //publisher->numSeq
        private Dictionary<string, List<Broker>> site_brokers;
        private List<Message> lstMessage;
        private List<string> lstReplicas;
        private bool isLeader;
        private int myPort;
        private string urlLider;
        private Dictionary<string, Broker> site_lider; //lider para cada site
        private List<string> sitesOK; //para cada evento que envio guardo os oks de resposta para cada site (so envio se for lider e so recebo de liders)
        private int TIMEOUT = 2000;
        private Dictionary<string, List<string>> receivePubOK;

        private Dictionary<string, List<int>> pubMsgsNum;
        private Boolean envieiRoot = false;
        private int esperadoCount = 1;
        private int totalCount = 1;
        private string urlPai;
        private string myURL;
        private string mySite;
        private Boolean root;
        private Boolean frozen;
        private List<Evento> frozenEvents;
        private int liderCount;
        private string urlPup;

        public BrokerCommunication(List<Broker> lst, string n, List<string> replicas, string port, string up, string pup, string myurl, string mysite)
        {

            if (n.Contains("?root"))
            {
                root = true;
                name = n.Split('?')[0];
            }
            else
            {
                name = n;
                root = false;
            }
            myURL = myurl;
            frozen = false;
            urlPai = up;
            urlPup = pup;

            pubMsgsNum = new Dictionary<string, List<int>>();

            lstSubsTopic = new Dictionary<string, List<string>>();
            routingTable = new Dictionary<Broker, List<string>>();
            lstVizinhos = lst;
            pubCount = new Dictionary<string, int>();
            seqPub = new Dictionary<string, int>();
            lstMessage = new List<Message>();
            lstReplicas = replicas;
            myPort = Int32.Parse(port);
            mySite = mysite;
            tryLeader(lstReplicas);
            liderCount = 0;
            site_brokers = new Dictionary<string, List<Broker>>();
            site_lider = new Dictionary<string, Broker>();
            sitesOK = new List<string>();
            frozenEvents = new List<Evento>();
            receivePubOK = new Dictionary<string, List<string>>();
            
            Console.WriteLine("@broker !!! porto -> {0}, isLeader {1}", port, isLeader.ToString());

            foreach (var z in lstVizinhos)
            {
                if (site_brokers.ContainsKey(z.Site))
                {
                    site_brokers[z.Site].Add(z);
                }
                else {
                    site_brokers.Add(z.Site, new List<Broker>());
                    site_brokers[z.Site].Add(z);
                }
            }

            foreach (var site in site_brokers.Keys)
            {
                site_lider.Add(site, viz_lider(site));
                //Console.WriteLine("Lider do site {0}: {1}", site, site_lider[site]);
            }
        }
        public static void LeaderCallBack(IAsyncResult ar)
        {
            LeaderDelegate del = (LeaderDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        // This is the call that the AsyncCallBack delegate will reference.
        public static void FilterFloodRemoteAsyncCallBack(IAsyncResult ar)
        {
            FilterFloodRemoteAsyncDelegate del = (FilterFloodRemoteAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public static void SubUnsubRemoteAsyncCallBack(IAsyncResult ar)
        {
            SubUnsubRemoteAsyncDelegate del = (SubUnsubRemoteAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }


        public static void SendOKAsyncCallBack(IAsyncResult ar)
        {
            SendOKAsyncDelegate del = (SendOKAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public void ElectLeaderAsyncCallBack(IAsyncResult ar)
        {
            ElectLeaderAsyncDelegate del = (ElectLeaderAsyncDelegate)((AsyncResult)ar).AsyncDelegate;

            string[] site_newLeader = new String[] { del.EndInvoke(ar)[0], del.EndInvoke(ar)[1] };

            Broker aux = null;
            foreach(var i in site_brokers[site_newLeader[0]]) {
                if(i.URL.Equals(site_newLeader[1]))
                    aux = i;
            }

            if(aux != null) {
                site_lider[site_newLeader[0]] = aux;
                Console.WriteLine("O novo lider do site {0} eh {1} ",site_newLeader[0],aux.URL);
            }

            return;
        }

        public void ElectLeaderPubAsyncCallBack(IAsyncResult ar)
        {
            ElectLeaderPubAsyncDelegate del = (ElectLeaderPubAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            
            string[] result = new string[] {del.EndInvoke(ar)[0],del.EndInvoke(ar)[1]};

            Broker aux = null;
            foreach (var i in site_brokers[result[0]])
            {
                if (i.URL.Equals(result[1]))
                    aux = i;
            }

            site_lider[result[0]] = aux;

            Console.WriteLine("Actualizei o meu lider no site {0} para {1}",result[0],site_lider[result[0]]);

            return;

        }

        public static void NotifyPubAsyncCallBack(IAsyncResult ar)
        {
            NotifyPubAsyncDelegate del = (NotifyPubAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public static void ReceivePubOKAsyncCallBack(IAsyncResult ar)
        {
            ReceivePubOKAsyncDelegate del = (ReceivePubOKAsyncDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public void SendPubOKAsyncCallBack(IAsyncResult ar)
        {
            SendPubOK del = (SendPubOK)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
            return;
        }

        public Boolean isRoot()
        {
            return root;
        }

        public Broker viz_lider(string site)
        {
            List<int> lstPort = new List<int>();

            foreach (var broker in site_brokers[site])
            {
                string[] a = broker.URL.Split(':');
                string[] b = a[2].Split('/');
                lstPort.Add(int.Parse(b[0]));
            }

            int min = lstPort.Min();

            foreach (var broker in site_brokers[site])
                if (broker.URL.Contains(min.ToString()))
                {
                    Console.WriteLine("o lider do site {0} eh {1}", site, broker.URL);
                    return broker;
                }

            Console.WriteLine("Eleicao de lider para o site {0} falhou!", site);
            return null;

        }

        private void tryLeader(List<string> l)
        {
            List<int> lstPort = new List<int>();
            lstPort.Add(myPort);
            foreach (var url in l)
            {
                string[] a = url.Split(':');
                string b = a[2].Trim('/');
                lstPort.Add(int.Parse(b));
            }
            string aux = myURL.Substring(0, myURL.Length-11);
            urlLider = aux + lstPort.Min().ToString() + "/";
            Console.WriteLine("O grande lider: {0}", urlLider);
            isLeader = lstPort.Min() == myPort;
            
        }



        public void brokerCrashedSub(string bCrash, string topic, bool isSub)
        {

            List<string> aux = new List<string>();
            foreach (var rep in lstReplicas)
            {
                if (!rep.Equals(urlLider))
                {
                    aux.Add(rep);
                }
            }
            
            if (!isLeader)
            {
                tryLeader(aux);

                if (isLeader)
                {
                    if (isSub)
                    {
                        forwardSub(topic, name);
                    }
                    else
                    {
                        forwardUnsub(topic, name);
                    }
                }
            }
        }

        public List<Message> SortList(List<Message> l)
        {
            int length = l.Count;

            Message temp = l[0];

            for (int i = 0; i < length; i++)
            {
                for (int j = i + 1; j < length; j++)
                {
                    if (l[i].SeqNum > l[j].SeqNum)
                    {
                        temp = l[i];

                        l[i] = l[j];

                        l[j] = temp;
                    }
                }
            }

            return l;
        }

        public void forwardFlood(Message m, string brokerName, int eventNumber, int order, int logMode) {
            Console.WriteLine("Inicio flood chamado por " + brokerName);
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsForwarding(m, brokerName, eventNumber, order, logMode, false);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
            }
            else
            {
                if (isLeader)
                {
                    if (!receivePubOK.ContainsKey(m.author + "|" + eventNumber.ToString()))
                    {
                        receivePubOK.Add(m.author + "|" + eventNumber.ToString(), new List<string>());
                    }
                    string senderURL = "";

                    foreach (var b in lstVizinhos)
                    {
                        if (b.Name.Equals(brokerName))
                        {
                            senderURL = b.URL.Substring(0, b.URL.Length - 6);
                            break;
                        }
                    }

                    //Console.WriteLine("O meu sender foi {0}", senderURL);

                    //vou avisar o remetente que recebi a sub se nao tiver sido eu proprio
                    if (!brokerName.Equals(name))
                    {

                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), senderURL + "BrokerCommunication");
                        try
                        {
                            
                            ReceivePubOKAsyncDelegate receiveOKPubDel = new ReceivePubOKAsyncDelegate(bro.receiveOKPub);
                            AsyncCallback receivePubOKPubCallBack = new AsyncCallback(ReceivePubOKAsyncCallBack);
                            IAsyncResult receiveOKAr = receiveOKPubDel.BeginInvoke(m.author + "|" + eventNumber.ToString(), mySite, receivePubOKPubCallBack, null);
                            Console.WriteLine("Enviei o ok ao sender broker "+senderURL);
                            
                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }

                    }
                }
                

                if (order == 2)//TOTAL
                {
                    if (!root)
                    {
                        lock (myLock)
                        {
                            if (m.TotalNumber != 0)//ja foi a root
                            {

                                if (esperadoCount == m.TotalNumber)
                                { //msg que estava a espera

                                    Console.WriteLine("Msg ja foi a root e a que estava a espera ------ TN #" + m.TotalNumber);

                                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                    {
                                        if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                        {
                                            Console.WriteLine("Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.TotalNumber);
                                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                            not.notify(m, eventNumber);
                                        }
                                    }
                                    esperadoCount++;
                                }
                                else
                                {
                                    Console.WriteLine("Mensagem ja foi a root mas nao era a que eu queria TN #{0}", m.TotalNumber);

                                    lstMessage.Add(m);
                                    lstMessage = SortList(lstMessage);
                                }

                                //iterar lista de espera
                                for (int j = 0; j < lstMessage.Count;)
                                {
                                    int next = esperadoCount;// e o proximo numSeq que estou a espera

                                    if (lstMessage[j].TotalNumber == next)
                                    {//msg na lista de espera e a que estava a espera
                                        foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                        {
                                            if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                            {
                                                Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, TN da msg ---> {0}", lstMessage[j].TotalNumber);
                                                SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                                not.notify(lstMessage[j], eventNumber);
                                            }
                                        }

                                        lstMessage.Remove(lstMessage[j]);

                                        j = 0; // voltar ao início da lista
                                        esperadoCount++;
                                    }
                                    else
                                    {
                                        j++; //continuar a iterar }
                                    }
                                }
                            }
                        }
                    }

                    else
                    { //root
                        lock (myLock)
                        {
                            if (m.TotalNumber == 0)
                            {
                                if (!pubMsgsNum.ContainsKey(m.author))
                                {//nao numerei nada deste autor
                                    pubMsgsNum.Add(m.author, new List<int>());

                                    m.TotalNumber = totalCount;
                                    pubMsgsNum[m.author].Add(m.SeqNum);
                                    Console.WriteLine("Numerei a msg {0} do topico {1} com #{2} -- recebi esta msg do {3}", m.SeqNum, m.Topic, totalCount, brokerName);

                                    totalCount++;
                                }
                                else
                                {
                                    foreach (var p in pubMsgsNum)
                                    {
                                        if (p.Key.Equals(m.author))
                                        {
                                            if (!p.Value.Contains(m.SeqNum))//nao numerei esta msg deste autor
                                            {

                                                m.TotalNumber = totalCount;
                                                pubMsgsNum[m.author].Add(m.SeqNum);
                                                Console.WriteLine("Numerei a msg {0} do topico {1} com #{2} -- recebi esta msg do {3}", m.SeqNum, m.Topic, totalCount, brokerName);

                                                totalCount++;

                                            }
                                        }
                                    }
                                }
                            }

                            foreach (var viz in lstVizinhos)
                            {
                                //propagar para o broker que me mandou esta msg pois na fase de propagacao nao mando para ele

                                if (viz.Name.Equals(brokerName))
                                {

                                    string urlRemote = viz.URL.Substring(0, viz.URL.Length - 6);//retirar broker

                                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");
                                    try
                                    {
                                        if (logMode == 1)
                                        {
                                            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                                            log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                                        }
                                        FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFlood);
                                        AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                        IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);
                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                            }


                            if (m.TotalNumber != 0 && m.TotalNumber == esperadoCount)
                            {
                                foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                {
                                    if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                    {
                                        Console.WriteLine("Eu ROOT notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.TotalNumber);
                                        SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                        not.notify(m, eventNumber);
                                    }
                                }
                                esperadoCount++;
                            }
                            else
                            {
                                Console.WriteLine("Mensagem ja foi a root mas nao era a que eu queria TN #{0}", m.TotalNumber);

                                lstMessage.Add(m);
                                lstMessage = SortList(lstMessage);
                            }

                            //iterar lista de espera
                            for (int j = 0; j < lstMessage.Count; )
                            {
                                int next = esperadoCount;// e o proximo numSeq que estou a espera

                                if (lstMessage[j].TotalNumber == next)
                                {//msg na lista de espera e a que estava a espera
                                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                    {
                                        if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                        {
                                            Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, TN da msg ---> {0}", lstMessage[j].TotalNumber);
                                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                            not.notify(lstMessage[j], eventNumber);
                                        }
                                    }

                                    lstMessage.Remove(lstMessage[j]);

                                    j = 0; // voltar ao início da lista
                                    esperadoCount++;
                                }
                                else
                                {
                                    j++; //continuar a iterar }
                                }
                            }
                        }
                    }

                }
                string pubTopic = m.author + "%" + m.Topic;
                if (order == 1)//FIFO
                {

                    lock (myLock)
                    {
                        if (!seqPub.ContainsKey(m.author))
                        {//publisher nao publicou nada
                            seqPub.Add(m.author, 1);//actualizar numSeq deste pub

                        }

                        if (!pubCount.ContainsKey(pubTopic))//publisher nao publicou nada neste topico
                        {
                            pubCount.Add(pubTopic, 1);
                        }

                        if (m.SeqNum == seqPub[m.author])//msg esperada para aquele publisher
                        {
                            foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                            {
                                if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                {
                                    Console.WriteLine("Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.SeqNum);
                                    SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                    not.notify(m, eventNumber);
                                }
                            }
                            //ja notifiquei quem tinha a notificar com esta publicacao
                            seqPub[m.author]++;
                            pubCount[pubTopic]++;
                        }
                        else
                        { //nao e a msg esperada

                            Console.WriteLine("{0} - Esperado {1} ----- {2} Recebido", pubTopic, seqPub[m.author], m.SeqNum);
                            lstMessage.Add(m);
                            lstMessage = SortList(lstMessage);
                        }



                        //iterar sobre lista de espera para ver se posso mandar alguma coisa
                        for (int j = 0; j < lstMessage.Count;)
                        {
                            int next = seqPub[m.author];// e o proximo numSeq que estou a espera para aquele autor

                            Console.WriteLine("Estou a iterar a procura do {0} para <{1}>", next, pubTopic);
                            foreach (Message mi in lstMessage) Console.Write("a: " + mi.author + ",n: " + mi.SeqNum + "#");
                            Console.WriteLine("");
                            if (lstMessage[j].author.Equals(m.author) && lstMessage[j].SeqNum == next)
                            {//msg na lista de espera e do mesmo autor, e tambem e a que estava a espera
                                foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                {
                                    if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                    {
                                        Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", lstMessage[j].SeqNum);
                                        SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                        not.notify(lstMessage[j], eventNumber);
                                    }
                                }
                                Console.Write("antes de descartar mensagem : " + lstMessage[j].SeqNum + " #");
                                lstMessage.Remove(lstMessage[j]);

                                j = 0; // voltar ao início da lista
                                seqPub[m.author]++;
                                pubCount[pubTopic]++;
                                Console.WriteLine("seq actual: " + seqPub[m.author]);
                            }
                            else
                            {
                                j++; //continuar a iterar }
                            }
                        }
                    }
                }
                if (order == 0)//NO
                {
                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                    {
                        if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                        {
                            Console.WriteLine("Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.SeqNum);
                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                            not.notify(m, eventNumber);
                        }
                    }
                }


                //PROPAGATION TIME
                List<Broker> lst = new List<Broker>(lstVizinhos);

                
                    //eliminar remetente da lista
                    for (int i = 0; i < lst.Count; i++)
                    {
                        if (lst[i].Name.Equals(brokerName))
                        {
                            //Console.WriteLine("Removi o {0} da lista de vizinhos porque era o remetente", lst[i].Name);
                            lst.Remove(lst[i]);
                        }
                    }
                

                List<Broker> leaders = new List<Broker>(lst);
                //selecionar restantes lideres
                for (int i = 0; i < lst.Count; i++)
                {

                    if (!lst[i].URL.Equals(site_lider[lst[i].Site].URL))
                    {
                        //Console.WriteLine("Removi {0} da lista anterior pois nao era lider", lst[i].URL);
                        leaders.Remove(lst[i]);
                    }
                }

                //propagar para os outros lideres
                foreach (var viz in leaders)
                {
                    string urlRemote = viz.URL.Substring(0, viz.URL.Length - 6);//retirar broker

                    Console.WriteLine("Flooding MSG <<<<{0}>>>>  #seq {1} para o vizinho em {2}", pubTopic, m.SeqNum, urlRemote);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");

                    try
                    {
                        if (logMode == 1)
                        {
                            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                            log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                        }
                        FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFlood);
                        AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);
                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }

                //se enviou a alguem
                if (leaders.Count != 0)
                {
                    //espera respostas dos lideres de outros sites
                    bool allReceived = false;
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    string aux = m.author + "|" + eventNumber.ToString();

                    while (sw.ElapsedMilliseconds < TIMEOUT)
                    {
                        if (receivePubOK.Count != 0)
                        {
                            
                            if (receivePubOK[aux].Count >= leaders.Count)
                            {
                                allReceived = true;
                                Console.WriteLine("Todos os sites estao OK");
                                break;
                            }
                        }
                    }

                    sw.Stop();

                    if (!allReceived)
                    {
                        Console.WriteLine("Houve sites que nao responderam!");
                        
                        List<string> lstAux = new List<string>();

                        foreach (var a in leaders)
                        {
                            lstAux.Add(a.Site);
                        }
                        
                        Console.Write("aux ::::::::::: {0} ->>>> {1}",aux, receivePubOK.ContainsKey(aux));

                        if (receivePubOK.ContainsKey(aux))
                        {
                            List<string> sites_down = new List<string>(lstAux.Except(receivePubOK[aux]));
                            Console.WriteLine("THERE ARE {0} SITES DOWN", sites_down.Count);



                            //para cada um que falhou notificar as replicas desse site
                            foreach (var s in sites_down)
                            {

                                Console.WriteLine("Este lider {0} nao respondeu.", s);
                                foreach (var b in site_brokers[s])
                                {

                                    if (!b.URL.Equals(site_lider[s].URL))
                                    {

                                        //Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                        try
                                        {
                                            ElectLeaderPubAsyncDelegate electLeaderDel = new ElectLeaderPubAsyncDelegate(bro.electLeaderPub);
                                            AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderPubAsyncCallBack);
                                            IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(m, brokerName, eventNumber,order,logMode,false, electLeaderCallBack, null);
                                        }
                                        catch (SocketException)
                                        {
                                            Console.WriteLine("Could not locate server");
                                        }
                                    }

                                }

                            }
                        }

                    }
                    //faz reset ah estrutura dinamica para ser utilizado num evento futuro
                    //receivePubOK[aux] = new List<string>();
                }

                Console.WriteLine("VOU PROPAGAR PARA AS REPLICAS");
                //notificar as replicas
                foreach (var urlB in lstReplicas)
                {
                    Console.WriteLine("Vou notificar {0}", urlB, lstReplicas.Count);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlB + "BrokerCommunication");
                    try
                    {
                        NotifyPubAsyncDelegate notifyDel = new NotifyPubAsyncDelegate(bro.updateStructsPub);
                        AsyncCallback notifyCallBack = new AsyncCallback(NotifyPubAsyncCallBack);
                        IAsyncResult notifyAr = notifyDel.BeginInvoke(m.author, m.SeqNum, lstMessage, pubMsgsNum, envieiRoot, totalCount, esperadoCount, notifyCallBack, null);
                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
                Console.WriteLine("fim do flood chamado por " + brokerName);
            }
        }

        

        public void forwardFilter(Message m, string brokerName, int eventNumber, int order, int logMode){
            Console.WriteLine("Entrei no filter chamado por " + brokerName);

            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsForwarding(m, brokerName, eventNumber, order, logMode, true);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
            }
            else
            {
                if (isLeader)
                {
                    if (!receivePubOK.ContainsKey(m.author + "|" + eventNumber.ToString()))
                    {
                        receivePubOK.Add(m.author + "|" + eventNumber.ToString(), new List<string>());
                    }
                    string senderURL = "";

                    foreach (var b in lstVizinhos)
                    {
                        if (b.Name.Equals(brokerName))
                        {
                            senderURL = b.URL.Substring(0, b.URL.Length - 6);
                            break;
                        }
                    }

                    
                    Console.WriteLine("Comparando quem chamou {0} com o meu nome {1}", brokerName, this.name);
                    //vou avisar o remetente que recebi a sub se nao tiver sido eu proprio
                    if (!brokerName.Equals(name))
                    {
                        Console.WriteLine("A mandar OK para {0}", senderURL);
                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), senderURL + "BrokerCommunication");
                        try
                        {

                            ReceivePubOKAsyncDelegate receiveOKPubDel = new ReceivePubOKAsyncDelegate(bro.receiveOKPub);
                            AsyncCallback receivePubOKPubCallBack = new AsyncCallback(ReceivePubOKAsyncCallBack);
                            IAsyncResult receiveOKAr = receiveOKPubDel.BeginInvoke(m.author + "|" + eventNumber.ToString(), mySite, receivePubOKPubCallBack, null);
                            Console.WriteLine("Enviei o ok ao sender broker " + senderURL);

                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }

                    }
                }
                

                if (order == 2)//TOTAL
                {
                    envieiRoot = false;
                    if (root)
                    {
                        Console.WriteLine("Sou root");

                        if (m.TotalNumber == 0)
                        {
                            //Console.WriteLine("Msg TO e 0");

                            lock (myLock)
                            {
                                //Console.WriteLine("GANHEI LOCK");

                                if (!pubMsgsNum.ContainsKey(m.author))
                                {//nao numerei nada deste autor
                                    Console.WriteLine("Nao numerei nada deste autor ---- {0}", m.SeqNum);
                                    pubMsgsNum.Add(m.author, new List<int>());

                                    m.TotalNumber = totalCount;
                                    pubMsgsNum[m.author].Add(m.SeqNum);
                                    Console.WriteLine("Numerei a msg {0} do topico {1} com #{2} -- recebi esta msg do {3}", m.SeqNum, m.Topic, totalCount, brokerName);

                                    totalCount++;
                                }
                                else
                                {
                                    Console.WriteLine("Ja numerei algo deste autor ---- {0}", m.SeqNum);

                                    foreach (var p in pubMsgsNum)
                                    {
                                        if (p.Key.Equals(m.author))
                                        {
                                            if (!p.Value.Contains(m.SeqNum))//nao numerei esta msg deste autor
                                            {

                                                m.TotalNumber = totalCount;
                                                pubMsgsNum[m.author].Add(m.SeqNum);
                                                Console.WriteLine("Numerei a msg {0} do topico {1} com #{2} -- recebi esta msg do {3}", m.SeqNum, m.Topic, totalCount, brokerName);

                                                totalCount++;

                                            }
                                            else
                                            {
                                                Console.WriteLine("ja numerei esta msg ---- {0}", m.SeqNum);
                                            }
                                        }
                                    }
                                }
                                // Console.WriteLine("PERDI LOCK");
                            }
                        }
                        lock (myLock)
                        {
                            if (m.TotalNumber != 0 && m.TotalNumber == esperadoCount)
                            {
                                foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                {
                                    if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                    {
                                        Console.WriteLine("Eu ROOT notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.TotalNumber);
                                        SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                        not.notify(m, eventNumber);
                                    }
                                }
                                esperadoCount++;
                            }
                            else
                            {
                                Console.WriteLine("Mensagem ja foi a root mas nao era a que eu queria TN #{0}", m.TotalNumber);

                                lstMessage.Add(m);
                                lstMessage = SortList(lstMessage);
                            }

                            //iterar lista de espera
                            for (int j = 0; j < lstMessage.Count; )
                            {
                                int next = esperadoCount;// e o proximo numSeq que estou a espera

                                if (lstMessage[j].TotalNumber == next)
                                {//msg na lista de espera e a que estava a espera
                                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                    {
                                        if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                        {
                                            Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, TN da msg ---> {0}", lstMessage[j].TotalNumber);
                                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                            not.notify(lstMessage[j], eventNumber);
                                        }
                                    }

                                    lstMessage.Remove(lstMessage[j]);

                                    j = 0; // voltar ao início da lista
                                    esperadoCount++;
                                }
                                else
                                {
                                    j++; //continuar a iterar }
                                }
                            }
                        }
                        
                        List<Broker> lidersNotified = new List<Broker>();

                        foreach (KeyValuePair<Broker, List<string>> t in routingTable)
                        {
                            string urlRemote = t.Key.URL.Substring(0, t.Key.URL.Length - 6);//retirar broker

                            //Console.WriteLine("Estou a considerar mandar a msg {0}", m.TotalNumber);

                            BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");

                            if (searchTopicList(m.Topic, t.Value))
                            {
                                if (m.TotalNumber != 0)
                                {
                                    Console.WriteLine("Sou o root estou a mandar TN #{0} para {1}", m.TotalNumber, urlRemote);
                                    lidersNotified.Add(t.Key);

                                    try
                                    {
                                        if (logMode == 1)
                                        {
                                            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                                            log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                                        }

                                        FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                        AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                        IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);

                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                            }
                            else
                            {
                                if (m.TotalNumber != 0)
                                {
                                    Message dummy = new Message("null", "null", m.SeqNum, m.author);
                                    dummy.TotalNumber = m.TotalNumber;
                                    Console.WriteLine("----------Messagem dummy--------------- {0}", t.Key.Name);

                                    try
                                    {

                                        FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                        AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                        IAsyncResult RemAr = RemoteDel.BeginInvoke(dummy, name, eventNumber, order, logMode, RemoteCallBack, null);

                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }

                                }
                            }
                        }
                        if (lidersNotified.Count != 0)
                        {
                            //espera respostas dos lideres de outros sites
                            bool allReceived = false;
                            Stopwatch sw = new Stopwatch();
                            sw.Start();
                            string aux = m.author + "|" + eventNumber.ToString();

                            while (sw.ElapsedMilliseconds < TIMEOUT)
                            {
                                if (receivePubOK.Count != 0)
                                {

                                    if (receivePubOK[aux].Count >= lidersNotified.Count)
                                    {
                                        allReceived = true;
                                        Console.WriteLine("Todos os sites estao OK");
                                        break;
                                    }
                                }
                            }

                            sw.Stop();

                            if (!allReceived)
                            {
                                Console.WriteLine("Houve sites que nao responderam!");

                                List<string> lstAux = new List<string>();

                                foreach (var a in lidersNotified)
                                {
                                    lstAux.Add(a.Site);
                                }

                                Console.Write("aux ::::::::::: {0} ->>>> {1}", aux, receivePubOK.ContainsKey(aux));

                                if (receivePubOK.ContainsKey(aux))
                                {
                                    List<string> sites_down = new List<string>(lstAux.Except(receivePubOK[aux]));
                                    Console.WriteLine("THERE ARE {0} SITES DOWN", sites_down.Count);



                                    //para cada um que falhou notificar as replicas desse site
                                    foreach (var s in sites_down)
                                    {

                                        Console.WriteLine("Este lider {0} nao respondeu.", s);
                                        foreach (var b in site_brokers[s])
                                        {
                                            if (!b.URL.Equals(site_lider[s].URL))
                                            {
                                                //Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                                BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                                try
                                                {
                                                    ElectLeaderPubAsyncDelegate electLeaderDel = new ElectLeaderPubAsyncDelegate(bro.electLeaderPub);
                                                    AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderPubAsyncCallBack);
                                                    IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(m, brokerName, eventNumber, order, logMode, false, electLeaderCallBack, null);
                                                }
                                                catch (SocketException)
                                                {
                                                    Console.WriteLine("Could not locate server");
                                                }
                                            }

                                        }

                                    }
                                }
                            }
                        }
                        Console.WriteLine("VOU PROPAGAR PARA AS REPLICAS");
                        //notificar as replicas
                        foreach (var urlB in lstReplicas)
                        {
                            Console.WriteLine("Vou notificar {0}", urlB, lstReplicas.Count);
                            BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlB + "BrokerCommunication");
                            try
                            {
                                NotifyPubAsyncDelegate notifyDel = new NotifyPubAsyncDelegate(bro.updateStructsPub);
                                AsyncCallback notifyCallBack = new AsyncCallback(NotifyPubAsyncCallBack);
                                IAsyncResult notifyAr = notifyDel.BeginInvoke(m.author, m.SeqNum, lstMessage, pubMsgsNum, envieiRoot, totalCount, esperadoCount, notifyCallBack, null);
                            }
                            catch (SocketException)
                            {
                                Console.WriteLine("Could not locate server");
                            }
                        }
                    }


                    else
                    { //nao root
                        
                            if (m.TotalNumber != 0)//ja foi a root
                            {

                                if (esperadoCount == m.TotalNumber)
                                { //msg que estava a espera

                                    Console.WriteLine("Msg ja foi a root e a que estava a espera ------ TN #" + m.TotalNumber);

                                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                    {
                                        if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                        {
                                            Console.WriteLine("Eu notifiquie o sub interesado, TN da msg ---> {0}", m.TotalNumber);
                                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                            not.notify(m, eventNumber);
                                        }
                                    }
                                    esperadoCount++;
                                }
                                else
                                {
                                    Console.WriteLine("Mensagem ja foi a root mas nao era a que eu queria TN #{0}", m.TotalNumber);

                                    lstMessage.Add(m);
                                    lstMessage = SortList(lstMessage);
                                }

                                //iterar lista de espera
                                for (int j = 0; j < lstMessage.Count; )
                                {
                                    int next = esperadoCount;// e o proximo numSeq que estou a espera

                                    if (lstMessage[j].TotalNumber == next)
                                    {//msg na lista de espera e a que estava a espera
                                        foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                        {
                                            if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                            {
                                                Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, TN da msg ---> {0}", lstMessage[j].TotalNumber);
                                                SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                                not.notify(lstMessage[j], eventNumber);
                                            }
                                        }

                                        lstMessage.Remove(lstMessage[j]);

                                        j = 0; // voltar ao início da lista
                                        esperadoCount++;
                                    }
                                    else
                                    {
                                        j++; //continuar a iterar }
                                    }
                                }
                            }

                            //ainda nao foi a root, enviar para o pai
                            else
                            {
                                string[] pais = urlPai.Split('|');

                                string urlpailider = "";
                                string sitepai = "";
                                foreach (var v in lstVizinhos)
                                {
                                    //qualquer um dos urls pai serve. estamos so a tentar obter o site
                                    if (v.URL.Substring(0, v.URL.Length - 6).Equals(pais[0]))
                                    {
                                        sitepai = v.Site;
                                        urlpailider = site_lider[sitepai].URL;
                                        urlpailider = urlpailider.Substring(0, urlpailider.Length - 6);
                                    }
                                }

                                Console.WriteLine("Mandar para o pai pois Ainda nao foi a root #{0} ----- pai: {1}", m.SeqNum,urlpailider);
                                BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlpailider + "BrokerCommunication");

                                envieiRoot = true;
                                try
                                {
                                    if (logMode == 1)
                                    {
                                        LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                                        log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                                    }

                                    FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                    AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                    IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);


                                }
                                catch (SocketException)
                                {
                                    Console.WriteLine("Could not locate server");
                                }


                                //espera resposta do pai
                                bool allReceived = false;
                                Stopwatch sw = new Stopwatch();
                                sw.Start();
                                string aux = m.author + "|" + eventNumber.ToString();

                                while (sw.ElapsedMilliseconds < TIMEOUT)
                                {
                                    if (receivePubOK.Count != 0)
                                    {

                                        if (receivePubOK[aux].Count >= 1)
                                        {
                                            allReceived = true;
                                            Console.WriteLine("O pai esta OK");
                                            break;
                                        }
                                    }
                                }

                                sw.Stop();

                                if (!allReceived)
                                {
                                    Console.WriteLine("O pai nao respondeu!");


                                    Console.Write("aux ::::::::::: {0} ->>>> {1}", aux, receivePubOK.ContainsKey(aux));

                                    if (receivePubOK.ContainsKey(aux))
                                    {

                                        //notificar as replicas do site do pai, a dizer que ele falhou

                                        Console.WriteLine("Este pai {0} nao respondeu.", urlpailider);


                                        foreach (var b in lstVizinhos)
                                        {
                                            if (b.Site.Equals(sitepai))
                                            {
                                                //Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                                bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                                try
                                                {
                                                    ElectLeaderPubAsyncDelegate electLeaderDel = new ElectLeaderPubAsyncDelegate(bro.electLeaderPub);
                                                    AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderPubAsyncCallBack);
                                                    IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(m, brokerName, eventNumber, order, logMode, false, electLeaderCallBack, null);
                                                }
                                                catch (SocketException)
                                                {
                                                    Console.WriteLine("Could not locate server");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        

                        Dictionary<Broker, List<string>> lst = new Dictionary<Broker, List<string>>(routingTable);
                        //eliminar remetente da routing table
                        foreach (KeyValuePair<Broker, List<string>> par in routingTable)
                        {
                            if (par.Key.Name.Equals(brokerName))
                            {
                                lst.Remove(par.Key);
                            }
                        }

                        List<Broker> lnotified = new List<Broker>();

                        foreach (KeyValuePair<Broker, List<string>> t in lst)
                        {
                            string urlRemote = t.Key.URL.Substring(0, t.Key.URL.Length - 6);//retirar broker

                            Console.WriteLine("Estou a considerar o {0} para propagar", urlRemote);

                            BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");

                            if ((searchTopicList(m.Topic, t.Value)))
                            { //ha alguem na routing table que quer este topico

                                if (!bro.isRoot() && !envieiRoot)
                                {
                                    lnotified.Add(t.Key);
                                    Console.WriteLine("Filtering vizinho em {0} --> #msg {1}", urlRemote, m.SeqNum);

                                    try
                                    {
                                        if (logMode == 1)
                                        {
                                            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                                            log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                                        }

                                        FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                        AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                        IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);

                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                            }
                            else
                            {

                                Message dummy = new Message("null", "null", m.SeqNum, m.author);
                                dummy.TotalNumber = m.TotalNumber;

                                //Console.WriteLine("----------Messagem dummy---------------");

                                try
                                {

                                    FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                    AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                    IAsyncResult RemAr = RemoteDel.BeginInvoke(dummy, name, eventNumber, order, logMode, RemoteCallBack, null);

                                }
                                catch (SocketException)
                                {
                                    Console.WriteLine("Could not locate server");
                                }

                            }
                        }
                        if (lnotified.Count != 0)
                        {

                            //espera respostas dos lideres de outros sites
                            bool allReceived = false;
                            Stopwatch sw = new Stopwatch();
                            sw.Start();
                            string aux = m.author + "|" + eventNumber.ToString();

                            while (sw.ElapsedMilliseconds < TIMEOUT)
                            {
                                if (receivePubOK.Count != 0)
                                {

                                    if (receivePubOK[aux].Count >= lnotified.Count)
                                    {
                                        allReceived = true;
                                        Console.WriteLine("Todos os sites estao OK");
                                        break;
                                    }
                                }
                            }

                            sw.Stop();

                            if (!allReceived)
                            {
                                Console.WriteLine("Houve sites que nao responderam!");

                                List<string> lstAux = new List<string>();

                                foreach (var a in lnotified)
                                {
                                    lstAux.Add(a.Site);
                                }

                                Console.Write("aux ::::::::::: {0} ->>>> {1}", aux, receivePubOK.ContainsKey(aux));

                                if (receivePubOK.ContainsKey(aux))
                                {
                                    List<string> sites_down = new List<string>(lstAux.Except(receivePubOK[aux]));
                                    Console.WriteLine("THERE ARE {0} SITES DOWN", sites_down.Count);



                                    //para cada um que falhou notificar as replicas desse site
                                    foreach (var s in sites_down)
                                    {

                                        Console.WriteLine("Este lider {0} nao respondeu.", s);
                                        foreach (var b in site_brokers[s])
                                        {

                                            if (!b.URL.Equals(site_lider[s].URL))
                                            {

                                                //Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                                BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                                try
                                                {
                                                    ElectLeaderPubAsyncDelegate electLeaderDel = new ElectLeaderPubAsyncDelegate(bro.electLeaderPub);
                                                    AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderPubAsyncCallBack);
                                                    IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(m, brokerName, eventNumber, order, logMode, false, electLeaderCallBack, null);
                                                }
                                                catch (SocketException)
                                                {
                                                    Console.WriteLine("Could not locate server");
                                                }
                                            }

                                        }

                                    }
                                }

                            }
                        }

                        Console.WriteLine("VOU PROPAGAR PARA AS REPLICAS");
                        //notificar as replicas
                        foreach (var urlB in lstReplicas)
                        {
                            Console.WriteLine("Vou notificar {0}", urlB, lstReplicas.Count);
                            BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlB + "BrokerCommunication");
                            try
                            {
                                NotifyPubAsyncDelegate notifyDel = new NotifyPubAsyncDelegate(bro.updateStructsPub);
                                AsyncCallback notifyCallBack = new AsyncCallback(NotifyPubAsyncCallBack);
                                IAsyncResult notifyAr = notifyDel.BeginInvoke(m.author, m.SeqNum, lstMessage, pubMsgsNum, envieiRoot, totalCount, esperadoCount, notifyCallBack, null);
                            }
                            catch (SocketException)
                            {
                                Console.WriteLine("Could not locate server");
                            }
                        }
                    }
                }


                string pubTopic = m.author + "%" + m.Topic;

                if (order == 1)//FIFO
                {
                    lock (myLock)
                    {
                        if (!seqPub.ContainsKey(m.author))
                        {//publisher nao publicou nada
                            seqPub.Add(m.author, 1);
                        }

                        if (!pubCount.ContainsKey(pubTopic))//publisher nao publicou nada neste topico
                        {
                            pubCount.Add(pubTopic, 1);
                        }

                        if (m.SeqNum == seqPub[m.author])//msg esperada para aquele publisher
                        {
                            foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                            {
                                if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                                {
                                    Console.WriteLine("Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", m.SeqNum);
                                    SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                    not.notify(m, eventNumber);
                                }
                            }
                            //ja notifiquei quem tinha a notificar com esta publicacao
                            seqPub[m.author]++;
                            pubCount[pubTopic]++;
                        }
                        else
                        { //nao e a msg esperada
                          //Console.WriteLine("{0} - Esperado {1} ----- {2} Recebido", pubTopic, seqPub[m.author], m.SeqNum);
                            lstMessage.Add(m);
                            lstMessage = SortList(lstMessage);
                        }



                        //iterar sobre lista de espera para ver se posso mandar alguma coisa
                        for (int j = 0; j < lstMessage.Count;)
                        {
                            int next = seqPub[m.author];// e o proximo numSeq que estou a espera para aquele autor

                            int seqPubTop = pubCount[pubTopic];//numSeq para aquele topico+publisher

                            //Console.WriteLine("Estou a iterar a procura do {0} para <{1}>", next, pubTopic);
                            foreach (Message mi in lstMessage) Console.Write("a: " + mi.author + ",n: " + mi.SeqNum + "#");
                            //Console.WriteLine("");
                            if (lstMessage[j].author.Equals(m.author) && lstMessage[j].SeqNum == next)//msg na lista de espera e do mesmo autor e a que estou a espera
                            {
                                foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                                {
                                    if (searchTopicList(lstMessage[j].Topic, t.Value))//ha match de topicos para um sub meu
                                    {
                                        //Console.WriteLine("Durante a iteracao Eu notifiquie o sub interesado, numeroSeq da msg ---> {0}", lstMessage[j].SeqNum);
                                        SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                                        not.notify(lstMessage[j], eventNumber);
                                    }
                                }
                                //Console.Write("antes de descartar mensagem : " + lstMessage[j].SeqNum + " #");
                                lstMessage.Remove(lstMessage[j]);

                                j = 0; // voltar ao início da lista
                                seqPub[m.author]++;
                                pubCount[pubTopic]++;
                                //Console.WriteLine("seq actual: " + seqPub[m.author]);
                            }
                            else
                            {
                                j++; //continuar a iterar }
                            }

                        }
                    }
                }
                if (order == 0) //modo NO order
                {
                    foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
                    {

                        if (searchTopicList(m.Topic, t.Value))//ha match de topicos
                        {
                            Console.WriteLine("Eu notifiquie o sub interesado, numeroSeq ---> {0}", m.SeqNum);
                            SubscriberNotify not = (SubscriberNotify)Activator.GetObject(typeof(SubscriberNotify), t.Key + "/Notify");
                            not.notify(m, eventNumber);
                        }
                    }
                }

                if ((order == 0) || (order == 1))
                {
                    //PROPAGATION TIME


                    Dictionary<Broker, List<string>> lst = new Dictionary<Broker, List<string>>(routingTable);
                    //eliminar remetente da routing table
                    foreach (KeyValuePair<Broker, List<string>> par in routingTable)
                    {
                        if (par.Key.Name.Equals(brokerName))
                        {
                            lst.Remove(par.Key);
                        }
                    }

                    List<Broker> lnotified = new List<Broker>();
                    foreach (KeyValuePair<Broker, List<string>> t in lst)
                    {
                        string urlRemote = t.Key.URL.Substring(0, t.Key.URL.Length - 6);//retirar broker

                        //Console.WriteLine("Estou a considerar o {0}", urlRemote);

                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");

                        if ((searchTopicList(m.Topic, t.Value)) || (order == 3 && root))
                        { //ha alguem na routing table que quer este topico



                            Console.WriteLine("Filtering vizinho em {0} --> #msg {1}", urlRemote, m.SeqNum);

                            try
                            {
                                if (logMode == 1)
                                {
                                    LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+urlPup+":1001/PuppetMasterLog");
                                    log.log(this.name, m.author, m.Topic, eventNumber, "broker");
                                }

                                FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                IAsyncResult RemAr = RemoteDel.BeginInvoke(m, name, eventNumber, order, logMode, RemoteCallBack, null);

                            }
                            catch (SocketException)
                            {
                                Console.WriteLine("Could not locate server");
                            }
                        }
                        else
                        {
                            Message dummy = new Message("null", "null", m.SeqNum, m.author);

                            //Console.WriteLine("----------Messagem dummy---------------");

                            try
                            {

                                FilterFloodRemoteAsyncDelegate RemoteDel = new FilterFloodRemoteAsyncDelegate(bro.forwardFilter);
                                AsyncCallback RemoteCallBack = new AsyncCallback(FilterFloodRemoteAsyncCallBack);
                                IAsyncResult RemAr = RemoteDel.BeginInvoke(dummy, name, eventNumber, order, logMode, RemoteCallBack, null);

                            }
                            catch (SocketException)
                            {
                                Console.WriteLine("Could not locate server");
                            }
                        }
                    }
                    if (lnotified.Count != 0)
                    {

                        //espera respostas dos lideres de outros sites
                        bool allReceived = false;
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        string aux = m.author + "|" + eventNumber.ToString();

                        while (sw.ElapsedMilliseconds < TIMEOUT)
                        {
                            if (receivePubOK.Count != 0)
                            {

                                if (receivePubOK[aux].Count >= lnotified.Count)
                                {
                                    allReceived = true;
                                    Console.WriteLine("Todos os sites estao OK");
                                    break;
                                }
                            }
                        }

                        sw.Stop();

                        if (!allReceived)
                        {
                            Console.WriteLine("Houve sites que nao responderam!");

                            List<string> lstAux = new List<string>();

                            foreach (var a in lnotified)
                            {
                                lstAux.Add(a.Site);
                            }

                            Console.Write("aux ::::::::::: {0} ->>>> {1}", aux, receivePubOK.ContainsKey(aux));

                            if (receivePubOK.ContainsKey(aux))
                            {
                                List<string> sites_down = new List<string>(lstAux.Except(receivePubOK[aux]));
                                Console.WriteLine("THERE ARE {0} SITES DOWN", sites_down.Count);



                                //para cada um que falhou notificar as replicas desse site
                                foreach (var s in sites_down)
                                {

                                    Console.WriteLine("Este lider {0} nao respondeu.", s);
                                    foreach (var b in site_brokers[s])
                                    {

                                        if (!b.URL.Equals(site_lider[s].URL))
                                        {

                                            //Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                            BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                            try
                                            {
                                                ElectLeaderPubAsyncDelegate electLeaderDel = new ElectLeaderPubAsyncDelegate(bro.electLeaderPub);
                                                AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderPubAsyncCallBack);
                                                IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(m, brokerName, eventNumber, order, logMode, false, electLeaderCallBack, null);
                                            }
                                            catch (SocketException)
                                            {
                                                Console.WriteLine("Could not locate server");
                                            }
                                        }

                                    }

                                }
                            }

                        }
                    }

                    Console.WriteLine("VOU PROPAGAR PARA AS REPLICAS");
                    //notificar as replicas
                    foreach (var urlB in lstReplicas)
                    {
                        Console.WriteLine("Vou notificar {0}", urlB, lstReplicas.Count);
                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlB + "BrokerCommunication");
                        try
                        {
                            NotifyPubAsyncDelegate notifyDel = new NotifyPubAsyncDelegate(bro.updateStructsPub);
                            AsyncCallback notifyCallBack = new AsyncCallback(NotifyPubAsyncCallBack);
                            IAsyncResult notifyAr = notifyDel.BeginInvoke(m.author, m.SeqNum, lstMessage, pubMsgsNum, envieiRoot, totalCount, esperadoCount, notifyCallBack, null);
                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }
                    }
                }
            }
            Console.WriteLine("Sai do filter chamado por " + brokerName);
        }

        public void forwardSub(string topic, string brokerName)
        {
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsForwardSubUnsub(topic, brokerName, true);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
            }
            else
            {
                Console.WriteLine("ForwardSub recebido de " + brokerName + " topico -> " + topic);

                if (isLeader)
                {
                    string senderURL = "";

                    foreach (var b in lstVizinhos)
                    {
                        if (b.Name.Equals(brokerName))
                            senderURL = b.URL.Substring(0, b.URL.Length - 6);
                    }

                    //Console.WriteLine("O meu sender foi {0}", senderURL);

                    //vou avisar o remetente que recebi a sub se nao tiver sido eu proprio
                    if (!brokerName.Equals(name))
                    {
                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), senderURL + "BrokerCommunication");
                        try
                        {
                            //Console.WriteLine("Vou enviar o OK do meu site " + mySite);
                            SendOKAsyncDelegate sendOKDel = new SendOKAsyncDelegate(bro.receiveOK);
                            AsyncCallback sendOKCallBack = new AsyncCallback(SendOKAsyncCallBack);
                            IAsyncResult sendOKAr = sendOKDel.BeginInvoke(mySite, sendOKCallBack, null);
                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }

                    }
                }
                lock (myLock)
                {
                    //buscar broker com nome brokerName
                    foreach (var v in lstVizinhos)
                    {
                        if (v.Name.Equals(brokerName))
                        {
                            Broker aux = v;

                            if (routingTable.ContainsKey(aux))//ja tenho uma entrada para este broker
                            {
                                //Console.WriteLine("Ja tinha uma entrada para este broker - " + brokerName);

                                if (!routingTable[aux].Contains(topic))//adicionar apenas se for outro topico
                                {
                                    routingTable[aux].Add(topic);

                                }
                            }
                            else
                            {
                                //Console.WriteLine("Nao tinha uma entrada para este broker - " + brokerName);

                                //nao tinha uma entrada para este broker
                                if (!brokerName.Equals(this.name))
                                {
                                    List<string> seqs = new List<string>();

                                    foreach (var entry in seqPub)
                                    {
                                        string ss = entry.Key + "%" + entry.Value;
                                        seqs.Add(ss);
                                    }

                                    string urlRemote = aux.URL.Substring(0, aux.URL.Length - 6);
                                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");


                                    if (this.isRoot())
                                    {
                                        //Console.WriteLine("ROOT::Vou mandar o updateSeqs para este broker - " + urlRemote);
                                        bro.updateSequenceNumbers(seqs, this.totalCount);
                                    }
                                    else
                                    {
                                        if (!bro.isRoot())
                                        {
                                            bro.updateSequenceNumbers(seqs, this.esperadoCount);
                                            //Console.WriteLine("Vou mandar o updateSeqs para este broker - " + aux.Name);
                                        }
                                    }

                                    //Console.WriteLine("Depois de mandar update");
                                }

                                //Console.WriteLine("criei entrada na routing table para este broker - " + brokerName);
                                routingTable[aux] = new List<string> { topic };
                            }
                        }

                    }
                }

                if (!isLeader)
                {
                    //Console.WriteLine("FWSUB :: Nao era lider terminei");
                    return;
                }

                List<Broker> lst = new List<Broker>(lstVizinhos);
                
                //eliminar remetente da lista
                for (int i = 0; i < lst.Count; i++)
                {
                    if (lst[i].Name.Equals(brokerName))
                    {
                        //Console.WriteLine("Removi o {0} da lista de vizinhos porque era o remetente", lst[i].Name);
                        lst.Remove(lst[i]);
                    }
                }

                List<Broker> leaders = new List<Broker>(lst);
                //selecionar restantes lideres
                for (int i = 0; i < lst.Count; i++) {
                    
                    if (!lst[i].URL.Equals(site_lider[lst[i].Site].URL))
                    {
                        //Console.WriteLine("Removi {0} da lista anterior pois nao era lider", lst[i].URL);
                        leaders.Remove(lst[i]);
                    }
                }

                //propagar para os outros todos lideres
                foreach (var viz in leaders)
                {
                    string urlRemote = viz.URL.Substring(0, viz.URL.Length - 6);//retirar broker
                    //Console.WriteLine("Depois das listas, Flooding lider em {0}", urlRemote);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");
                    try
                    {
                        SubUnsubRemoteAsyncDelegate RemoteDel = new SubUnsubRemoteAsyncDelegate(bro.forwardSub);
                        AsyncCallback RemoteCallBack = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(topic, name, RemoteCallBack, null);
                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
                //se enviou a alguem
                if (leaders.Count != 0)
                {
                    //espera respostas dos lideres de outros sites
                    bool allReceived = false;
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    while (sw.ElapsedMilliseconds < TIMEOUT)
                    {
                        if (sitesOK.Count >= leaders.Count)
                        {
                            allReceived = true;
                            //Console.WriteLine("Todos os sites estao OK");
                            break;
                        }
                    }

                    sw.Stop();

                    if (!allReceived)
                    {
                        
                        //Console.WriteLine("Houve lideres que nao responderam com OK!");
                        List<string> aux = new List<string>();

                        foreach(var a in leaders) {
                            aux.Add(a.Site);
                        }

                        List<string> sites_down = new List<string>(aux.Except(sitesOK));

                        //para cada um que falhou notificar as replicas desse site
                        foreach (var s in sites_down)
                        {
                            Console.WriteLine("Este lider {0} nao respondeu.", s);
                            foreach (var b in site_brokers[s])
                            {
                                
                                if (!b.URL.Equals(site_lider[s].URL))
                                {

                                    Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                    try
                                    {
                                        ElectLeaderAsyncDelegate electLeaderDel = new ElectLeaderAsyncDelegate(bro.electLeader);
                                        AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderAsyncCallBack);
                                        IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(name, topic, true, electLeaderCallBack, null);
                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                                
                            }

                        }


                    }
                    //faz reset ha estrutura dinamica para ser utilizado num evento futuro
                    sitesOK = new List<string>();
                }

                //propagar para as replicas
                foreach (var rep in lstReplicas)
                {
                    //Console.WriteLine("----Vou enviar para a minha replica {0}", rep);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), rep + "BrokerCommunication");
                    try
                    {
                        SubUnsubRemoteAsyncDelegate RemoteDel = new SubUnsubRemoteAsyncDelegate(bro.forwardSub);
                        AsyncCallback RemoteCallBack = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(topic, name, RemoteCallBack, null);

                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
                
                
                Console.WriteLine("Acabei o FWSUB");

            }
        }

        public void receiveOKPub(string pubName_eventN, string senderSite)
        {
            
                Console.WriteLine("Recebi OK , Vou adicionar {0} -> {1}", pubName_eventN, senderSite);

                if (receivePubOK.ContainsKey(pubName_eventN))
                {
                    Console.WriteLine("Ja tinha a key : adicionei {0} ", senderSite);
                    receivePubOK[pubName_eventN].Add(senderSite);
                }
                else
                {
                    Console.WriteLine("Nao tinha a key : adicionei {0} ", senderSite);
                    List<string> lstAux = new List<string>();
                    lstAux.Add(senderSite);
                    receivePubOK.Add(pubName_eventN, lstAux);

                }
            

            /*
            Console.WriteLine("PUB ::: " + pubName_eventN[0]);
            Console.WriteLine("EVN ::: " + pubName_eventN[1]);

            Console.WriteLine("Estrutura contem [{0},{1}] -> {2}", pubName_eventN[0], pubName_eventN[1], receivePubOK.ContainsKey(pubName_eventN));
            Console.WriteLine("Na estrutura esta {0} -> {1}", senderSite, receivePubOK[pubName_eventN].Contains(senderSite));
            */
        }

        public void updateStructsPub(string pubName, int seqN, List<Message> lstMsg, Dictionary<string, List<int>> lstMsgTO, Boolean t, int tCount, int espCount)
        {

            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsUpdateStructs(pubName, seqN, lstMsg, lstMsgTO, t, tCount, espCount);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
            }
            else
            {
                //update seqNum
                if (!seqPub.ContainsKey(pubName))
                {
                    seqPub.Add(pubName, seqN);
                }
                else
                {
                    seqPub[pubName] = seqN;
                }

                //update lista de mensagens
                lstMessage = new List<Message>(lstMsg);

                //update  lista total order number
                pubMsgsNum = new Dictionary<string, List<int>>(lstMsgTO);

                // update se já enviei para o root
                envieiRoot = t;

                totalCount = tCount;

                esperadoCount = espCount;

                Console.WriteLine("Update de estruturas.");
            }

        }

        public string[] electLeaderPub(Message m, String brokerName, int eventN, int order, int logMode, Boolean filtering)
        {
           
            List<string> aux = new List<string>();
            foreach (var rep in lstReplicas)
            {
                if (!rep.Equals(urlLider))
                {
                    aux.Add(rep);
                }
            }

            tryLeader(aux);

            if (isLeader)
            {
                BrokerReceiveBroker bro;
                foreach (var k in site_lider)
                {
                    string url = k.Value.URL.Substring(0, k.Value.URL.Length - 6);
                    Console.WriteLine("Fiquei lider, vou mandar cenas para os vizinhos ---- {0}", url);
                    bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), url + "BrokerCommunication");
                    try
                    {
                        LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                        AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(this.myURL, RemoteCallBack, null);

                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }


                foreach (var r in lstReplicas)
                {
                    Console.WriteLine("Fiquei lider, vou mandar cenas para as replicas ---- {0}", r);
                    bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), r + "BrokerCommunication");
                    try
                    {
                        LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                        AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(this.myURL, RemoteCallBack, null);

                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }

                if (filtering)
                    forwardFilter(m, brokerName, eventN, order, logMode);
                else
                    forwardFlood(m, brokerName, eventN, order, logMode);
            }

            return new string[] { mySite, urlLider };
        }

        public string[] electLeader(string brokerName, string topic, bool isSub)
        {

            Console.WriteLine("Vou eleger um novo lider. quem chamou foi "+brokerName);

            
            List<string> aux = new List<string>();
            foreach (var rep in lstReplicas)
            {
                if (!rep.Equals(urlLider))
                {
                    aux.Add(rep);
                }
            }

            tryLeader(aux);

            Console.WriteLine("sou o lider? : {0}", isLeader);
            if (isLeader)
            {
                if(isSub)
                    forwardSub(topic, brokerName);
                else
                    forwardUnsub(topic, brokerName);
            }

            Console.WriteLine("\to novo lider para o meu site {0} eh {1}", mySite, urlLider);

            return new string[] { mySite, urlLider };

        }

        public void receiveOK(string senderSite)
        {

            Console.WriteLine("Recebi OK do site " + senderSite);

            if (sitesOK.Contains(senderSite))
            {
                Console.WriteLine("sitesOK nao vazia, quem mandou o OK foi o " + senderSite);
                foreach (var z in sitesOK)
                {
                    Console.WriteLine("Quem ja esta nos sitesOk -> " + z);
                }
            }
            else
                sitesOK.Add(senderSite);
        }

        public void forwardUnsub(string topic, string brokerName)
        {
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsForwardSubUnsub(topic, brokerName, false);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
            }
            else
            {
                Console.WriteLine("ForwardUnsub recebido de " + brokerName + " topico -> " + topic);
                if (isLeader)
                {
                    string senderURL = "";

                    foreach (var b in lstVizinhos)
                    {
                        if (b.Name.Equals(brokerName))
                            senderURL = b.URL.Substring(0, b.URL.Length - 6);
                    }

                    Console.WriteLine("O meu sender foi {0}", senderURL);

                    //vou avisar o remetente que recebi a sub se nao tiver sido eu proprio
                    if (!brokerName.Equals(name))
                    {
                        BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), senderURL + "BrokerCommunication");
                        try
                        {
                            Console.WriteLine("Vou enviar o OK do meu site " + mySite);
                            SendOKAsyncDelegate sendOKDel = new SendOKAsyncDelegate(bro.receiveOK);
                            AsyncCallback sendOKCallBack = new AsyncCallback(SendOKAsyncCallBack);
                            IAsyncResult sendOKAr = sendOKDel.BeginInvoke(mySite, sendOKCallBack, null);
                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }

                    }
                }

                //Console.WriteLine("unsub on topic {0} received from {1}", topic, brokerName);

                //buscar broker com nome brokerName
                foreach (var v in lstVizinhos)
                {
                    if (v.Name.Equals(brokerName))
                    {
                        Broker aux = v;
                        if (routingTable.ContainsKey(aux))//ja tenho uma entrada para este broker
                        {
                            //routingTable[aux].Remove(topic);
                        }
                    }
                }

                List<Broker> lst = new List<Broker>(lstVizinhos);
                //eliminar remetente da lista
                for (int i = 0; i < lst.Count; i++)
                {
                    if (lst[i].Name.Equals(brokerName))
                    {
                        //Console.WriteLine("Removi o {0} da lista de vizinhos", lst[i].Name);
                        lst.Remove(lst[i]);
                    }
                }

                if (!isLeader)
                    return;

                List<Broker> lstAux = new List<Broker>(lstVizinhos);

                //eliminar remetente da lista
                for (int i = 0; i < lst.Count; i++)
                {
                    if (lstAux[i].Name.Equals(brokerName))
                    {
                        //Console.WriteLine("Removi o {0} da lista de vizinhos", lst[i].Name);
                        lstAux.Remove(lst[i]);
                    }
                }

                List<Broker> leaders = new List<Broker>(lst);
                //selecionar restantes lideres
                for (int i = 0; i < lst.Count; i++)
                {

                    if (!lst[i].URL.Equals(site_lider[lst[i].Site].URL))
                    {
                        //Console.WriteLine("Removi {0}", lst[i].URL);
                        leaders.Remove(lst[i]);
                    }
                }

                //propagar para os outros todos lideres
                foreach (var viz in leaders)
                {
                    string urlRemote = viz.URL.Substring(0, viz.URL.Length - 6);//retirar broker
                    //Console.WriteLine("Flooding vizinho em {0}", urlRemote);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), urlRemote + "BrokerCommunication");
                    try
                    {
                        SubUnsubRemoteAsyncDelegate RemoteDel = new SubUnsubRemoteAsyncDelegate(bro.forwardUnsub);
                        AsyncCallback RemoteCallBack = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(topic, name, RemoteCallBack, null);
                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
                //se enviou a alguem
                if (leaders.Count != 0)
                {
                    //espera respostas dos lideres de outros sites
                    bool allReceived = false;
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    while (sw.ElapsedMilliseconds < TIMEOUT)
                    {
                        if (sitesOK.Count >= leaders.Count)
                        {
                            allReceived = true;
                            Console.WriteLine("Todos os sites estao OK");
                            break;
                        }
                    }

                    sw.Stop();

                    if (!allReceived)
                    {

                        Console.WriteLine("Houve lideres que nao responderam com OK!");
                        List<string> aux = new List<string>();

                        foreach (var a in leaders)
                        {
                            aux.Add(a.Site);
                        }

                        List<string> sites_down = new List<string>(aux.Except(sitesOK));

                        //para cada um que falhou notificar as replicas desse site
                        foreach (var s in sites_down)
                        {
                            Console.WriteLine("Este lider {0} nao respondeu.", s);
                            foreach (var b in site_brokers[s])
                            {
                                if (!b.URL.Equals(site_lider[s].URL))
                                {
                                    Console.WriteLine("VOU NOTIFICAR A REPLICA {0} QUE O SEU LIDER {1} MORREU!", b.URL, s);
                                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), b.URL.Substring(0, b.URL.Length - 6) + "BrokerCommunication");
                                    try
                                    {
                                        ElectLeaderAsyncDelegate electLeaderDel = new ElectLeaderAsyncDelegate(bro.electLeader);
                                        AsyncCallback electLeaderCallBack = new AsyncCallback(ElectLeaderAsyncCallBack);
                                        IAsyncResult electLeaderAr = electLeaderDel.BeginInvoke(name, topic, false, electLeaderCallBack, null);
                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                            }
                        }
                    }
                    //faz reset a estrutura dinamica para ser utilizado num evento futuro
                    sitesOK = new List<string>();
                }

                //propagar para as replicas
                foreach (var rep in lstReplicas)
                {
                    //Console.WriteLine("----Vou enviar para a minha replica {0}", rep);
                    BrokerReceiveBroker bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), rep + "BrokerCommunication");
                    try
                    {
                        SubUnsubRemoteAsyncDelegate RemoteDel = new SubUnsubRemoteAsyncDelegate(bro.forwardUnsub);
                        AsyncCallback RemoteCallBack = new AsyncCallback(SubUnsubRemoteAsyncCallBack);
                        IAsyncResult RemAr = RemoteDel.BeginInvoke(topic, name, RemoteCallBack, null);

                    }
                    catch (SocketException)
                    {
                        Console.WriteLine("Could not locate server");
                    }
                }
            }
        }
        
        public string receiveSub(string topic, string subName)
        {
            Console.WriteLine("Receive sub on topic {0} from {1}", topic, subName);
            if (frozen)
            {
                Console.WriteLine("Estava frozen");
                Evento eve = new Evento();
                eve.setArgumentsSubUnsub(topic, subName, true);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
                return "";
            }
            else
            {
                Console.WriteLine("sub on topic {0} received from subscriber -> {1}", topic, subName);
                if (lstSubsTopic.ContainsKey(subName))
                {
                    //Console.WriteLine("Ja tinha uma subs para este tipo");
                    lstSubsTopic[subName].Add(topic);
                }
                else
                {
                    //Console.WriteLine("Nao tinha nenhuma sub para este tipo");
                    lstSubsTopic[subName] = new List<string> { topic };
                }
                if (isLeader)
                {
                    forwardSub(topic, name);
                    return myURL.Substring(0, myURL.Length - 6);
                }
                else
                {
                    return myURL.Substring(0, myURL.Length - 6);
                }
            }
        }

        
        public string receiveUnsub(string topic, string subName)
        {
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsSubUnsub(topic, subName, false);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
                return "";
            }
            else
            {
                Console.WriteLine("unsub on topic {0} received from {1}", topic, subName);
                if (lstSubsTopic.ContainsKey(subName))
                {
                    lstSubsTopic[subName].Remove(topic);
                }
                if (isLeader)
                {
                    forwardUnsub(topic, name);
                    return myURL;
                }
                else
                {
                    return myURL;
                }
            }
        }

        public void receivePublication(Message m, string pubName, string pubURL, int filter, int order, int eventNumber, int logMode)
        {
            
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setArgumentsReceivePub(m, pubName, pubURL, filter, order, eventNumber, logMode);

                frozenEvents.Add(eve);

                Console.WriteLine("Estava frozen, pus na lista");
                
            }

            else
            {

                PubInterface pub = (PubInterface)Activator.GetObject(typeof(PubInterface), pubURL + "PMPublish");
                try
                {
                    SendPubOK sendPubOKDel = new SendPubOK(pub.receivePubOK);
                    AsyncCallback sendPubOKCallBack = new AsyncCallback(SendPubOKAsyncCallBack);
                    IAsyncResult sendPubOKAr = sendPubOKDel.BeginInvoke(myURL, eventNumber, sendPubOKCallBack, null);
                    Console.WriteLine("Enviei ok ao publisher" + pubURL);
                }
                catch (SocketException)
                {
                    Console.WriteLine("Could not locate server");
                }
                


                if (isLeader)
                {
                    Console.WriteLine("LIDER Recebi publicacao no eventNumber {0} e NumSeq {1}", eventNumber, m.SeqNum);
                    if (filter == 0)
                    {
                        forwardFlood(m, name, eventNumber, order, logMode);
                        
                    }
                    else
                    {
                        forwardFilter(m, name, eventNumber, order, logMode);
                      
                    }
                }
                else
                {
                    Console.WriteLine("Recebi publicação {0}", m.SeqNum);
 
                }
            }
            

        }

        public string alive() {
            return this.myURL.Substring(0, myURL.Length - 6);
        }

        public void updateSequenceNumbers(List<string> lst, int TO)
        {

            lock (myLock)
            {
                //Console.WriteLine("GANHEI LOCK updateSeqNum");
                if (this.esperadoCount < TO)
                {

                    //Console.WriteLine("Update do meu TO esperado para {0}", TO);
                    this.esperadoCount = TO;
                }
                else
                {
                    //Console.WriteLine("O meu esperado era igual ou maior ---- {0} > {1}", this.esperadoCount, TO);
                }

                foreach (var ss in lst)
                {
                    string[] res = ss.Split('%');
                    //res[0]-publisher ;res[1]-seqNum
                    int seq = Int32.Parse(res[1]);


                    foreach (var entry in pubCount)
                    {
                        if (entry.Key.Equals(res[0]))
                        {
                            //ja tinha guardado um seqNum para este publisher
                            if (entry.Value < seq)
                            {
                                //valor guardado era inferior
                                pubCount[res[0]] = seq;
                            }
                        }
                        else
                        {
                            //nao tinha guardado um seqNum para este publisher
                            pubCount.Add(res[0], seq);
                        }

                    }
                }
                //Console.WriteLine("PERDI LOCK updateSeqNum");
            }
        }

        public void iamleader(string u) {
            if (frozen)
            {
                Evento eve = new Evento();
                eve.setIamLeaderArguments(u);

                frozenEvents.Add(eve);

                //Console.WriteLine("Estava frozen, pus na lista");
            }
            else {
                Boolean alterei = false;
                foreach (var s in lstReplicas)
                {
                    if (s.Equals(u.Substring(0,u.Length-6)))
                    {
                        Console.WriteLine("Recebi iamlider de uma das minhas replicas, alterei o lider do meu site para " + u);
                        this.urlLider = u;
                        alterei = true;
                        isLeader = false;
                    }
                }
                if (!alterei)
                {
                    foreach (var v in lstVizinhos) {
                        if (v.URL.Equals(u)) {
                            Console.WriteLine("Recebi iamlider dum gajo nao do meu site --- " + u);
                            Console.WriteLine("Actualizei lider do site{0} para {1} ",v.Site,u);
                            site_lider[v.Site] = v;
                            if (isLeader)
                            {
                                Console.WriteLine("Actualizei as minhas replicas para saberem o lider nao do meu site");

                                BrokerReceiveBroker bro;
                                foreach (var r in lstReplicas)
                                {
                                    bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), r + "BrokerCommunication");
                                    try
                                    {
                                        LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                                        AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                                        IAsyncResult RemAr = RemoteDel.BeginInvoke(u, RemoteCallBack, null);

                                    }
                                    catch (SocketException)
                                    {
                                        Console.WriteLine("Could not locate server");
                                    }
                                }
                            }

                            break;
                        }
                    }
                }
            }

        }

        public void receiveDeadSub(string subName, string topic, string dead, Boolean sub)
        {
            //Console.WriteLine("Receive DEAD sub, recebido do sub {0} no topico {1}, quem esta morto {2}", subName, topic, dead);

            List<string> aux = new List<string>();
            foreach (var rep in lstReplicas)
            {
                if (!rep.Equals(dead))
                {
                    aux.Add(rep);
                }
            }

            tryLeader(aux);

            
            if (!isLeader && dead.Equals(urlLider))
            {
                Console.WriteLine("Lider morto");
                
                tryLeader(aux);

                if (isLeader)
                {
                    BrokerReceiveBroker bro;
                    foreach (var k in site_lider) {
                        string url = k.Value.URL.Substring(0, k.Value.URL.Length - 6);
                        Console.WriteLine("Fiquei lider, vou mandar cenas para os vizinhos ---- {0}", url);
                        bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), url + "BrokerCommunication");
                        try
                        {
                            LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                            AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                            IAsyncResult RemAr = RemoteDel.BeginInvoke(this.myURL, RemoteCallBack, null);

                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }
                    }


                    foreach (var r in lstReplicas) {
                        Console.WriteLine("Fiquei lider, vou mandar cenas para as replicas ---- {0}", r);
                        bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), r + "BrokerCommunication");
                        try
                        {
                            LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                            AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                            IAsyncResult RemAr = RemoteDel.BeginInvoke(this.myURL, RemoteCallBack, null);

                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }
                    }

                    if (sub)
                    {
                        forwardSub(topic, name);
                    }
                    else
                    {
                        forwardUnsub(topic, name);
                    }

                }
            }
        }

        public void receiveDeadPub(string pubName, Message m, string morto, int filter, int order, int eventNumber, int logMode)
        {
            Console.WriteLine("Receive DEAD pub, recebido do pub {0} no topico {1}, numero de Seq {2}", pubName, m.Topic, m.SeqNum);

            List<string> aux = new List<string>();
            foreach (var rep in lstReplicas)
            {
                if (!rep.Equals(urlLider))
                {
                    aux.Add(rep);
                }
            }

            if (!isLeader && morto.Equals(urlLider))
            {
                Console.WriteLine("Lider morto");

                tryLeader(aux);

                if (isLeader)
                {
                    Console.WriteLine("Fiquei lider, vou mandar cenas");
                    BrokerReceiveBroker bro;
                    foreach (var r in lstReplicas)
                    {
                        bro = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), r + "BrokerCommunication");
                        try
                        {
                            LeaderDelegate RemoteDel = new LeaderDelegate(bro.iamleader);
                            AsyncCallback RemoteCallBack = new AsyncCallback(LeaderCallBack);
                            IAsyncResult RemAr = RemoteDel.BeginInvoke(this.myURL, RemoteCallBack, null);

                        }
                        catch (SocketException)
                        {
                            Console.WriteLine("Could not locate server");
                        }
                    }
                    if (filter == 1)
                    {
                        Console.WriteLine("----------Processando msg -> {0}", m.SeqNum);
                        this.forwardFilter(m, this.name, eventNumber, order, logMode);
                    }
                    else
                    {
                        Console.WriteLine("---------------Processando msg -> {0}", m.SeqNum);
                        this.forwardFlood(m, this.name, eventNumber, order, logMode);
                    }
                }
                
            }
        }

        public void status()
        {

            Console.WriteLine("I'm Broker {0}\r\n", name);
            Console.WriteLine("These are my Broker neighbours:");

            foreach (Broker b in lstVizinhos)
            {
                Console.WriteLine("\tBroker {0}\r\n", b.Name);
            }
            Console.WriteLine("these are my subTopics:");
            foreach (KeyValuePair<string, List<string>> t in lstSubsTopic)
            {
                foreach(string s in t.Value)
                {
                    Console.WriteLine("Sub:{0} -> topic: {1}\r\n", t.Key, s);
                }
               
            }
        }

        public bool searchTopicList(string topic, List<string> subs)
        {
            foreach (string s in subs)
            {
                if (isInterested(topic, s))
                {
                    return true;
                }
            }

            return false;
        }

        public bool isInterested(string topic, string sub)
        {
            if (topic.Equals(sub)) { return true; }

            
            string[] topicV = topic.Split('/');
            string[] subV = sub.Split('/');

            if (topicV.Length < subV.Length)
            {
                return false;
            }

            for (int i = 0; i < subV.Length; i++)
            {
                if (i == subV.Length - 1)
                {
                    if (subV[i].Equals("*"))
                    { 
                        return true;
                    }
                    else
                    {
                        
                        return false;
                    }
                }
                if (!topicV[i].Equals(subV[i]))
                {
                    
                    return false;
                }
                
            }
            
            return false;

        }

        public void freeze() {
            Console.WriteLine("Freeze!");
            this.frozen = true;
        }

        public void unfreeze()
        {
            Console.WriteLine("Unfreeze!!");
            this.frozen = false;

            //procurar por lideres eleitos
            foreach (var p in frozenEvents) {
                if (p.isIamLeader()) {
                    Console.WriteLine("Recebi um iamlider enquanto frozen - lider -> "+p.lider);
                    this.iamleader(p.lider.Substring(0,p.lider.Length-6));
                }
                if (p.isUpdateStructs())
                {
                    Console.WriteLine("Recebi um updateStructs enquanto frozen");
                    this.updateStructsPub(p.name, p.seqN, p.lstMsg, p.lstMsgTO, p.envieiRoot, p.tCount, p.espCount);
                }
            }

            if (isLeader)
            {

                Console.WriteLine("Fiz unfreeze, era lider, vou processar o que chegou entretanto");
                //iterar sobre lista de frozen events para processa-los
                foreach (var pendente in frozenEvents)
                {
                    if (pendente.isForwardFilter())
                    {
                        this.forwardFilter(pendente.msg, pendente.name, pendente.eventNumber, pendente.order, pendente.logMode);
                    }

                    if (pendente.isForwardFlood())
                    {
                        this.forwardFlood(pendente.msg, pendente.name, pendente.eventNumber, pendente.order, pendente.logMode);
                    }

                    if (pendente.isForwardSub())
                    {
                        Console.WriteLine("entrei no fw sub");
                        this.forwardSub(pendente.name, pendente.subUnsubTopic);
                    }

                    if (pendente.isForwardUnsub())
                    {
                        this.forwardUnsub(pendente.name, pendente.subUnsubTopic);
                    }

                    if (pendente.isReceiveSub())
                    {
                        this.receiveSub(pendente.subUnsubTopic, pendente.name);
                    }

                    if (pendente.isReceiveUnsub())
                    {
                        this.receiveUnsub(pendente.subUnsubTopic, pendente.name);
                    }

                    if (pendente.isReceivePub())
                    {
                        this.receivePublication(pendente.msg, pendente.name, pendente.url, pendente.filter, pendente.order, pendente.eventNumber, pendente.logMode);
                    }
                }
            }
            frozenEvents.Clear();
            Console.WriteLine("Acabei de iterar sobre frozen Events e ja clear a lista");
        }
    
    }



    public class MPMBrokerCmd : MarshalByRefObject, IProcessCmd
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

    class Evento
    {

        public Message msg;
        public String name;
        public String url;
        public int order;
        public int eventNumber;
        public int logMode;
        public int filter;
        //IamLider
        public String lider;

        //updateStructs
        public List<Message> lstMsg;
        public Dictionary<string, List<int>> lstMsgTO;
        public bool envieiRoot = false;
        public int tCount;
        public int espCount;
        public int seqN;

        Boolean receivePub = false;
        Boolean forwardFilter = false;
        Boolean forwardFlood = false;
        Boolean receiveSub = false;
        Boolean receiveUnsub = false;
        Boolean forwardSub = false;
        Boolean forwardUnsub = false;
        Boolean IAMLEADER = false;
        Boolean updateStructs = false;

        public string subUnsubTopic;

        public Evento()
        {
        }

        public Boolean isUpdateStructs()
        {
            return updateStructs;
        }

        public Boolean isReceivePub()
        {
            return receivePub;
        }

        public Boolean isForwardFilter()
        {
            return forwardFilter;
        }

        public Boolean isForwardFlood()
        {
            return forwardFlood;
        }

        public Boolean isForwardSub()
        {
            return forwardSub;
        }

        public Boolean isForwardUnsub()
        {
            return forwardUnsub;
        }

        public Boolean isReceiveSub()
        {
            return receiveSub;
        }


        public Boolean isIamLeader()
        {
            return IAMLEADER;
        }

        public Boolean isReceiveUnsub()
        {
            return receiveUnsub;
        }

        public void setIamLeaderArguments(String s) {
            this.IAMLEADER = true;
            this.lider = s;
        }

        public void setArgumentsReceivePub(Message m, string pubName, string u, int filter, int order, int eventNumber, int logMode)
        {
            this.receivePub = true;
            this.msg = m;
            this.name = pubName;
            this.url = u;
            this.filter = filter;
            this.order = order;
            this.eventNumber = eventNumber;
            this.logMode = logMode;
        }

        public void setArgumentsForwarding(Message m, string brokerName, int eventNumber, int order, int logMode, Boolean filter)
        {
            if (filter) { forwardFilter = true; }
            else { forwardFlood = true; }
            this.msg = m;
            this.name = brokerName;
            this.order = order;
            this.eventNumber = eventNumber;
            this.logMode = logMode;
        }

        public void setArgumentsSubUnsub(string topic, string name, Boolean sub)
        {
            if (sub)
            {
                receiveSub = true;
            }
            else
            {
                receiveUnsub = true;
            }
            this.subUnsubTopic = topic;
            this.name = name;
        }

        public void setArgumentsForwardSubUnsub(string topic, string name, Boolean sub)
        {
            if (sub)
            {
                forwardSub = true;
            }
            else
            {
                forwardUnsub = true;
            }
            this.subUnsubTopic = topic;
            this.name = name;
        }




        public void setArgumentsUpdateStructs(string pubName, int seqN, List<Message> lstMsg, Dictionary<string, List<int>> lstMsgTO, bool t, int tCount, int espCount)
        {
            this.updateStructs = true;
            this.seqN = seqN;
            this.name = pubName;
            this.lstMsg = lstMsg;
            this.lstMsgTO = lstMsgTO;
            this.envieiRoot = t;
            this.tCount = tCount;
            this.espCount = espCount;
        }
    }

}
