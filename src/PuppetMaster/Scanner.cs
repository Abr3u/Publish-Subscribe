using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace PubSub
{
    class Scanner
    {

        public delegate void PublishDelegate(string number, string topic, string secs, int filter, int order, int eventNumber, int logMode);

        //estruturas para optimizar procura
        private Dictionary<string, TreeNode> site_treeNode = new Dictionary<string, TreeNode>();
        private Dictionary<TreeNode, List<Broker>> node_broker = new Dictionary<TreeNode, List<Broker>>();// cada site tem uma lista de brokers
        private Dictionary<string, int> pname_port = new Dictionary<string, int>();
        private Dictionary<string, string> pname_ip = new Dictionary<string, string>();
        private Dictionary<string, int> site_port = new Dictionary<string, int>();
        private Dictionary<string, string> pname_type = new Dictionary<string, string>();
        private int routing = 0;//defaul flood -> flood(0), filter(1)
        private int order = 1;//default FIFO -> NO(0), FIFO(1), TOTAL(2)
        private int logMode = 0;//default light -> light(0), full(1)
        private int eventNumber = 1;
        private string rootSite;
        
        private string myURL;
        
        public int getRouting() { return this.routing; }
        public int getOrder() { return this.order; }
        public int getLogMode() { return this.logMode; }
        public int getEventNumber() { return this.eventNumber; }

        public Scanner(string u)
        {
            this.myURL = u;
        }


        public static void PublishCallBack(IAsyncResult ar)
        {
            PublishDelegate del = (PublishDelegate)((AsyncResult)ar).AsyncDelegate;
            return;
        }

        public Dictionary<string, string> Pname_type
        {
            get
            {
                return pname_type;
            }

            set
            {
                pname_type = value;
            }
        }

        public Dictionary<string, int> getPname_port()
        {
            return this.pname_port;
        }

        public Dictionary<string, string> getPname_url()
        {
            return this.pname_ip;
        }

        public Dictionary<string, int> getSite_port()
        {
            return this.site_port;
        }
        
        public Dictionary<string, TreeNode> getSite_Node()
        {
            return this.site_treeNode;
        }
        public Dictionary<TreeNode, List<Broker>> getNode_Broker()
        {
            return this.node_broker;
        }


        //actualiza site_node
        public TreeNode getRootNodeFromFile(string path)
        {
            string[] lines = System.IO.File.ReadAllLines(path);
            foreach (string line in lines)
            {
                if (line.Contains("Parent") && line.Contains("none"))
                {
                    string[] words = line.Split(' ');
                    TreeNode root = new TreeNode(words[1]);
                    site_treeNode.Add(words[1], root);
                    rootSite = words[1];
                    return root;
                }
            }
            return null; //em principio nao chega aqui
        }


        public void quickRead(string v, TreeNode root)
        {
            string[] lines = System.IO.File.ReadAllLines(v);
            int siteCount = 0;
            foreach (string line in lines)
            {
                if (line.Contains("LoggingLevel"))
                {
                    string[] words = line.Split(' ');//words[1] - metodo de log
                    if (words[1].Equals("full"))
                    {
                        logMode = 1;
                    }
                }
                if (line.Contains("RoutingPolicy"))
                {
                    string[] words = line.Split(' ');//words[1] - metodo de routing
                    if(words[1].Equals("filtering")){
                           routing = 1;
                    }
                }
                 if (line.Contains("Ordering"))
                {
                    string[] words = line.Split(' ');//words[1] - metodo de ordem
                    if(words[1].Equals("NO")){
                           order = 0;
                    }
                    if (words[1].Equals("TOTAL"))
                    {
                        order = 2;
                    }
                }
                if (line.Contains("Parent")){
                    string[] words = line.Split(' ');//words[1]-filho, words[3]-pai
                    int mult = 9000 + (siteCount*100);
                    site_port.Add(words[1], mult);
                    siteCount++;
                }
                if (line.Contains("is broker"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    //actualizar estruturas
                    pname_type.Add(words[1], "broker");

                    string[] z = words[7].Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                    string[] y = z[2].Split('/');
                    int port = Int32.Parse(y[0]);
                    pname_port[words[1]] = port;
                    pname_ip[words[1]] = z[1].Split('/')[2];

                    Broker aux = new Broker(words[7], words[1], words[5]);
                    // se o site já existe adicionar novo broker à lista
                    if (node_broker.ContainsKey(t))
                    {
                        node_broker[t].Add(aux);
                    }
                    else
                    {
                        node_broker.Add(t, new List<Broker> { aux });
                    }

                }
                if (line.Contains("is publisher"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    //actualizar
                    pname_type.Add(words[1], "publisher");

                    string[] z = words[7].Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                    string[] y = z[2].Split('/');
                    int port = Int32.Parse(y[0]);
                    pname_port[words[1]] = port;
                    pname_ip[words[1]] = z[1].Split('/')[2];
                }
                if (line.Contains("is subscriber"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    //actualizar
                    pname_type.Add(words[1], "subscriber");

                    string[] z = words[7].Split(':');//z[0]->tcp;z[1]->//localhost;z[2]->XXXX/broker
                    string[] y = z[2].Split('/');
                    int port = Int32.Parse(y[0]);
                    pname_port[words[1]] = port;
                    pname_ip[words[1]] = z[1].Split('/')[2];
                }
            }
        }

        public void readExe_scriptFile(string path)
        {
            string[] lines = System.IO.File.ReadAllLines(path);
            LogInterface log = (LogInterface)Activator.GetObject(typeof(LogInterface), "tcp://"+ myURL+":1001/PuppetMasterLog");
            foreach (string s in lines)
            {
                if (s.StartsWith("Publisher"))
                {
                    log.scriptLog(s);

                    string[] words = s.Split(' ');
                    //Publisher processname Publish numberofevents Ontopic topicname Interval x ms.
                    PubInterface pub = (PubInterface)Activator.GetObject(typeof(PubInterface), "tcp://"+pname_ip[words[1]]+":" + pname_port[words[1]] + "/PMPublish");


                    PublishDelegate RemoteDel = new PublishDelegate(pub.publish);
                    AsyncCallback RemoteCallBack = new AsyncCallback(PublishCallBack);
                    IAsyncResult RemAr = RemoteDel.BeginInvoke(words[3], words[5], words[7], routing,order,eventNumber,logMode, RemoteCallBack, null);
                    
                    eventNumber = eventNumber + Int32.Parse(words[3]);
                }

                if (s.StartsWith("Subscriber"))
                {

                    log.scriptLog(s);

                    string[] words = s.Split(' ');
                    SubInterface subint = (SubInterface)Activator.GetObject(typeof(SubInterface), "tcp://" + pname_ip[words[1]] + ":" + pname_port[words[1]] + "/MPMSubUnsub");

                    if (s.Contains("Unsubscribe"))
                    {
                        subint.unsubscribe(words[3]);
                    }
                    else
                    {
                        try
                        {
                            subint.subscribe(words[3]);
                        }
                        catch (InvalidOperationException)
                        {

                        }
                    }

                }
                if (s.Equals("Freeze")) {
                    string[] aux = s.Split(' ');
                    string url = "tcp://"+pname_ip[aux[1]]+":" + pname_port[aux[1]] + "/BrokerCommunication";
                    BrokerReceiveBroker b = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), url);
                    b.freeze();

                }

                if (s.Equals("Status"))
                {
                    string url;
                    foreach (KeyValuePair<string, int> n in pname_port)
                    {
                        switch (pname_type[n.Key])
                        {
                            case "broker":
                                url = "tcp://" + pname_ip[n.Key] + ":" + pname_port[n.Key] + "/BrokerCommunication";
                                BrokerReceiveBroker b = (BrokerReceiveBroker)Activator.GetObject(typeof(BrokerReceiveBroker), url);
                                b.status();

                                break;
                            case "publisher":

                                url = "tcp://"+pname_ip[n.Key]+":" + pname_port[n.Key] + "/PMPublish";
                                PubInterface p = (PubInterface)Activator.GetObject(typeof(PubInterface), url);
                                p.status();

                                break;
                            case "subscriber":

                                url = "tcp://"+pname_ip[n.Key]+":" + pname_port[n.Key] + "/MPMSubUnsub";
                                SubInterface sub = (SubInterface)Activator.GetObject(typeof(SubInterface), url);
                                sub.status();

                                break;
                        }
                    }
                }
                if (s.StartsWith("Wait"))
                {
                    log.scriptLog(s);

                    string[] words = s.Split(' ');
                    System.Threading.Thread.Sleep(Int32.Parse(words[1]));
                }
            }
        }

        //actualiza node_broker + site_name
        public List<MyProcess> fillProcessList(string v, TreeNode root)
        {
            PuppetInterface myremote;

            string[] lines = System.IO.File.ReadAllLines(v);
            List<MyProcess> res = new List<MyProcess>();

            foreach (string line in lines)
            {
                if (line.Contains("is broker"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    fillVizinhos(t);

                    //string urlService = words[7].Substring(0, words[7].Length - 6);
                    int portPM = site_port[words[5]];
                    string urlService = words[7].Substring(0, words[7].Length - 11);//retirar XXXX/broker
                    

                    //get lista de url dos brokers
                    TreeNode pubSite = site_treeNode[words[5]];
                    List<Broker> siteBroker = node_broker[pubSite];
                    List<string> urlBrokerList = new List<string>();
                    foreach (var b in siteBroker)
                    {
                        if (!b.Name.Equals(words[1]))
                        {
                            urlBrokerList.Add(urlService + pname_port[b.Name].ToString() + "/");
                        }
                    }


                    //urlService = urlService + portPM.ToString() + "/";
                    string pmService = urlService + 1000 + "/";

                    myremote = (PuppetInterface)Activator.GetObject(typeof(PuppetInterface),pmService+"PuppetMasterURL");
                    if (words[5].Equals(rootSite))
                    {
                        myremote.createProcess(t, "brokerRoot", words[1], words[5], words[7], urlBrokerList, null,myURL);
                    }
                    else
                    {
                        List<Broker> baux = node_broker[t.Parent];
                        List<string> parentURL = new List<string>();
                        foreach (var a in baux)
                        {
                            string aux = a.URL.Substring(0,a.URL.Length - 6);
                            parentURL.Add(aux);
                        }
                        //parentURL = parentURL.Substring(0, parentURL.Length - 6);
                        myremote.createProcess(t, "broker", words[1], words[5], words[7], urlBrokerList, parentURL,myURL);
                    }
                }
                if (line.Contains("is publisher"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    string urlService = words[7].Substring(0, words[7].Length - 8);//retirar XXXX/publisher

                    //get url do broker
                    /*char idSite = words[5][words[5].Length - 1];
                    string brokeraux = "broker" + idSite;
                    int portBroker = pname_port[brokeraux];
                    string urlBroker = urlService + portBroker.ToString()+"/";*/

                    //get lista de url dos brokers
                    TreeNode pubSite = site_treeNode[words[5]];
                    List<Broker> siteBroker = node_broker[pubSite];
                    List<string> urlBrokerList = new List<string>();
                    foreach (var b in siteBroker)
                    {
                        urlBrokerList.Add(urlService + pname_port[b.Name].ToString() + "/");
                    }


                    //get url do localPM
                    int portPM = site_port[words[5]];
                    //urlService = urlService + portPM.ToString() + "/";
                    string pmService = urlService + 1000 + "/";

                    myremote = (PuppetInterface)Activator.GetObject(typeof(PuppetInterface), pmService + "PuppetMasterURL");
                    myremote.createProcess(t, "publisher", words[1], words[5], words[7], urlBrokerList, null, myURL);

                }
                if (line.Contains("is subscriber"))
                {
                    string[] words = line.Split(' '); //words[1]-name, words[5]-site, words[7]-url
                    TreeNode t = site_treeNode[words[5]];

                    string urlService = words[7].Substring(0, words[7].Length - 8);//retirar XXXX/publisher

                    //get url do broker
                    /*char idSite = words[5][words[5].Length - 1];
                    string brokeraux = "broker" + idSite;
                    int portBroker = pname_port[brokeraux];
                    string urlBroker = urlService + portBroker.ToString() + "/";*/

                    //get lista de url dos brokers
                    TreeNode pubSite = site_treeNode[words[5]];
                    List<Broker> siteBroker = node_broker[pubSite];
                    List<string> urlBrokerList = new List<string>();
                    foreach (var b in siteBroker)
                    {
                        urlBrokerList.Add(urlService + pname_port[b.Name].ToString() + "/");
                    }

                    //get url do localPM
                    int portPM = site_port[words[5]];
                    //urlService = urlService + portPM.ToString() + "/";
                    string pmService = urlService + 1000 + "/";

                    myremote = (PuppetInterface)Activator.GetObject(typeof(PuppetInterface), pmService + "PuppetMasterURL");
                    myremote.createProcess(t,"subscriber", words[1], words[5], words[7],urlBrokerList, null, myURL);

                }
            }
            return res;
        }

        private void fillVizinhos(TreeNode t)
        {
            string brokerName;
            string info;
            if (t.Parent != null)//root nao tem PAI
            {
                foreach(var b in node_broker[t.Parent])
                {
                    brokerName = b.Name;
                    info = b.Site + "%" + b.URL;
                    if (t.getVizinhos().ContainsKey(brokerName) == false)
                    {
                        t.getVizinhos().Add(brokerName, info);
                    }
                }

            }


            foreach (var f in t.GetChildren()) {
                foreach (var b in node_broker[f])
                {
                    brokerName = b.Name;
                    info = b.Site + "%" + b.URL;
                    if (t.getVizinhos().ContainsKey(brokerName) == false)
                    {
                        t.getVizinhos().Add(brokerName, info);
                    }
                   
                }
                
            }

        }

        //actualiza-se o site_node aqui
        public void readTreeFromFile(TreeNode root, string path)
        {
            string[] lines = System.IO.File.ReadAllLines(path);
            foreach (string line in lines)
            {
                if (line.Contains("Parent") && !line.Contains("none"))
                {
                    string[] words = line.Split(' '); //words[1]-filho, words[3]-pai

                    if (words[3].Equals(root.ID)) //root e o pai
                    {
                        TreeNode aux = new TreeNode(words[1]);
                        root.AddChild(aux);
                        site_treeNode.Add(words[1], aux);
                    }
                    else
                    { //temos de encontrar o pai, comecando a procura nos filhos do root
                        find(root, words[1], words[3]);
                    }
                }
            }
        }

        //actualiza site_node ( readTreeFromFile() )
        private void find(TreeNode no, string filho, string pai)
        {
            List<TreeNode> filhos = no.GetChildren();
            if (filhos != null)
            {
                foreach (var child in filhos)
                {
                    if (child.ID.Equals(pai))
                    { //child e o pai que estavamos a procura
                        TreeNode aux = new TreeNode(filho);
                        child.AddChild(aux);
                        site_treeNode.Add(filho, aux);
                    }
                }
                //pai nao esta nos filhos de "no"
                foreach (var newnode in filhos)
                { //tentar encontrar pai comecando a procura em cada filho de "no"
                    find(newnode, filho, pai);
                }
            }
        }

        

        private List<Broker> findBroker(string site)
        {
            return node_broker[site_treeNode[site]];
        }
    }
}
